"""
aaiclick.snowflake_id - Snowflake ID generation backed by ClickHouse.

Uses ClickHouse's generateSnowflakeID() as the single source of truth for
globally unique, time-ordered 64-bit identifiers. IDs are pre-fetched in
batches for efficiency, served one at a time from an in-memory buffer until
empty, then refilled from ClickHouse.

In local mode, uses chdb (embedded ClickHouse) with a throwaway in-memory
session — no data path or shared session needed.  In distributed mode, uses
clickhouse-connect directly.

Snowflake ID format (64 bits):
- Bit 63: Sign bit (always 0 for positive integers)
- Bits 62-22: Timestamp in milliseconds (41 bits)
- Bits 21-12: Machine/worker ID (10 bits)
- Bits 11-0: Sequence number (12 bits)
"""

from collections import deque

from chdb.session import Session
from clickhouse_connect import get_client as get_ch_client

from .backend import is_chdb, parse_ch_url

# Bit allocation (Wikipedia Snowflake ID standard)
MACHINE_ID_BITS = 10  # Bits 21-12: supports 1024 machines
SEQUENCE_BITS = 12    # Bits 11-0: supports 4096 IDs per millisecond

# Maximum values
MAX_SEQUENCE = (1 << SEQUENCE_BITS) - 1  # 4095

# Bit shifts for decoding the 64-bit ID
TIMESTAMP_SHIFT = MACHINE_ID_BITS + SEQUENCE_BITS  # 22
MACHINE_ID_SHIFT = SEQUENCE_BITS                   # 12

# Default batch size for pre-fetching IDs from ClickHouse
_BUFFER_SIZE = 100


class SnowflakeGenerator:
    """Snowflake ID generator backed by ClickHouse.

    Pre-fetches batches of IDs from ClickHouse's generateSnowflakeID(),
    serving them from an in-memory buffer for efficiency. When the buffer
    is exhausted, a new batch is fetched automatically.

    Uses chdb (embedded) in local mode, clickhouse-connect in distributed mode.
    """

    def __init__(self, buffer_size: int = _BUFFER_SIZE):
        self._buffer_size = buffer_size
        self._buffer: deque[int] = deque()

    def _fetch_ids(self, count: int) -> list[int]:
        """Fetch a batch of Snowflake IDs from ClickHouse.

        Opens a fresh session/connection, fetches IDs, and closes it.
        For chdb, uses an empty ``Session()`` (in-memory, no data path).
        For remote ClickHouse, uses clickhouse-connect directly.
        """
        if is_chdb():
            return self._fetch_ids_chdb(count)
        return self._fetch_ids_remote(count)

    @staticmethod
    def _fetch_ids_chdb(count: int) -> list[int]:
        """Fetch IDs via a throwaway chdb in-memory session."""
        session = Session()
        result = session.query(
            f"SELECT groupArray(generateSnowflakeID()) FROM numbers({count})",
            "TabSeparated",
        )
        raw = result.bytes()
        if raw:
            text = raw.decode("utf-8").strip().strip("[]")
            if text:
                return [int(x) for x in text.split(",") if x.strip()]
        return []

    @staticmethod
    def _fetch_ids_remote(count: int) -> list[int]:
        """Fetch IDs via a clickhouse-connect client."""
        client = get_ch_client(**parse_ch_url())
        try:
            result = client.query(
                f"SELECT generateSnowflakeID() FROM numbers({count})"
            )
            return [row[0] for row in result.result_rows]
        finally:
            client.close()

    def generate(self) -> int:
        """Generate a single Snowflake ID."""
        if not self._buffer:
            self._buffer.extend(self._fetch_ids(self._buffer_size))
        return self._buffer.popleft()

    def generate_bulk(self, count: int) -> list[int]:
        """Generate multiple Snowflake IDs.

        Args:
            count: Number of IDs to generate (must be >= 1)

        Returns:
            list[int]: List of unique Snowflake IDs in ascending order
        """
        if count < 1:
            raise ValueError(f"Count must be at least 1, got {count}")
        return self.get(count)

    def get(self, size: int) -> list[int]:
        """Get multiple Snowflake IDs.

        Args:
            size: Number of IDs to generate (must be >= 0)

        Returns:
            list[int]: List of unique Snowflake IDs in ascending order
        """
        if size < 0:
            raise ValueError(f"Size must be >= 0, got {size}")
        if size == 0:
            return []

        if size <= len(self._buffer):
            return [self._buffer.popleft() for _ in range(size)]

        # Drain buffer, fetch remaining + refill from CH
        result = list(self._buffer)
        self._buffer.clear()
        remaining = size - len(result)
        fetched = self._fetch_ids(remaining + self._buffer_size)
        result.extend(fetched[:remaining])
        self._buffer.extend(fetched[remaining:])
        return result


# Global generator instance (lazy CH connection on first use)
_generator = SnowflakeGenerator()


def get_snowflake_id() -> int:
    """Get a single Snowflake ID from ClickHouse.

    Returns:
        int: Unique 64-bit Snowflake ID
    """
    return _generator.generate()


def get_snowflake_ids(size: int) -> list[int]:
    """Get multiple Snowflake IDs from ClickHouse.

    Args:
        size: Number of IDs to generate (must be >= 0)

    Returns:
        list[int]: List of unique 64-bit Snowflake IDs in ascending order
    """
    return _generator.get(size)


def decode_snowflake_id(id_val: int) -> tuple[int, int, int]:
    """Decode a Snowflake ID into its component parts.

    Args:
        id_val: 64-bit Snowflake ID to decode

    Returns:
        tuple: (timestamp, machine_id, sequence)
            - timestamp: Milliseconds since epoch (bits 62-22)
            - machine_id: Machine/worker ID (bits 21-12)
            - sequence: Sequence number (bits 11-0)

    Example:
        >>> id_val = get_snowflake_id()
        >>> timestamp, machine_id, sequence = decode_snowflake_id(id_val)
    """
    timestamp = id_val >> TIMESTAMP_SHIFT
    machine_id = (id_val >> MACHINE_ID_SHIFT) & ((1 << MACHINE_ID_BITS) - 1)
    sequence = id_val & MAX_SEQUENCE
    return timestamp, machine_id, sequence
