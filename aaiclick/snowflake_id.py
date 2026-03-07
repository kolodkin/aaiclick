"""
aaiclick.snowflake_id - Snowflake ID generator for unique table naming.

This module provides a Snowflake ID generator that creates unique,
time-ordered IDs suitable for distributed systems.

Snowflake ID format (64 bits) - based on Twitter's Snowflake algorithm:
Reference: https://en.wikipedia.org/wiki/Snowflake_ID

Bit layout (from MSB to LSB):
- Bit 63: Sign bit (always 0 for positive integers)
- Bits 62-22: Timestamp in milliseconds since custom epoch (41 bits)
- Bits 21-12: Machine/worker ID (10 bits, supports up to 1024 machines)
- Bits 11-0: Sequence number (12 bits, up to 4096 IDs per millisecond per machine)

This provides:
- ~69 years of timestamps from the epoch
- 1024 unique machine IDs (0-1023)
- 4096 IDs per millisecond per machine
- Time-ordered, globally unique identifiers
"""

import time
import threading
from .data.env import SNOWFLAKE_MACHINE_ID

# Custom epoch: January 1, 2024 00:00:00 UTC
# This gives us ~69 years from this epoch (41 bits of milliseconds)
EPOCH = 1704067200000  # milliseconds

# Bit allocation (Wikipedia Snowflake ID standard)
MACHINE_ID_BITS = 10  # Bits 21-12: supports 1024 machines
SEQUENCE_BITS = 12    # Bits 11-0: supports 4096 IDs per millisecond
# Timestamp uses 41 bits (bits 62-22) - bit 63 is sign bit (always 0)

# Maximum values
MAX_MACHINE_ID = (1 << MACHINE_ID_BITS) - 1  # 1023
MAX_SEQUENCE = (1 << SEQUENCE_BITS) - 1      # 4095

# Bit shifts for constructing the 64-bit ID
TIMESTAMP_SHIFT = MACHINE_ID_BITS + SEQUENCE_BITS  # 22 (bits 62-22 for timestamp)
MACHINE_ID_SHIFT = SEQUENCE_BITS                   # 12 (bits 21-12 for machine ID)


class SnowflakeGenerator:
    """
    Thread-safe Snowflake ID generator.

    Generates unique 64-bit IDs that are time-ordered and suitable
    for distributed systems.
    """

    def __init__(self, machine_id: int = SNOWFLAKE_MACHINE_ID):
        """
        Initialize the Snowflake ID generator.

        Args:
            machine_id: Unique machine/worker ID (0-1023)

        Raises:
            ValueError: If machine_id is out of valid range
        """
        if not 0 <= machine_id <= MAX_MACHINE_ID:
            raise ValueError(
                f"Machine ID must be between 0 and {MAX_MACHINE_ID}, got {machine_id}"
            )

        self.machine_id = machine_id
        self.sequence = 0
        self.last_timestamp = -1
        self.lock = threading.Lock()

    def _current_timestamp(self) -> int:
        """Get current timestamp in milliseconds since epoch."""
        return int(time.time() * 1000) - EPOCH

    def _wait_next_millis(self, last_timestamp: int) -> int:
        """Wait until next millisecond."""
        timestamp = self._current_timestamp()
        while timestamp <= last_timestamp:
            timestamp = self._current_timestamp()
        return timestamp

    def _sync_timestamp(self) -> int:
        """Get current timestamp, waiting if last_timestamp is slightly ahead (from reserve)."""
        timestamp = self._current_timestamp()
        if timestamp < self.last_timestamp:
            gap = self.last_timestamp - timestamp
            if gap > 1000:
                raise RuntimeError(
                    f"Clock moved backwards by {gap}ms"
                )
            timestamp = self._wait_next_millis(self.last_timestamp - 1)
        return timestamp

    def generate(self) -> int:
        """
        Generate a new Snowflake ID.

        Returns:
            int: Unique 64-bit Snowflake ID

        Raises:
            RuntimeError: If clock moves backwards
        """
        return self.generate_bulk(1)[0]

    def generate_bulk(self, count: int) -> list[int]:
        """
        Generate multiple sequential Snowflake IDs efficiently.

        This method generates a bulk of sequential IDs in a single lock acquisition,
        which is more efficient than calling generate() multiple times.

        Args:
            count: Number of IDs to generate

        Returns:
            list[int]: List of unique 64-bit Snowflake IDs in ascending order

        Raises:
            ValueError: If count is less than 1
            RuntimeError: If clock moves backwards
        """
        if count < 1:
            raise ValueError(f"Count must be at least 1, got {count}")

        ids = []
        with self.lock:
            timestamp = self._sync_timestamp()

            # If new millisecond since last generate(), reset sequence
            if timestamp != self.last_timestamp:
                self.sequence = 0
                self.last_timestamp = timestamp

            # Generate all IDs with sequential sequence numbers
            for _ in range(count):
                # Check for sequence overflow
                if self.sequence > MAX_SEQUENCE:
                    # Wait for next millisecond
                    timestamp = self._wait_next_millis(timestamp)
                    self.last_timestamp = timestamp
                    self.sequence = 0

                # Combine all parts into final ID
                snowflake_id = (
                    (timestamp << TIMESTAMP_SHIFT)
                    | (self.machine_id << MACHINE_ID_SHIFT)
                    | self.sequence
                )

                ids.append(snowflake_id)
                self.sequence += 1

        return ids

    def begin_reserve(self) -> tuple[int, int, int]:
        """Get current position for SQL-side ID generation.

        Returns (base_timestamp, machine_id, start_sequence) without advancing
        state. Call end_reserve() after INSERT to advance by actual row count.
        """
        with self.lock:
            timestamp = self._sync_timestamp()
            if timestamp != self.last_timestamp:
                self.sequence = 0
                self.last_timestamp = timestamp
            return timestamp, self.machine_id, self.sequence

    def end_reserve(self, base_timestamp: int, start_sequence: int, count: int) -> None:
        """Advance state by actual count after INSERT completed.

        Only advances forward — safe if called out of order.
        """
        if count == 0:
            return
        with self.lock:
            seq_capacity = MAX_SEQUENCE + 1
            total_ids = start_sequence + count
            extra_ms = total_ids // seq_capacity
            final_seq = total_ids % seq_capacity

            if extra_ms > 0:
                if final_seq == 0:
                    end_ts = base_timestamp + extra_ms - 1
                    end_seq = seq_capacity
                else:
                    end_ts = base_timestamp + extra_ms
                    end_seq = final_seq
            else:
                end_ts = base_timestamp
                end_seq = final_seq

            # Only advance forward
            if end_ts > self.last_timestamp or (
                end_ts == self.last_timestamp and end_seq > self.sequence
            ):
                self.last_timestamp = end_ts
                self.sequence = end_seq

    def get(self, size: int) -> list[int]:
        """
        Get a bulk of Snowflake IDs.

        For sizes larger than MAX_SEQUENCE (4095), automatically splits the request
        into multiple sequential bulk generations to handle any size.

        Args:
            size: Number of IDs to generate (must be >= 0)

        Returns:
            list[int]: List of unique 64-bit Snowflake IDs

        Raises:
            ValueError: If size is negative
        """
        if size < 0:
            raise ValueError(f"Size must be >= 0, got {size}")

        if size == 0:
            return []

        # If size fits in one bulk, generate directly
        if size <= MAX_SEQUENCE:
            return self.generate_bulk(size)

        # For larger sizes, split into multiple bulk generations
        ids = []
        remaining = size

        while remaining > 0:
            chunk_size = min(remaining, MAX_SEQUENCE)
            chunk_ids = self.generate_bulk(chunk_size)
            ids.extend(chunk_ids)
            remaining -= chunk_size

        return ids


# Global generator instance
_generator = SnowflakeGenerator()


def get_snowflake_id() -> int:
    """
    Get a single Snowflake ID.

    Returns:
        int: Unique 64-bit Snowflake ID
    """
    return _generator.generate()


def snowflake_id_sql(base_timestamp: int, machine_id: int, start_sequence: int) -> str:
    """Generate ClickHouse SQL expression for snowflake IDs using row_number().

    Constructs proper snowflake IDs via bit manipulation, handling sequence
    overflow by incrementing the timestamp — mirrors generate_bulk() logic.
    """
    seq_capacity = MAX_SEQUENCE + 1  # 4096
    return (
        f"bitOr(bitOr("
        f"bitShiftLeft("
        f"toUInt64({base_timestamp} + intDiv({start_sequence} + row_number() OVER () - 1, {seq_capacity})), "
        f"{TIMESTAMP_SHIFT}), "
        f"bitShiftLeft(toUInt64({machine_id}), {MACHINE_ID_SHIFT})), "
        f"toUInt64(({start_sequence} + row_number() OVER () - 1) % {seq_capacity}))"
    )


def begin_snowflake_ids_sql() -> tuple[str, int, int]:
    """Get SQL expression for snowflake IDs and reservation token.

    Returns (sql_expr, base_ts, start_seq). After INSERT, call
    end_snowflake_ids(base_ts, start_seq, written_rows) to advance state.
    """
    base_ts, machine_id, start_seq = _generator.begin_reserve()
    return snowflake_id_sql(base_ts, machine_id, start_seq), base_ts, start_seq


def end_snowflake_ids(base_ts: int, start_seq: int, count: int) -> None:
    """Advance generator state by actual rows written."""
    _generator.end_reserve(base_ts, start_seq, count)


def get_snowflake_ids(size: int) -> list[int]:
    """
    Get a bulk of Snowflake IDs.

    For sizes larger than MAX_SEQUENCE (4095), automatically splits the request
    into multiple sequential bulk generations. This allows generating any number
    of IDs while maintaining uniqueness and time-ordering.

    Args:
        size: Number of IDs to generate (must be >= 0)

    Returns:
        list[int]: List of unique 64-bit Snowflake IDs in ascending order

    Raises:
        ValueError: If size is negative

    Examples:
        >>> ids = get_snowflake_ids(10000)  # Generates 10k IDs across multiple chunks
        >>> len(ids)
        10000
        >>> ids == sorted(ids)  # Always time-ordered
        True
    """
    return _generator.get(size)


def decode_snowflake_id(id_val: int) -> tuple[int, int, int]:
    """
    Decode a Snowflake ID into its component parts.

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
    timestamp = id_val >> 22  # Extract bits 62-22
    machine_id = (id_val >> 12) & 0x3FF  # Extract bits 21-12 (10 bits)
    sequence = id_val & 0xFFF  # Extract bits 11-0 (12 bits)
    return timestamp, machine_id, sequence


# Internal functions - use get_snowflake_id/get_snowflake_ids instead
def _generate_snowflake_id() -> int:
    """Internal: Generate a single Snowflake ID."""
    return _generator.generate()


def _generate_snowflake_ids(count: int) -> list[int]:
    """Internal: Generate multiple Snowflake IDs without size validation."""
    return _generator.generate_bulk(count)
