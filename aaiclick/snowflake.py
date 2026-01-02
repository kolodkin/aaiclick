"""
aaiclick.snowflake - Snowflake ID generator for unique table naming.

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
from .env import SNOWFLAKE_MACHINE_ID

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

    def generate(self) -> int:
        """
        Generate a new Snowflake ID.

        Returns:
            int: Unique 64-bit Snowflake ID

        Raises:
            RuntimeError: If clock moves backwards
        """
        with self.lock:
            timestamp = self._current_timestamp()

            # Check for clock moving backwards
            if timestamp < self.last_timestamp:
                raise RuntimeError(
                    f"Clock moved backwards. Refusing to generate ID for "
                    f"{self.last_timestamp - timestamp}ms"
                )

            # Same millisecond - increment sequence
            if timestamp == self.last_timestamp:
                self.sequence = (self.sequence + 1) & MAX_SEQUENCE
                # Sequence overflow - wait for next millisecond
                if self.sequence == 0:
                    timestamp = self._wait_next_millis(self.last_timestamp)
            else:
                # New millisecond - reset sequence
                self.sequence = 0

            self.last_timestamp = timestamp

            # Combine all parts into final ID
            snowflake_id = (
                (timestamp << TIMESTAMP_SHIFT)
                | (self.machine_id << MACHINE_ID_SHIFT)
                | self.sequence
            )

            return snowflake_id

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
            for _ in range(count):
                timestamp = self._current_timestamp()

                # Check for clock moving backwards
                if timestamp < self.last_timestamp:
                    raise RuntimeError(
                        f"Clock moved backwards. Refusing to generate ID for "
                        f"{self.last_timestamp - timestamp}ms"
                    )

                # Same millisecond - increment sequence
                if timestamp == self.last_timestamp:
                    self.sequence = (self.sequence + 1) & MAX_SEQUENCE
                    # Sequence overflow - wait for next millisecond
                    if self.sequence == 0:
                        timestamp = self._wait_next_millis(self.last_timestamp)
                else:
                    # New millisecond - reset sequence
                    self.sequence = 0

                self.last_timestamp = timestamp

                # Combine all parts into final ID
                snowflake_id = (
                    (timestamp << TIMESTAMP_SHIFT)
                    | (self.machine_id << MACHINE_ID_SHIFT)
                    | self.sequence
                )

                ids.append(snowflake_id)

        return ids

    def get(self, size: int) -> list[int]:
        """
        Get a bulk of Snowflake IDs with different sequence numbers.

        This method ensures the size is within the valid sequence number range
        (0 to MAX_SEQUENCE). For sizes up to 4096, all IDs will typically have
        different sequence numbers within the same millisecond.

        Args:
            size: Number of IDs to generate (0 to 4095 inclusive)

        Returns:
            list[int]: List of unique 64-bit Snowflake IDs with different sequences

        Raises:
            ValueError: If size is not in range [0, MAX_SEQUENCE]
        """
        if not (0 <= size <= MAX_SEQUENCE):
            raise ValueError(
                f"Size must be between 0 and {MAX_SEQUENCE} (inclusive), got {size}"
            )

        if size == 0:
            return []

        return self.generate_bulk(size)


# Global generator instance
_generator = SnowflakeGenerator()


def get_snowflake_id() -> int:
    """
    Get a single Snowflake ID.

    Returns:
        int: Unique 64-bit Snowflake ID
    """
    return _generator.generate()


def get_snowflake_ids(size: int) -> list[int]:
    """
    Get a bulk of Snowflake IDs with different sequence numbers.

    Validates that size is within the sequence number range (0 to MAX_SEQUENCE).
    This ensures all IDs can have different sequence numbers within the same
    millisecond. For larger sizes, use the internal generator directly.

    Args:
        size: Number of IDs to generate (0 to 4095 inclusive)

    Returns:
        list[int]: List of unique 64-bit Snowflake IDs with different sequences

    Raises:
        ValueError: If size is not in range [0, MAX_SEQUENCE]
    """
    return _generator.get(size)


# Internal functions - use get_snowflake_id/get_snowflake_ids instead
def _generate_snowflake_id() -> int:
    """Internal: Generate a single Snowflake ID."""
    return _generator.generate()


def _generate_snowflake_ids(count: int) -> list[int]:
    """Internal: Generate multiple Snowflake IDs without size validation."""
    return _generator.generate_bulk(count)
