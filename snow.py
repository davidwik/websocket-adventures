import time

# --- Constants ---
TIMESTAMP_BITS = 41
WORKER_BITS = 10
SEQUENCE_BITS = 12

MAX_WORKER_ID = (1 << WORKER_BITS) - 1  # 1023
MAX_SEQUENCE = (1 << SEQUENCE_BITS) - 1  # 4095

WORKER_SHIFT = SEQUENCE_BITS  # 12
TIMESTAMP_SHIFT = SEQUENCE_BITS + WORKER_BITS  # 22

# Default custom epoch (ms since UNIX epoch)
DEFAULT_EPOCH = 1700000000000


# --- Snowflake class ---
class Snowflake:
    __slots__ = ("worker_id", "last_ts", "sequence", "epoch")

    def __init__(self, worker_id: int, epoch: int = DEFAULT_EPOCH):
        if not (0 <= worker_id <= MAX_WORKER_ID):
            raise ValueError(f"worker_id must be 0-{MAX_WORKER_ID}")
        self.worker_id = worker_id
        self.last_ts = -1
        self.sequence = 0
        self.epoch = epoch

    # Generate next Snowflake ID
    def next_id(self) -> int:
        ts = time.time_ns() // 1_000_000  # current timestamp in ms

        if ts == self.last_ts:
            self.sequence += 1
            if self.sequence > MAX_SEQUENCE:
                # Busy-wait until next millisecond
                while ts <= self.last_ts:
                    ts = time.time_ns() // 1_000_000
                self.sequence = 0
        else:
            self.sequence = 0

        self.last_ts = ts

        # Compose Snowflake ID
        return (
            ((ts - self.epoch) << TIMESTAMP_SHIFT)
            | (self.worker_id << WORKER_SHIFT)
            | self.sequence
        )

    # Decode a Snowflake ID into its components
    def decode(self, snowflake_id: int):
        ts = (
            (snowflake_id >> TIMESTAMP_SHIFT) & ((1 << TIMESTAMP_BITS) - 1)
        ) + self.epoch
        worker = (snowflake_id >> WORKER_SHIFT) & MAX_WORKER_ID
        seq = snowflake_id & MAX_SEQUENCE
        return ts, worker, seq

    # Extract just worker ID
    @staticmethod
    def worker_id_from(snowflake_id: int) -> int:
        return (snowflake_id >> WORKER_SHIFT) & MAX_WORKER_ID

    # Extract just sequence number
    @staticmethod
    def sequence_from(snowflake_id: int) -> int:
        return snowflake_id & MAX_SEQUENCE

    # Extract just timestamp (ms)
    @staticmethod
    def timestamp_from(snowflake_id: int, epoch: int = DEFAULT_EPOCH) -> int:
        return ((snowflake_id >> TIMESTAMP_SHIFT) & ((1 << TIMESTAMP_BITS) - 1)) + epoch


if __name__ == "__main__":
    breakpoint()
