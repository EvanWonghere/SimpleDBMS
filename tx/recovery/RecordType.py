from enum import Enum


class RecordType(Enum):
    """Enumeration for different types of log records.

    Each record in the log corresponds to an operation or transaction event.
    These types define the kind of operation that the log record represents.
    """

    CHECKPOINT = 0  # Represents a checkpoint record for recovery.
    START = 1  # Represents a start transaction record.
    COMMIT = 2  # Represents a commit record, indicating a successful transaction.
    ROLLBACK = 3  # Represents a rollback record, indicating a transaction failure.
    SET_INT = 4  # Represents a log record for setting an integer value.
    SET_STRING = 5  # Represents a log record for setting a string value.
