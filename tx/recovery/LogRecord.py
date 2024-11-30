from abc import ABC, abstractmethod

from tx.recovery.RecordType import RecordType


class LogRecord(ABC):
    """The base class for all log records.

    This class defines the interface for all log records in the system.
    Log records are used for transaction management and recovery, and each log record represents
    a specific operation or transaction event.

    Attributes:
        _TYPE_POS (int): The position where the log type is stored.
        _TX_POS (int): The position where the transaction number is stored.
        _FILE_POS (int): The position where the file identifier is stored.
    """

    _TYPE_POS = 0  # Position to store the type of log.
    _TX_POS = 4  # Position to store the transaction's number.
    _FILE_POS = 8  # Position to store the file.

    @abstractmethod
    def op(self) -> RecordType:
        """Return the type of this log record.

        This method should return the type of operation (e.g., SET_INT, COMMIT).
        """
        pass

    @abstractmethod
    def tx_number(self) -> int:
        """Return the transaction number associated with this log record."""
        pass

    @abstractmethod
    def undo(self, tx):
        """Undo the transaction associated with this log record.

        Args:
            tx: The transaction object that needs to be rolled back.
        """
        pass
