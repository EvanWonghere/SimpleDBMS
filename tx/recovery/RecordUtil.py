from typing import Union
from file.Page import Page
from tx.recovery.CheckPointRecord import CheckPointRecord
from tx.recovery.CommitRecord import CommitRecord
from tx.recovery.RecordType import RecordType
from tx.recovery.RollbackRecord import RollbackRecord
from tx.recovery.SetIntRecord import SetIntRecord
from tx.recovery.SetStringRecord import SetStringRecord
from tx.recovery.StartRecord import StartRecord


class RecordUtil:
    """Utility class to create log records based on log data.

    This class provides a static method to create different types of log records
    from raw byte arrays, based on the record type.
    """

    @staticmethod
    def create_log_record(b: bytearray) -> Union[
        CheckPointRecord, StartRecord, CommitRecord, RollbackRecord, SetIntRecord, SetStringRecord, None]:
        """Create a log record from a byte array.

        The byte array `b` is parsed to determine the type of log record and the
        corresponding record is created.

        Args:
            b (bytearray): The raw log data from which a log record will be created.

        Returns:
            Union[CheckPointRecord, StartRecord, CommitRecord, RollbackRecord, SetIntRecord, SetStringRecord, None]:
                The corresponding log record object, or None if the type is unrecognized.
        """
        p = Page(b)
        op_code = RecordType(p.get_int(0))

        # Handle each record type and create the appropriate log record.
        if op_code == RecordType.CHECKPOINT:
            return CheckPointRecord()
        elif op_code == RecordType.START:
            return StartRecord(p)
        elif op_code == RecordType.COMMIT:
            return CommitRecord(p)
        elif op_code == RecordType.ROLLBACK:
            return RollbackRecord(p)
        elif op_code == RecordType.SET_INT:
            return SetIntRecord(p)
        elif op_code == RecordType.SET_STRING:
            return SetStringRecord(p)
        else:
            print(f"Unrecognized log operation code: {op_code}")
            return None
