# -*- coding: utf-8 -*-
# @Time    : 2024/11/28 21:37
# @Author  : EvanWong
# @File    : RecoveryMgr.py
# @Project : TestDB

from buffer.Buffer import Buffer
from buffer.BufferMgr import BufferMgr
from log.LogMgr import LogMgr
from tx.recovery.CommitRecord import CommitRecord
from tx.recovery.RecordType import RecordType
from tx.recovery.RecordUtil import RecordUtil
from tx.recovery.RollbackRecord import RollbackRecord
from tx.recovery.SetFloatRecord import SetFloatRecord
from tx.recovery.SetIntRecord import SetIntRecord
from tx.recovery.SetStringRecord import SetStringRecord
from tx.recovery.StartRecord import StartRecord


class RecoveryMgr:
    """Recovery Manager for handling transaction recovery, commit, and rollback.

    This manager ensures the consistency of the database by maintaining logs of
    transaction operations, performing rollbacks or commits, and recovering the
    database state to a consistent point.

    Attributes:
        __tx_num (int): The transaction number associated with this recovery.
        __lm (LogMgr): The Log Manager used to write and read log records.
        __bm (BufferMgr): The Buffer Manager responsible for managing data buffers.
    """

    def __init__(self, tx_num: int, lm: LogMgr, bm: BufferMgr):
        """Initialize the Recovery Manager with transaction number, LogMgr, and BufferMgr.

        Args:
            tx_num (int): The transaction number for the current transaction.
            lm (LogMgr): The Log Manager that handles log operations.
            bm (BufferMgr): The Buffer Manager that handles the data buffers.
        """
        self.__tx_num: int = tx_num
        self.__lm: LogMgr = lm
        self.__bm: BufferMgr = bm

        # Log the start of the transaction
        StartRecord.write_to_log(lm, tx_num)

    def commit(self):
        """Commit the current transaction, ensuring changes are persisted and logged.

        This method flushes all the buffers related to the transaction and writes a commit log record.
        """
        lsn = CommitRecord.write_to_log(self.__lm, self.__tx_num)
        self.__lm.flush(lsn)
        self.__bm.flush_all(self.__tx_num)

    def rollback(self, tx):
        """Rollback the current transaction, undoing all the changes made by the transaction.

        This method performs a rollback by iterating through the log records, undoing changes
        and flushing all buffers.

        Args:
            tx: The transaction object representing the transaction to rollback.
        """
        self.__do_rollback(tx)
        self.__bm.flush_all(self.__tx_num)
        lsn = RollbackRecord.write_to_log(self.__lm, self.__tx_num)
        self.__lm.flush(lsn)

    def recover(self, tx):
        """Recover the database to a consistent state, applying all the changes up until the last checkpoint.

        This method reads log records, performs any necessary undo operations, and ensures that
        the database state is consistent, applying only valid operations.

        Args:
            tx: The transaction object for recovery operations.
        """
        self.__do_recover(tx)
        self.__bm.flush_all(self.__tx_num)
        # A rollback record is written at the end of recovery, but this may not always be required.
        lsn = RollbackRecord.write_to_log(self.__lm, self.__tx_num)
        self.__lm.flush(lsn)

    def set_int(self, buff: Buffer, offset: int) -> int:
        """Write the set int record to log.

        Args:
            buff (Buffer): The buffer containing the block to modify.
            offset (int): The offset in the block where the integer will be set.

        Returns:
            int: The LSN of the log record created for this operation.
        """
        val = buff.contents.get_int(offset)
        return SetIntRecord.write_to_log(self.__lm, self.__tx_num, buff.block, offset, val)

    def set_string(self, buff: Buffer, offset: int) -> int:
        """Write the set string record to log.

        Args:
            buff (Buffer): The buffer containing the block to modify.
            offset (int): The offset in the block where the string will be set.

        Returns:
            int: The LSN of the log record created for this operation.
        """
        val = buff.contents.get_string(offset)
        return SetStringRecord.write_to_log(self.__lm, self.__tx_num, buff.block, offset, val)

    def set_float(self, buff: Buffer, offset: int) -> int:
        """Write the set float record to log.

        Args:
            buff (Buffer): The buffer containing the block to modify.
            offset (int): The offset in the block where the float will be set.

        Returns:
            int: The LSN of the log record created for this operation.
        """
        val = buff.contents.get_float(offset)
        return SetFloatRecord.write_to_log(self.__lm, self.__tx_num, buff.block, offset, val)

    def __do_rollback(self, tx):
        """Perform rollback operations for the transaction, undoing all operations in reverse order.

        This method iterates through the log records, undoing changes made by the transaction.

        Args:
            tx: The transaction object for rolling back operations.
        """
        it = self.__lm.iterator
        while it.has_next():
            rec = RecordUtil.create_log_record(it.next())
            if rec.tx_number == self.__tx_num:
                if rec.op == RecordType.START:
                    return  # The start record indicates the transaction began, stop after that.
                rec.undo(tx)

    def __do_recover(self, tx):
        """Recover the database state, undoing all operations that were not committed.

        This method scans through the log records, rolling back uncommitted transactions
        and stopping at the last checkpoint 

        Args:
            tx: The transaction object for recovery operations.
        """
        finished = list()
        it = self.__lm.iterator
        while it.has_next():
            rec = RecordUtil.create_log_record(it.next())
            if rec.op == RecordType.CHECKPOINT:
                return  # Stop recovery at the checkpoint.
            elif rec.op == RecordType.COMMIT or rec.op == RecordType.ROLLBACK:
                finished.append(rec.tx_number)
            elif rec.tx_number not in finished:
                rec.undo(tx)
