# -*- coding: utf-8 -*-
# @Time    : 2024/11/29 15:04
# @Author  : EvanWong
# @File    : Transaction.py
# @Project : TestDB

from buffer.BufferMgr import BufferMgr
from file.BlockID import BlockID
from file.FileMgr import FileMgr
from log.LogMgr import LogMgr
from tx.BufferList import BufferList
from tx.concurrency.ConcurrencyMgr import ConcurrencyMgr
from tx.recovery.RecoveryMgr import RecoveryMgr


class Transaction:
    """ Represents a transaction with its state, concurrency control, and recovery mechanisms. """

    __next_tx_num = 0
    __EOF = -1

    def __init__(self, fm: FileMgr, lm: LogMgr, bm: BufferMgr):
        """ Initialize the transaction with the provided file, log, and buffer managers. """
        self.__tx_num: int = self.__next_tx_number()
        self.__buffers: BufferList = BufferList(bm)
        self.__fm: FileMgr = fm
        self.__bm: BufferMgr = bm
        self.__cm: ConcurrencyMgr = ConcurrencyMgr()
        self.__rm: RecoveryMgr = RecoveryMgr(self.__tx_num, lm, bm)

    def commit(self):
        """ Commit the transaction, making all changes permanent. """
        self.__rm.commit()
        self.__perform_transaction_action("Committing")

    def rollback(self):
        """ Rollback the transaction, undoing all changes. """
        self.__rm.rollback(self)
        self.__perform_transaction_action("Rolling back")

    def __perform_transaction_action(self, action_message: str):
        """ Perform a commit or rollback action for the transaction.

        Args:
            action_message (str): The action's message to print.
        """
        print(f"{action_message} transaction {self.__tx_num}")
        self.__cm.release()  # Release all locks held by the transaction
        self.__buffers.unpin_all()  # Unpin all buffers associated with this transaction

    def recover(self):
        """ Recover the transaction's state from logs. """
        self.__bm.flush_all(self.__tx_num)
        self.__rm.recover(self)  # Recover the transaction's state from the log

    def pin(self, blk: BlockID):
        """ Pin a block into the buffer pool. """
        # print("Transaction called pin")
        self.__buffers.pin(blk)

    def unpin(self, blk: BlockID):
        """ Unpin a block from the buffer pool. """
        # print("Transaction called unpin")
        self.__buffers.unpin(blk)

    def get_int(self, blk: BlockID, offset: int) -> int:
        """ Get an integer value from a block at a specified offset. """
        if not self.__cm.s_lock(blk):
            raise InterruptedError("Unable to acquire shared lock on block.")
        buff = self.__buffers.get_buffer(blk)
        # print(f"Try get buff of {blk}")
        if buff is None:
            return -1
        return buff.contents.get_int(offset)

    def set_int(self, blk: BlockID, offset: int, value: int, ok_to_log: bool):
        """ Set an integer value in a block at a specified offset. """
        if not self.__cm.x_lock(blk):
            raise InterruptedError("Unable to acquire exclusive lock on block.")
        buff = self.__buffers.get_buffer(blk)
        lsn = -1 if not ok_to_log else self.__rm.set_int(buff, offset)
        buff.contents.set_int(offset, value)
        buff.set_modified(self.__tx_num, lsn)

    def get_string(self, blk: BlockID, offset: int) -> str:
        """ Get a string value from a block at a specified offset. """
        if not self.__cm.s_lock(blk):
            raise InterruptedError("Unable to acquire shared lock on block.")
        buff = self.__buffers.get_buffer(blk)
        return buff.contents.get_string(offset)

    def set_string(self, blk: BlockID, offset: int, value: str, ok_to_log: bool):
        """ Set a string value in a block at a specified offset. """
        if not self.__cm.x_lock(blk):
            raise InterruptedError("Unable to acquire exclusive lock on block.")
        buff = self.__buffers.get_buffer(blk)
        lsn = -1 if not ok_to_log else self.__rm.set_string(buff, offset)
        buff.contents.set_string(offset, value)
        buff.set_modified(self.__tx_num, lsn)

    def get_float(self, blk: BlockID, offset: int) -> float:
        """ Get a float value from a block at a specified offset. """
        if not self.__cm.s_lock(blk):
            raise InterruptedError("Unable to acquire shared lock on block.")
        buff = self.__buffers.get_buffer(blk)
        return buff.contents.get_float(offset)

    def set_float(self, blk: BlockID, offset: int, value: float, ok_to_log: bool):
        """ Set a float value in a block at a specified offset. """
        if not self.__cm.x_lock(blk):
            raise InterruptedError("Unable to acquire exclusive lock on block.")
        buff = self.__buffers.get_buffer(blk)
        lsn = -1 if not ok_to_log else self.__rm.set_float(buff, offset)
        buff.contents.set_float(offset, value)
        buff.set_modified(self.__tx_num, lsn)

    def size(self, filename: str) -> int:
        """ Get the size of a file by checking its length. """
        dummy_blk = BlockID(filename, self.__EOF)
        if not self.__cm.s_lock(dummy_blk):
            raise InterruptedError("Unable to acquire shared lock on file.")
        return self.__fm.block_num(filename)

    def append(self, filename: str) -> BlockID:
        """ Append a new block to a file. """
        dummy_blk = BlockID(filename, self.__EOF)
        if not self.__cm.x_lock(dummy_blk):
            raise InterruptedError("Unable to acquire exclusive lock on file.")
        return self.__fm.append(filename)

    @property
    def block_size(self) -> int:
        """ Return the block size for the file manager. """
        return self.__fm.block_size

    @property
    def available_buffs(self) -> int:
        """ Return the number of available buffers in the buffer pool. """
        return self.__bm.available

    @staticmethod
    def __next_tx_number() -> int:
        """ Generate the next unique transaction number. """
        Transaction.__next_tx_num += 1
        return Transaction.__next_tx_num
