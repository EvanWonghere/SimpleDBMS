# -*- coding: utf-8 -*-
# @Time    : 2024/11/28 21:59
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


# Transaction.py
class Transaction:

    __next_tx_num = 0
    __EOF = -1

    def __init__(self, fm: FileMgr, lm: LogMgr, bm: BufferMgr):
        self.__tx_num: int = self.__next_tx_number()
        self.__buffers: BufferList = BufferList(bm)
        self.__fm: FileMgr = fm
        self.__bm: BufferMgr = bm
        self.__cm: ConcurrencyMgr = ConcurrencyMgr()
        self.__rm: RecoveryMgr = RecoveryMgr(self.__tx_num, lm, bm)

    def commit(self):
        self.__rm.commit()
        print(f"Commiting transaction {self.__tx_num}")
        self.__cm.release()
        self.__buffers.unpin_all()

    def rollback(self):
        self.__rm.rollback(self)
        print(f"Rollback transaction {self.__tx_num}")
        self.__cm.release()
        self.__buffers.unpin_all()

    def recover(self):
        self.__bm.flush_all(self.__tx_num)
        self.__rm.recover(self)

    def pin(self, blk: BlockID):
        self.__buffers.pin(blk)

    def unpin(self, blk: BlockID):
        self.__buffers.unpin(blk)

    def get_int(self, blk: BlockID, offset: int) -> int:
        if not self.__cm.s_lock(blk):
            raise InterruptedError
        buff = self.__buffers.get_buffer(blk)
        return buff.contents.get_int(offset)

    def set_int(self, blk: BlockID, offset: int, value: int, ok_to_log: bool):
        if not self.__cm.x_lock(blk):
            raise InterruptedError
        buff = self.__buffers.get_buffer(blk)
        lsn = -1 if not ok_to_log else self.__rm.set_int(buff, offset)
        buff.contents.set_int(offset, value)
        buff.set_modified(self.__tx_num, lsn)

    def get_string(self, blk: BlockID, offset: int) -> str:
        if not self.__cm.s_lock(blk):
            raise InterruptedError
        buff = self.__buffers.get_buffer(blk)
        return buff.contents.get_string(offset)

    def set_string(self, blk: BlockID, offset: int, value: str, ok_to_log: bool):
        if not self.__cm.x_lock(blk):
            raise InterruptedError
        buff = self.__buffers.get_buffer(blk)
        lsn = -1 if not ok_to_log else self.__rm.set_string(buff, offset)
        buff.contents.set_string(offset, value)
        buff.set_modified(self.__tx_num, lsn)

    def size(self, filename: str) -> int:
        dummy_blk = BlockID(filename, self.__EOF)
        if not self.__cm.s_lock(dummy_blk):
            raise InterruptedError
        return self.__fm.length(filename)

    def append(self, filename: str) -> BlockID:
        dummy_blk = BlockID(filename, self.__EOF)
        if not self.__cm.x_lock(dummy_blk):
            raise InterruptedError
        return self.__fm.append(filename)

    @property
    def block_size(self) -> int:
        return self.__fm.block_size

    @property
    def available_buffs(self) -> int:
        return self.__bm.available

    @staticmethod
    def __next_tx_number() -> int:
        Transaction.__next_tx_num += 1
        return Transaction.__next_tx_num
