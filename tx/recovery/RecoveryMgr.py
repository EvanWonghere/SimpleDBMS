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
from tx.recovery.SetIntRecord import SetIntRecord
from tx.recovery.SetStringRecord import SetStringRecord
from tx.recovery.StartRecord import StartRecord


class RecoveryMgr:
    def __init__(self, tx_num: int, lm: LogMgr, bm: BufferMgr):
        self.__tx_num: int = tx_num
        self.__lm: LogMgr = lm
        self.__bm: BufferMgr = bm

        StartRecord.write_to_log(lm, tx_num)

    def commit(self):
        self.__bm.flush_all(self.__tx_num)
        lsn = CommitRecord.write_to_log(self.__lm, self.__tx_num)
        self.__lm.flush(lsn)

    def rollback(self, tx):
        self.__do_rollback(tx)
        self.__bm.flush_all(self.__tx_num)
        lsn = RollbackRecord.write_to_log(self.__lm, self.__tx_num)
        self.__lm.flush(lsn)

    def recover(self, tx):
        self.__do_recover(tx)
        self.__bm.flush_all(self.__tx_num)
        lsn = RollbackRecord.write_to_log(self.__lm, self.__tx_num)
        self.__lm.flush(lsn)

    def set_int(self, buff: Buffer, offset: int) -> int:
        val = buff.contents.get_int(offset)
        return SetIntRecord.write_to_log(self.__lm, self.__tx_num, buff.block, offset, val)

    def set_string(self, buff: Buffer, offset: int) -> int:
        val = buff.contents.get_string(offset)
        return SetStringRecord.write_to_log(self.__lm, self.__tx_num, buff.block, offset, val)

    def __do_rollback(self, tx):
        it = self.__lm.iterator
        while it.has_next():
            rec = RecordUtil.create_log_record(it.next())
            if rec.tx_number == self.__tx_num:
                if rec.op == RecordType.START:
                    return
                rec.undo(tx)

    def __do_recover(self, tx):
        finished = list()
        it = self.__lm.iterator
        while it.has_next():
            rec = RecordUtil.create_log_record(it.next())
            if rec.op == RecordType.CHECKPOINT:
                return
            elif rec.op == RecordType.COMMIT or rec.op == RecordType.ROLLBACK:
                finished.append(rec.tx_number)
            elif rec.tx_number not in finished:
                rec.undo(tx)
