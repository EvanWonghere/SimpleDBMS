# -*- coding: utf-8 -*-
# @Time    : 2024/11/29 15:52
# @Author  : EvanWong
# @File    : RecordUtil.py
# @Project : TestDB
from file.Page import Page
from tx.recovery.CheckPointRecord import CheckPointRecord
from tx.recovery.CommitRecord import CommitRecord
from tx.recovery.RecordType import RecordType
from tx.recovery.RollbackRecord import RollbackRecord
from tx.recovery.SetIntRecord import SetIntRecord
from tx.recovery.SetStringRecord import SetStringRecord
from tx.recovery.StartRecord import StartRecord


class RecordUtil:
    @staticmethod
    def create_log_record(b: bytearray)\
            -> CheckPointRecord | StartRecord | CommitRecord | RollbackRecord | SetIntRecord | SetStringRecord | None:
        p = Page(b)
        op_code = RecordType(p.get_int(0))
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
            print(f"None returned, op code is {op_code}")
            return None
