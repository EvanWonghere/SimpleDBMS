# -*- coding: utf-8 -*-
# @Time    : 2024/11/28 21:00
# @Author  : EvanWong
# @File    : CheckPointRecord.py
# @Project : TestDB
from tx.recovery.LogRecord import LogRecord
from tx.recovery.RecordType import RecordType


class CheckPointRecord(LogRecord):
    def __init__(self):
        pass

    @property
    def op(self) -> RecordType:
        return RecordType.CHECKPOINT

    @property
    def tx_number(self) -> int:
        return -1

    def undo(self, tx):
        pass
