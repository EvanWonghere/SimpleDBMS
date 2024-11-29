# -*- coding: utf-8 -*-
# @Time    : 2024/11/28 21:01
# @Author  : EvanWong
# @File    : RollbackRecord.py
# @Project : TestDB
from file.Page import Page
from log.LogMgr import LogMgr
from tx.recovery.LogRecord import LogRecord
from tx.recovery.RecordType import RecordType


class RollbackRecord(LogRecord):
    def __init__(self, p: Page):
        self.__tx_num: int = p.get_int(self._TX_POS)

    def __str__(self):
        return f"<ROLLBACK {self.__tx_num} >"

    @property
    def op(self) -> RecordType:
        return RecordType.ROLLBACK

    @property
    def tx_number(self) -> int:
        return self.__tx_num

    def undo(self, tx):
        pass

    @staticmethod
    def write_to_log(lm: LogMgr, tx_num: int) -> int:
        """

        Args:
            lm: Log manager, uses it to add log.
            tx_num: The transaction number.

        Returns: The LSN of the new log.

        """
        rec = bytearray(2 * 4)  # Store 2 int, one for record type, another for transaction number.
        p = Page(rec)
        p.set_int(LogRecord._TYPE_POS, RecordType.ROLLBACK.value)
        p.set_int(LogRecord._TX_POS, tx_num)
        return lm.append(p.content)

