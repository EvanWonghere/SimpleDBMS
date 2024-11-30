# -*- coding: utf-8 -*-
# @Time    : 2024/11/28 21:00
# @Author  : EvanWong
# @File    : SetIntRecord.py
# @Project : TestDB
from file.BlockID import BlockID
from file.Page import Page
from log.LogMgr import LogMgr
from tx.recovery.LogRecord import LogRecord
from tx.recovery.RecordType import RecordType


class SetIntRecord(LogRecord):
    def __init__(self, p: Page):
        self.__tx_num: int = p.get_int(self._TX_POS)

        filename: str = p.get_string(self._FILE_POS)
        block_pos = self._FILE_POS + p.max_length(len(filename))
        offset_pos = block_pos + 4
        value_pos = offset_pos + 4

        self.__blk: BlockID = BlockID(filename, p.get_int(block_pos))
        self.__offset: int = p.get_int(offset_pos)
        self.__value: int = p.get_int(value_pos)

    def __str__(self):
        return f"< SET_INT {self.__tx_num} {self.__blk} {self.__offset} {self.__value} >"

    @property
    def op(self) -> RecordType:
        return RecordType.SET_INT

    @property
    def tx_number(self) -> int:
        return self.__tx_num

    def undo(self, tx):
        tx.pin(self.__blk)
        tx.set_int(self.__blk, self.__offset, self.__value, False)
        tx.unpin(self.__blk)

    @staticmethod
    def write_to_log(lm: LogMgr, tx_num: int, blk: BlockID, offset: int, value: int) -> int:
        """

        Args:
            lm (LogMgr): Log manager, uses it to add log.
            tx_num (int): The transaction number.
            blk (BlockID): The block number.
            offset (int): The transaction offset.
            value (int): The transaction value.

        Returns: The LSN of the new log.

        """
        block_pos = LogRecord._FILE_POS + Page.max_length(len(blk.filename))
        offset_pos = block_pos + 4
        value_pos = offset_pos + 4

        rec = bytearray(value_pos + 4)
        p = Page(rec)
        p.set_int(LogRecord._TYPE_POS, RecordType.SET_INT.value)
        p.set_int(LogRecord._TX_POS, tx_num)
        p.set_string(LogRecord._FILE_POS, blk.filename)
        p.set_int(block_pos, blk.number)
        p.set_int(offset_pos, offset)
        p.set_int(value_pos, value)

        return lm.append(p.content)
