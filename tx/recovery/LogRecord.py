# -*- coding: utf-8 -*-
# @Time    : 2024/11/28 21:02
# @Author  : EvanWong
# @File    : LogRecord.py
# @Project : TestDB
from abc import abstractmethod


class LogRecord:
    _TYPE_POS = 0
    _TX_POS = 4
    _FILE_POS = 8

    @abstractmethod
    def op(self):
        pass

    @abstractmethod
    def tx_number(self):
        pass

    @abstractmethod
    def undo(self, tx):
        pass
