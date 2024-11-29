# -*- coding: utf-8 -*-
# @Time    : 2024/11/28 21:21
# @Author  : EvanWong
# @File    : RecordType.py
# @Project : TestDB
from enum import Enum


class RecordType(Enum):
    CHECKPOINT = 0
    START = 1
    COMMIT = 2
    ROLLBACK = 3
    SET_INT = 4
    SET_STRING = 5
