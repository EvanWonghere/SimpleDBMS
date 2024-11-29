# -*- coding: utf-8 -*-
# @Time    : 2024/11/28 19:20
# @Author  : EvanWong
# @File    : LockAbortException.py
# @Project : TestDB


class LockAbortException(Exception):
    def __init__(self):
        pass

    def __str__(self):
        return "simpleDB.tx.concurrency.LockAbortException"
