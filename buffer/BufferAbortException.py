# -*- coding: utf-8 -*-
# @Time    : 2024/11/28 13:52
# @Author  : EvanWong
# @File    : BufferAbortException.py
# @Project : TestDB


class BufferAbortException(Exception):
    def __init__(self, message="BufferAbortException"):
        self.message = message
        super().__init__(self.message)
