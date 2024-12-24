# -*- coding: utf-8 -*-
# @Time    : 2024/12/7 18:04
# @Author  : EvanWong
# @File    : BadSyntaxException.py
# @Project : TestDB
from typing import Optional


class BadSyntaxException(Exception):
    def __init__(self, err: Optional[str]):
        self.err = err

    def __str__(self):
        return "simpleDB.parse.BadSyntaxException: " + self.err
