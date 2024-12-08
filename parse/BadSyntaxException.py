# -*- coding: utf-8 -*-
# @Time    : 2024/12/7 18:04
# @Author  : EvanWong
# @File    : BadSyntaxException.py
# @Project : TestDB


class BadSyntaxException(Exception):
    def __init__(self):
        pass

    def __str__(self):
        return "simpleDB.parse.BadSyntaxException"
