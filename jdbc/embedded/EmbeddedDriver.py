# -*- coding: utf-8 -*-
# @Time    : 2024/12/9 19:07
# @Author  : EvanWong
# @File    : EmbeddedDriver.py
# @Project : TestDB
from jdbc.embedded.EmbeddedConnection import EmbeddedConnection
from simpledb.SimpleDB import SimpleDB


class EmbeddedDriver:
    @staticmethod
    def connect(db_name: str) -> EmbeddedConnection:
        db = SimpleDB(db_name)
        return EmbeddedConnection(db)
