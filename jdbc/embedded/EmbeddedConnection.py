# -*- coding: utf-8 -*-
# @Time    : 2024/12/9 19:06
# @Author  : EvanWong
# @File    : EmbeddedConnection.py
# @Project : TestDB
from simpledb.SimpleDB import SimpleDB
from tx.Transaction import Transaction


class EmbeddedConnection:
    def __init__(self, db: SimpleDB):
        self.__db = db
        self.__current_tx=  self.__db.newTx()
        self.__planner = self.__db.planner()

    def create_statement(self):
        from jdbc.embedded.EmbeddedStatement import EmbeddedStatement
        return EmbeddedStatement(self, self.__planner)

    def close(self):
        self.__current_tx.commit()

    def commit(self):
        self.__current_tx.commit()
        self.__current_tx = self.__db.newTx()

    def rollback(self):
        self.__current_tx.rollback()
        self.__current_tx = self.__db.newTx()

    def get_transaction(self) -> Transaction:
        return self.__current_tx
