# -*- coding: utf-8 -*-
# @Time    : 2024/12/9 19:06
# @Author  : EvanWong
# @File    : EmbeddedConnection.py
# @Project : TestDB

from simpledb.SimpleDB import SimpleDB
from tx.Transaction import Transaction

class EmbeddedConnection:
    """
    A simplified Connection-like class wrapping a single transaction
    and referencing a SimpleDB instance.

    Attributes:
        __db (SimpleDB): The underlying database engine.
        __current_tx (Transaction): The current transaction.
        __planner: The planner for query/update execution (provided by the DB).
    """

    def __init__(self, db: SimpleDB):
        """
        Initialize with a SimpleDB instance, start a new transaction,
        and retrieve the DB's planner.

        Args:
            db (SimpleDB): The DB engine instance.
        """
        self.__db = db
        self.__current_tx = self.__db.new_tx
        self.__planner = self.__db.planner

    def create_statement(self):
        """
        Create a new statement for sending queries/updates
        using the current connection.

        Returns:
            EmbeddedStatement: A statement bound to this connection.
        """
        from jdbc.embedded.EmbeddedStatement import EmbeddedStatement
        return EmbeddedStatement(self, self.__planner)

    def close(self):
        """
        Close the connection by committing the current transaction.
        """
        self.__current_tx.commit()

    def commit(self):
        """
        Commit the current transaction and start a new one.
        """
        self.__current_tx.commit()
        self.__current_tx = self.__db.new_tx

    def rollback(self):
        """
        Rollback the current transaction and start a new one.
        """
        self.__current_tx.rollback()
        self.__current_tx = self.__db.new_tx

    def get_transaction(self) -> Transaction:
        """
        Retrieve the current transaction.

        Returns:
            Transaction: The current transaction.
        """
        return self.__current_tx