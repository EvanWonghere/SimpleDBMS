# -*- coding: utf-8 -*-
# @Time    : 2024/12/9 19:07
# @Author  : EvanWong
# @File    : EmbeddedDriver.py
# @Project : TestDB

from jdbc.embedded.EmbeddedConnection import EmbeddedConnection
from simpledb.SimpleDB import SimpleDB

class EmbeddedDriver:
    """
    A simplified Driver-like class that can connect to a local embedded DB.

    Methods:
        connect(db_name: str) -> EmbeddedConnection
            Create a new EmbeddedConnection to the specified database.
    """

    @staticmethod
    def connect(db_name: str) -> EmbeddedConnection:
        """
        Create or open a local embedded DB with the specified name,
        and return a new EmbeddedConnection.

        Args:
            db_name (str): The name/path of the database.

        Returns:
            EmbeddedConnection: A new connection instance.
        """
        db = SimpleDB(db_name)
        return EmbeddedConnection(db)