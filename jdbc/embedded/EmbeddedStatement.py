# -*- coding: utf-8 -*-
# @Time    : 2024/12/9 19:06
# @Author  : EvanWong
# @File    : EmbeddedStatement.py
# @Project : TestDB

from sqlite3 import Error
from jdbc.embedded.EmbeddedConnection import EmbeddedConnection
from jdbc.embedded.EmbeddedResultSet import EmbeddedResultSet
from parse.BadSyntaxException import BadSyntaxException
from plan.Planner import Planner

class EmbeddedStatement:
    """
    A simplified Statement-like class that can execute query or update commands
    via a Planner, using the current transaction from an EmbeddedConnection.
    """

    def __init__(self, ec: EmbeddedConnection, planner: Planner):
        """
        Initialize with a connection and a planner.

        Args:
            ec (EmbeddedConnection): The active connection.
            planner (Planner): The planner for creating query/update plans.
        """
        self.__embedded_connection = ec
        self.__planner = planner

    def execute_query(self, query: str) -> EmbeddedResultSet:
        """
        Execute a SELECT query and return a result set.

        Args:
            query (str): The SQL query string (SELECT ...).

        Returns:
            EmbeddedResultSet: The result set of the query.

        Raises:
            Error: If there's a parse or runtime error, triggers a rollback.
        """
        try:
            tx = self.__embedded_connection.get_transaction()
            plan = self.__planner.create_query_plan(query, tx)
            return EmbeddedResultSet(plan, self.__embedded_connection)
        except (BadSyntaxException, ValueError, KeyError, InterruptedError, RuntimeError) as e:
            self.__embedded_connection.rollback()
            raise Error(e)

    def execute_update(self, cmd: str) -> int:
        """
        Execute an update command (INSERT, DELETE, UPDATE, CREATE).

        Args:
            cmd (str): The SQL command string.

        Returns:
            int: The number of affected rows or status code for DDL.

        Raises:
            Error: If there's a parse or runtime error, triggers a rollback.
        """
        try:
            tx = self.__embedded_connection.get_transaction()
            res = self.__planner.execute_update(cmd, tx)
            self.__embedded_connection.commit()
            return res
        except (BadSyntaxException, ValueError, InterruptedError, RuntimeError) as e:
            self.__embedded_connection.rollback()
            raise Error(e)

    def close(self):
        """
        Close the statement. Usually a no-op in this simplified DB.
        """
        pass