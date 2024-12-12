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
    def __init__(self, ec: EmbeddedConnection, planner: Planner):
        self.__embedded_connection = ec
        self.__planner = planner

    def execute_query(self, query: str) -> EmbeddedResultSet:
        try:
            tx = self.__embedded_connection.get_transaction()
            plan = self.__planner.create_query_plan(query, tx)
            return EmbeddedResultSet(plan, self.__embedded_connection)
        except (RuntimeError, BadSyntaxException, ValueError, KeyError) as e:
            self.__embedded_connection.rollback()
            raise Error(e)

    def execute_update(self, cmd: str) -> int:
        try:
            tx = self.__embedded_connection.get_transaction()
            # print("Start executing update")
            res = self.__planner.execute_update(cmd, tx)
            self.__embedded_connection.commit()
            return res
        except (RuntimeError, BadSyntaxException, ValueError) as e:
            self.__embedded_connection.rollback()
            raise Error(e)

    def close(self):
        pass
