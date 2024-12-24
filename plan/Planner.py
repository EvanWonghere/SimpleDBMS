# -*- coding: utf-8 -*-
# @Time    : 2024/12/8 20:53
# @Author  : EvanWong
# @File    : Planner.py
# @Project : TestDB

from parse.Parser import Parser
from parse.data.CreateIndexData import CreateIndexData
from parse.data.CreateTableData import CreateTableData
from parse.data.CreateViewData import CreateViewData
from parse.data.DeleteData import DeleteData
from parse.data.InsertData import InsertData
from parse.data.ModifyData import ModifyData
from parse.data.QueryData import QueryData
from plan.Plan import Plan
from plan.QueryPlanner import QueryPlanner
from plan.UpdatePlanner import UpdatePlanner
from tx.Transaction import Transaction

class Planner:
    """
    The Planner class coordinates query planning and update execution.

    It uses a QueryPlanner to create Plans for SELECT queries,
    and uses an UpdatePlanner to execute update commands (INSERT, DELETE, UPDATE, CREATE).
    """

    def __init__(self, query_planner: QueryPlanner, update_planner: UpdatePlanner):
        """
        Initialize the Planner with a QueryPlanner and an UpdatePlanner.

        Args:
            query_planner (QueryPlanner): The planner responsible for SELECT queries.
            update_planner (UpdatePlanner): The planner responsible for INSERT/DELETE/UPDATE/CREATE operations.
        """
        self.__query_planner = query_planner
        self.__update_planner = update_planner

    def create_query_plan(self, query: str, tx: Transaction) -> Plan:
        """
        Create a Plan object for the specified SELECT query string.

        Args:
            query (str): The SQL-like query string (SELECT ...).
            tx (Transaction): The current transaction.

        Returns:
            Plan: The Plan for executing the query.
        """
        parser = Parser(query)
        query_data: QueryData = parser.query_data()
        return self.__query_planner.create_plan(query_data, tx)

    def execute_update(self, cmd: str, tx: Transaction) -> int:
        """
        Execute an update command (INSERT, DELETE, UPDATE, CREATE).

        Args:
            cmd (str): The SQL-like command string.
            tx (Transaction): The current transaction.

        Returns:
            int: The number of affected records (for data-modifying statements),
                 or a status code for CREATE statements.
        """
        parser = Parser(cmd)
        data = parser.update_command()

        if isinstance(data, InsertData):
            return self.__update_planner.execute_insert(data, tx)
        elif isinstance(data, DeleteData):
            return self.__update_planner.execute_delete(data, tx)
        elif isinstance(data, ModifyData):
            return self.__update_planner.execute_modify(data, tx)
        elif isinstance(data, CreateTableData):
            return self.__update_planner.execute_create_table(data, tx)
        elif isinstance(data, CreateViewData):
            return self.__update_planner.execute_create_view(data, tx)
        elif isinstance(data, CreateIndexData):
            return self.__update_planner.execute_create_index(data, tx)
        else:
            # If no recognized data type, return 0 or raise an exception.
            return 0