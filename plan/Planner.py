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
    def __init__(self, query_planner: QueryPlanner, update_planner: UpdatePlanner):
        self.__query_planner = query_planner
        self.__update_planner = update_planner

    def create_query_plan(self, query: str, tx: Transaction) -> Plan:
        parser = Parser(query)
        query_data = parser.query_data()
        return self.__query_planner.create_plan(query_data, tx)

    def execute_update(self, cmd: str, tx: Transaction) -> int:
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
            return 0

    def __verify_query(self, query: QueryData):
        pass

    def __verify_update(self, cmd):
        pass
