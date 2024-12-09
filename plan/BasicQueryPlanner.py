# -*- coding: utf-8 -*-
# @Time    : 2024/12/8 20:31
# @Author  : EvanWong
# @File    : BasicQueryPlanner.py
# @Project : TestDB
from metadata.MetadataMgr import MetadataMgr
from parse.Parser import Parser
from parse.data.QueryData import QueryData
from plan.Plan import Plan
from plan.ProductPlan import ProductPlan
from plan.ProjectPlan import ProjectPlan
from plan.QueryPlanner import QueryPlanner
from plan.SelectPlan import SelectPlan
from plan.TablePlan import TablePlan
from tx.Transaction import Transaction


class BasicQueryPlanner(QueryPlanner):
    def __init__(self, mdm: MetadataMgr):
        self.__mdm = mdm

    def create_plan(self, data: QueryData, tx: Transaction) -> Plan:
        plans: [Plan] = []
        for table_name in data.tables:
            view_definition = self.__mdm.get_view_definition(table_name, tx)
            if view_definition is None:
                plans.append(TablePlan(tx, table_name, self.__mdm))
            else:
                parser = Parser(view_definition)
                view_data = parser.query_data()
                plans.append(self.create_plan(view_data, tx))

        plan = plans.pop(0)
        for next_plan in plans:
            plan = ProductPlan(plan, next_plan)
        plan = SelectPlan(plan, data.predicate)
        plan = ProjectPlan(plan, data.fields)

        return plan