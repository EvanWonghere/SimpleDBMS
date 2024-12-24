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
    """
    A simplistic QueryPlanner that uses TablePlan for each table,
    then forms a ProductPlan for multiple tables,
    applies a SelectPlan for the predicate,
    and finally a ProjectPlan for the selected fields.

    Also handles view definitions by recursively parsing them as subqueries.
    """

    def __init__(self, mdm: MetadataMgr):
        """
        Initialize with a MetadataMgr for table & view lookups.

        Args:
            mdm (MetadataMgr): The metadata manager for retrieving schema, stats, and view definitions.
        """
        self.__mdm = mdm

    def create_plan(self, data: QueryData, tx: Transaction) -> Plan:
        """
        Create a plan for the given QueryData by:
          1) For each table in data.tables:
             - If it's a view, parse and recursively plan it
             - Else create a TablePlan
          2) Combine them with a ProductPlan
          3) Wrap the product in a SelectPlan for data.predicate
          4) Wrap the select in a ProjectPlan for data.fields

        Args:
            data (QueryData): The parsed query info (fields, tables, predicate).
            tx (Transaction): The current transaction.

        Returns:
            Plan: The final plan implementing the query.
        """
        plans: list[Plan] = []
        # For each table, either use a TablePlan or recursively parse a view
        for table_name in data.tables:
            view_def = self.__mdm.get_view_definition(table_name, tx)
            if view_def is None:
                # It's a base table
                p = TablePlan(tx, table_name, self.__mdm)
            else:
                # It's a view; parse recursively
                parser = Parser(view_def)
                view_qd = parser.query_data()
                p = self.create_plan(view_qd, tx)
            plans.append(p)

        # Combine all plans into one via ProductPlan
        plan = plans.pop(0)
        for next_plan in plans:
            plan = ProductPlan(plan, next_plan)

        # Apply selection
        plan = SelectPlan(plan, data.predicate)
        # Apply projection
        plan = ProjectPlan(plan, data.fields)
        return plan