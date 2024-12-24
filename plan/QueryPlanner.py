# -*- coding: utf-8 -*-
# @Time    : 2024/12/8 20:21
# @Author  : EvanWong
# @File    : QueryPlanner.py
# @Project : TestDB

from abc import ABC, abstractmethod
from parse.data.QueryData import QueryData
from plan.Plan import Plan
from tx.Transaction import Transaction

class QueryPlanner(ABC):
    """
    A QueryPlanner is responsible for creating a query Plan from a QueryData object.
    Different planning strategies can be implemented (e.g., heuristic, cost-based).
    """

    @abstractmethod
    def create_plan(self, data: QueryData, tx: Transaction) -> Plan:
        """
        Create a Plan for the specified query data.

        Args:
            data (QueryData): The parsed query information (SELECT fields, tables, predicate).
            tx (Transaction): The current transaction.

        Returns:
            Plan: The query execution plan.
        """
        pass