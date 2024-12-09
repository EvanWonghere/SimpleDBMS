# -*- coding: utf-8 -*-
# @Time    : 2024/12/8 20:21
# @Author  : EvanWong
# @File    : QueryPlanner.py
# @Project : TestDB
from abc import ABC, abstractmethod

from parse.data.QueryData import QueryData
from tx.Transaction import Transaction


class QueryPlanner(ABC):
    @abstractmethod
    def create_plan(self, data: QueryData, tx: Transaction):
        pass
