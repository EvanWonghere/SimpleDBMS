# -*- coding: utf-8 -*-
# @Time    : 2024/12/8 20:21
# @Author  : EvanWong
# @File    : UpdatePlanner.py
# @Project : TestDB

from abc import ABC, abstractmethod
from parse.data.CreateIndexData import CreateIndexData
from parse.data.CreateTableData import CreateTableData
from parse.data.CreateViewData import CreateViewData
from parse.data.DeleteData import DeleteData
from parse.data.InsertData import InsertData
from parse.data.ModifyData import ModifyData
from tx.Transaction import Transaction

class UpdatePlanner(ABC):
    """
    An abstract class defining methods to execute update statements.

    Each method returns the number of affected records (except CREATE statements,
    which could return a status code).
    """

    @abstractmethod
    def execute_insert(self, data: InsertData, tx: Transaction) -> int:
        pass

    @abstractmethod
    def execute_delete(self, data: DeleteData, tx: Transaction) -> int:
        pass

    @abstractmethod
    def execute_modify(self, data: ModifyData, tx: Transaction) -> int:
        pass

    @abstractmethod
    def execute_create_table(self, data: CreateTableData, tx: Transaction) -> int:
        pass

    @abstractmethod
    def execute_create_view(self, data: CreateViewData, tx: Transaction) -> int:
        pass

    @abstractmethod
    def execute_create_index(self, data: CreateIndexData, tx: Transaction) -> int:
        pass