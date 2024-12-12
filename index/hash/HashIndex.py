# -*- coding: utf-8 -*-
# @Time    : 2024/12/3 15:07
# @Author  : EvanWong
# @File    : HashIndex.py
# @Project : TestDB
from typing import Optional

from index.Index import Index
from query.Constant import Constant
from record.Layout import Layout
from record.RID import RID
from record.TableScan import TableScan
from tx.Transaction import Transaction


class HashIndex(Index):

    BUCKET_NUM = 100

    def __init__(self, tx: Transaction, index_name: str, layout: Layout):
        self.__tx: Transaction = tx
        self.__index_name: str = index_name
        self.__layout: Layout = layout
        self.__search_key: Optional[Constant] = None
        self.__ts: Optional[TableScan] = None

    def before_first(self, search_key: Constant):
        self.close()
        self.__search_key = search_key
        bucket = hash(search_key) % HashIndex.BUCKET_NUM
        table_name = f"{self.__index_name}{bucket}"
        self.__ts = TableScan(self.__tx, table_name, self.__layout)

    def next(self) -> bool:
        while self.__ts.next():
            if self.__ts.get_string("data_value") == self.__search_key:
                return True
        return False

    def get_data_rid(self) -> RID:
        blk = self.__ts.get_int("block")
        identification = self.__ts.get_int("id")
        return RID(blk, identification)

    def insert(self, data_value: Constant, data_rid: RID):
        self.before_first(self.__search_key)
        self.__ts.insert()
        self.__ts.set_int("block", data_rid.block_number)
        self.__ts.set_int("id", data_rid.slot)
        self.__ts.set_value("data_value", data_value)

    def delete(self, data_value: Constant, data_rid: RID):
        self.before_first(self.__search_key)
        while self.__ts.next():
            if self.__ts.get_int("data_value") == data_value:
                self.__ts.delete()
                return

    def close(self):
        if self.__ts is not None:
            self.__ts.close()

    @staticmethod
    def search_cost(block_num: int) -> int:
        return block_num // HashIndex.BUCKET_NUM