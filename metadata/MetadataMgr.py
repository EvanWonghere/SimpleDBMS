# -*- coding: utf-8 -*-
# @Time    : 2024/12/3 19:56
# @Author  : EvanWong
# @File    : MetadataMgr.py
# @Project : TestDB
from metadata.IndexInfo import IndexInfo
from metadata.IndexMgr import IndexMgr
from metadata.StatInfo import StatInfo
from metadata.StatMgr import StatMgr
from metadata.TableMgr import TableMgr
from metadata.ViewMgr import ViewMgr
from record.Layout import Layout
from record.Schema import Schema
from tx.Transaction import Transaction


class MetadataMgr:
    def __init__(self, is_new: bool, tx: Transaction):
        self.__tm = TableMgr(is_new, tx)
        self.__vm = ViewMgr(is_new, self.__tm, tx)
        # print("\n**************************************************************8888")
        # print("Start StatMgr init")
        self.__sm = StatMgr(self.__tm, tx)
        self.__im = IndexMgr(is_new, self.__tm, self.__sm, tx)

    def create_table(self, table_name: str, schema: Schema, tx: Transaction):
        self.__tm.create_table(table_name, schema, tx)

    def get_layout(self, table_name: str, tx: Transaction) -> Layout:
        return self.__tm.get_layout(table_name, tx)

    def create_view(self, view_name: str, view_definition: str, tx: Transaction):
        self.__vm.create_view(view_name, view_definition, tx)

    def get_view_definition(self, view_name: str, tx: Transaction) -> str:
        return self.__vm.get_view_definition(view_name, tx)

    def create_index(self, index_name: str, table_name: str, field_name: str, tx: Transaction):
        self.__im.create_index(index_name, table_name, field_name, tx)

    def get_index_info(self, table_name: str, tx: Transaction) -> dict[str, IndexInfo]:
        return self.__im.get_index_info(table_name, tx)

    def get_stat_info(self, table_name: str, layout: Layout, tx: Transaction) -> StatInfo:
        return self.__sm.get_stat_info(table_name, layout, tx)