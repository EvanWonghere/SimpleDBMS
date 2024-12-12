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
    """The metadata manager.
    This is the only for client to acquire the metadata,
    the relevant managers were hidden and the interfaces were simplified.

    Attributes:
        __tm (TableMgr): The TableMgr instance.
        __vm(ViewMgr): The ViewMgr instance.
        __sm(StatMgr): The StatMgr instance.
        __im(IndexMgr): The IndexMgr instance.
    """

    def __init__(self, is_new: bool, tx: Transaction):
        self.__tm: TableMgr = TableMgr(is_new, tx)
        self.__vm: ViewMgr = ViewMgr(is_new, self.__tm, tx)
        # print("\n**************************************************************8888")
        # print("Start StatMgr init")
        self.__sm: StatMgr = StatMgr(self.__tm, tx)
        self.__im: IndexMgr = IndexMgr(is_new, self.__tm, self.__sm, tx)

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