# -*- coding: utf-8 -*-
# @Time    : 2024/12/8 20:52
# @Author  : EvanWong
# @File    : TablePlan.py
# @Project : TestDB
from metadata.MetadataMgr import MetadataMgr
from plan.Plan import Plan
from record.Schema import Schema
from record.TableScan import TableScan
from tx.Transaction import Transaction


class TablePlan(Plan):
    def __init__(self, tx: Transaction, table_name: str, mdm: MetadataMgr):
        self.__table_name = table_name
        self.__tx = tx
        self.__layout = mdm.get_layout(table_name, tx)
        try:
            self.__stat_info = mdm.get_stat_info(table_name, self.__layout, tx)
        except:
            raise RuntimeError(f"Table {table_name} not found")

    def open(self) -> TableScan:
        return TableScan(self.__tx, self.__table_name, self.__layout)

    @property
    def accessed_blocks(self) -> int:
        return self.__stat_info.accessed_blocks

    @property
    def output_records(self) -> int:
        return self.__stat_info.output_records

    def distinct_values(self, field_name: str) -> int:
        return self.__stat_info.distinct_values

    def schema(self) -> Schema:
        return self.__layout.schema