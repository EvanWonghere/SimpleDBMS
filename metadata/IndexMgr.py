# -*- coding: utf-8 -*-
# @Time    : 2024/12/2 17:16
# @Author  : EvanWong
# @File    : IndexMgr.py
# @Project : TestDB
from metadata.IndexInfo import IndexInfo
from metadata.StatMgr import StatMgr
from metadata.TableMgr import TableMgr
from record.Schema import Schema
from record.TableScan import TableScan
from tx.Transaction import Transaction


class IndexMgr:
    def __init__(self, is_new: bool, tm: TableMgr, sm: StatMgr, tx: Transaction):
        if is_new:
            schema = Schema()
            schema.add_string_field("index_name", TableMgr.MAX_NAME_LENGTH)
            schema.add_string_field("table_name", TableMgr.MAX_NAME_LENGTH)
            schema.add_string_field("field_name", TableMgr.MAX_NAME_LENGTH)
            tm.create_table("index_cat", schema, tx)

        self.__tm = tm
        self.__sm = sm
        self.__layout = self.__tm.get_layout("index_cat", tx)

    def create_index(self, index_name: str, table_name: str, field_name: str, tx:Transaction):
        ts = TableScan(tx, "index_cat", self.__layout)
        ts.insert()
        # print(f"TS current slot is {ts.get_current_slot}")
        ts.set_string("index_name", index_name)
        ts.set_string("table_name", table_name)
        ts.set_string("field_name", field_name)
        s = ts.get_string("field_name")
        # print(f"TS field name sat? {s}")
        ts.close()

    def get_index_info(self, table_name: str, tx: Transaction) -> dict[str, IndexInfo]:
        res: dict[str, IndexInfo] = {}
        ts = TableScan(tx, "index_cat", self.__layout)

        while ts.next():
            # print(f"TS current slot is {ts.get_current_slot}")
            s = ts.get_string("table_name")
            if s == table_name:
                # print(f"Got table name is {s}")
                index_name = ts.get_string("index_name")
                field_name = ts.get_string("field_name")
                layout = self.__tm.get_layout(table_name, tx)
                # print(f"Is layout.schema None? {layout.schema is None}")
                # print(f"layout.schema's fields are {layout.schema.fields}")
                stat_info = self.__sm.get_stat_info(table_name, layout, tx)
                index_info = IndexInfo(index_name, field_name, tx, layout.schema, stat_info)
                res[field_name] = index_info
        ts.close()
        return res
