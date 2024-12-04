# -*- coding: utf-8 -*-
# @Time    : 2024/12/2 17:15
# @Author  : EvanWong
# @File    : ViewMgr.py
# @Project : TestDB
from metadata.TableMgr import TableMgr
from record.Schema import Schema
from record.TableScan import TableScan
from tx.Transaction import Transaction


class ViewMgr:

    __MAX_VIEW_DEFINITION = 100

    def __init__(self, is_new: bool, tm: TableMgr, tx: Transaction):
        self.__tm = tm
        if is_new:
            schema = Schema()
            schema.add_string_field("view_name", tm.MAX_NAME_LENGTH)
            schema.add_string_field("view_definition", self.__MAX_VIEW_DEFINITION)
            self.__tm.create_table("view_cat", schema, tx)

    def create_view(self, view_name: str, view_definition: str, tx: Transaction):
        layout = self.__tm.get_layout("view_cat", tx)
        ts = TableScan(tx, "view_cat", layout)
        ts.insert()
        ts.set_string("view_name", view_name)
        ts.set_string("view_definition", view_definition)
        ts.close()

    def get_view_definition(self, view_name: str, tx: Transaction):
        layout = self.__tm.get_layout("view_cat", tx)
        ts = TableScan(tx, "view_cat", layout)

        res = None
        while ts.next():
            if ts.get_string("view_name") == view_name:
                res = ts.get_string("view_definition")
                break
        ts.close()
        return res
