# -*- coding: utf-8 -*-
# @Time    : 2024/12/8 20:32
# @Author  : EvanWong
# @File    : BasicUpdatePlanner.py
# @Project : TestDB
from metadata.MetadataMgr import MetadataMgr
from parse.data.CreateIndexData import CreateIndexData
from parse.data.CreateTableData import CreateTableData
from parse.data.CreateViewData import CreateViewData
from parse.data.DeleteData import DeleteData
from parse.data.InsertData import InsertData
from parse.data.ModifyData import ModifyData
from plan.SelectPlan import SelectPlan
from plan.TablePlan import TablePlan
from plan.UpdatePlanner import UpdatePlanner
from query.UpdateScan import UpdateScan
from tx.Transaction import Transaction


class BasicUpdatePlanner(UpdatePlanner):
    def __init__(self, mdm: MetadataMgr):
        self.__mdm = mdm

    def execute_insert(self, data: InsertData, tx: Transaction) -> int:
        plan = TablePlan(tx, data.tabel_name, self.__mdm)
        us: UpdateScan = plan.open()
        us.insert()
        value_iter = iter(data.values)
        for field in data.fields:
            value = next(value_iter)
            us.set_value(field, value)
        us.close()
        return 1

    def execute_delete(self, data: DeleteData, tx: Transaction) -> int:
        plan = TablePlan(tx, data.table_name, self.__mdm)
        plan = SelectPlan(plan, data.predicate)
        us: UpdateScan = plan.open()

        count = 0
        while us.next():
            us.delete()
            count += 1
        return count

    def execute_modify(self, data: ModifyData, tx: Transaction) -> int:
        plan = TablePlan(tx, data.table_name, self.__mdm)
        plan = SelectPlan(plan, data.predicate)
        us: UpdateScan = plan.open()

        count = 0
        while us.next():
            value = data.new_value.evaluate(us)
            us.set_value(data.field_name, value)
            count += 1
        us.close()
        return count

    def execute_create_table(self, data: CreateTableData, tx: Transaction) -> int:
        self.__mdm.create_table(data.table_name, data.schema, tx)
        return 0

    def execute_create_view(self, data: CreateViewData, tx: Transaction) -> int:
        self.__mdm.create_view(data.view_name, data.query_data, tx)
        return 0

    def execute_create_index(self, data: CreateIndexData, tx: Transaction) -> int:
        self.__mdm.create_index(data.index_name, data.table_name, data.field_name, tx)
        return 0