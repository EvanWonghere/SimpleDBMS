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
    """
    A simple UpdatePlanner that directly executes insert/delete/modify
    by scanning the relevant table, or delegates CREATE to MetadataMgr.
    """

    def __init__(self, mdm: MetadataMgr):
        """
        Initialize with a MetadataMgr for table info & creation.

        Args:
            mdm (MetadataMgr): The metadata manager for table, view, index creation.
        """
        self.__mdm = mdm

    def execute_insert(self, data: InsertData, tx: Transaction) -> int:
        """
        Execute an INSERT by creating a TablePlan, opening an UpdateScan,
        calling insert(), setting field values, then closing the scan.

        Returns:
            int: 1, as presumably one record is inserted each time.
        """
        plan = TablePlan(tx, data.table_name, self.__mdm)
        us: UpdateScan = plan.open()
        us.insert()
        # set field values
        for field, value in zip(data.fields, data.values):
            us.set_value(field, value)
        us.close()
        return 1

    def execute_delete(self, data: DeleteData, tx: Transaction) -> int:
        """
        Execute a DELETE by scanning the table with TablePlan,
        applying a SelectPlan for the predicate,
        then deleting each matched record.

        Returns:
            int: The count of deleted records.
        """
        plan = TablePlan(tx, data.table_name, self.__mdm)
        plan = SelectPlan(plan, data.predicate)
        us: UpdateScan = plan.open()

        count = 0
        while us.next():
            us.delete()
            count += 1
        us.close()
        return count

    def execute_modify(self, data: ModifyData, tx: Transaction) -> int:
        """
        Execute an UPDATE statement, scanning the table with a SelectPlan,
        then for each matched record, evaluate the new expression and set the field.

        Returns:
            int: The count of modified records.
        """
        plan = TablePlan(tx, data.table_name, self.__mdm)
        plan = SelectPlan(plan, data.predicate)
        us: UpdateScan = plan.open()

        count = 0
        while us.next():
            val = data.new_value.evaluate(us)
            us.set_value(data.field_name, val)
            count += 1
        us.close()
        return count

    def execute_create_table(self, data: CreateTableData, tx: Transaction) -> int:
        """
        Execute CREATE TABLE by delegating to the MetadataMgr.

        Returns:
            int: 0 or a status code (some DBs might return success code).
        """
        self.__mdm.create_table(data.table_name, data.schema, tx)
        return 0

    def execute_create_view(self, data: CreateViewData, tx: Transaction) -> int:
        """
        Execute CREATE VIEW by storing the view definition in metadata.

        Returns:
            int: 0 or a status code.
        """
        self.__mdm.create_view(data.view_name, data.query_data, tx)
        return 0

    def execute_create_index(self, data: CreateIndexData, tx: Transaction) -> int:
        """
        Execute CREATE INDEX by delegating to the MetadataMgr.

        Returns:
            int: 0 or a status code.
        """
        self.__mdm.create_index(data.index_name, data.table_name, data.field_name, tx)
        return 0