# -*- coding: utf-8 -*-
# @Time    : 2024/12/2 17:16
# @Author  : EvanWong
# @File    : IndexMgr.py
# @Project : TestDB

from typing import Dict
from metadata.IndexInfo import IndexInfo
from metadata.StatMgr import StatMgr
from metadata.TableMgr import TableMgr
from record.Layout import Layout
from record.Schema import Schema
from record.TableScan import TableScan
from tx.Transaction import Transaction


class IndexMgr:
    """
    Manages the creation and retrieval of index metadata from the 'index_cat' catalog table.

    Attributes:
        __tm (TableMgr): The table manager that handles table creation and layout retrieval.
        __sm (StatMgr): The manager for table statistics.
        __layout (Layout): The layout used for the 'index_cat' catalog table.
    """

    def __init__(self, is_new: bool, tm: TableMgr, sm: StatMgr, tx: Transaction):
        """
        Initialize the IndexMgr.

        Args:
            is_new (bool): True if this is a new database, False otherwise.
            tm (TableMgr): The table manager instance.
            sm (StatMgr): The statistics manager instance.
            tx (Transaction): The current transaction.
        """
        self.__tm: TableMgr = tm
        self.__sm: StatMgr = sm

        # If the database is new, create the 'index_cat' table
        if is_new:
            schema = Schema()
            schema.add_string_field("index_name", TableMgr.MAX_NAME_LENGTH)
            schema.add_string_field("table_name", TableMgr.MAX_NAME_LENGTH)
            schema.add_string_field("field_name", TableMgr.MAX_NAME_LENGTH)
            self.__tm.create_table("index_cat", schema, tx)

        self.__layout: Layout = self.__tm.get_layout("index_cat", tx)

    def create_index(self, index_name: str, table_name: str, field_name: str, tx: Transaction):
        """
        Create a record in the 'index_cat' table representing a new index.

        Args:
            index_name (str): The name of the new index.
            table_name (str): The name of the table on which the index is built.
            field_name (str): The name of the field on which the index is built.
            tx (Transaction): The current transaction.
        """
        ts = TableScan(tx, "index_cat", self.__layout)
        ts.insert()
        ts.set_string("index_name", index_name)
        ts.set_string("table_name", table_name)
        ts.set_string("field_name", field_name)
        ts.close()

    def get_index_info(self, table_name: str, tx: Transaction) -> Dict[str, IndexInfo]:
        """
        Retrieve index information for a given table.

        This method scans the 'index_cat' table and collects all index records
        that match the specified table name, constructing IndexInfo objects for each.

        Args:
            table_name (str): The name of the table to retrieve index info for.
            tx (Transaction): The current transaction.

        Returns:
            Dict[str, IndexInfo]: A dictionary mapping field names to their IndexInfo.
        """
        index_infos: Dict[str, IndexInfo] = {}
        ts = TableScan(tx, "index_cat", self.__layout)

        while ts.next():
            catalog_table_name = ts.get_string("table_name")
            if catalog_table_name == table_name:
                index_name = ts.get_string("index_name")
                field_name = ts.get_string("field_name")
                layout = self.__tm.get_layout(table_name, tx)
                stat_info = self.__sm.get_stat_info(table_name, layout, tx)
                idx_info = IndexInfo(index_name, field_name, tx, layout.schema, stat_info)
                index_infos[field_name] = idx_info

        ts.close()
        return index_infos