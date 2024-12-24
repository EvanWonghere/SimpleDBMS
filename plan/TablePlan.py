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
    """
    A plan corresponding to a full table scan of a single table.

    This plan can open a TableScan, and uses metadata manager to
    retrieve statistical info (blocks, records, distinct values).
    """

    def __init__(self, tx: Transaction, table_name: str, mdm: MetadataMgr):
        """
        Initialize a TablePlan for the specified table.

        Args:
            tx (Transaction): The current transaction.
            table_name (str): The name of the table.
            mdm (MetadataMgr): Metadata manager for retrieving layout & stats.

        Raises:
            RuntimeError: If the table metadata is not found.
        """
        self.__tx = tx
        self.__table_name = table_name
        self.__layout = mdm.get_layout(table_name, tx)
        self.__stat_info = mdm.get_stat_info(table_name, self.__layout, tx)

    def open(self) -> TableScan:
        """
        Open a TableScan for this table.

        Returns:
            TableScan: The scan over the entire table.
        """
        return TableScan(self.__tx, self.__table_name, self.__layout)

    def accessed_blocks(self) -> int:
        """
        Return the number of blocks accessed by scanning the entire table.

        Returns:
            int: The number of blocks (from statistics).
        """
        return self.__stat_info.accessed_blocks

    def output_records(self) -> int:
        """
        Return the total number of records in the table.

        Returns:
            int: The record count (from statistics).
        """
        return self.__stat_info.output_records

    def distinct_values(self, field_name: str) -> int:
        """
        Estimate the number of distinct values for a given field.

        Note:
            The current StatInfo might only store a single approximate distinct
            value count for the entire table, or might store per-field stats
            in a more advanced design.

        Returns:
            int: The estimated distinct values, possibly ignoring the field_name.
        """
        return self.__stat_info.distinct_values

    def schema(self) -> Schema:
        """
        Get the schema of the table.

        Returns:
            Schema: The table's schema.
        """
        return self.__layout.schema