# -*- coding: utf-8 -*-
# @Time    : 2024/12/2 17:16
# @Author  : EvanWong
# @File    : StatMgr.py
# @Project : TestDB

from typing import Dict
from metadata.StatInfo import StatInfo
from metadata.TableMgr import TableMgr
from record.Layout import Layout
from record.TableScan import TableScan
from tx.Transaction import Transaction


class StatMgr:
    """
    Manages the statistical information of tables, such as the number of blocks and records.

    This manager computes and caches table statistics, and refreshes them after a certain number
    of calls to avoid overhead. No persistent storage for statistics is used.
    """

    __MAX_CALLS_NUM = 100

    def __init__(self, tm: TableMgr, tx: Transaction):
        """
        Initialize the StatMgr with a reference to the table manager and transaction.
        The constructor also triggers an initial refresh of statistics.

        Args:
            tm (TableMgr): The table manager instance.
            tx (Transaction): The current transaction.
        """
        self.__tm: TableMgr = tm
        self.__calls_num: int = 0
        self.__table_stats: Dict[str, StatInfo] = {}
        self.__refresh_stats(tx)

    def get_stat_info(self, table_name: str, layout: Layout, tx: Transaction) -> StatInfo:
        """
        Retrieve or compute the statistics for the specified table.

        If the method has been called more than __MAX_CALLS_NUM times since
        the last stats refresh, it will trigger a refresh of all table stats.

        Args:
            table_name (str): The name of the table to get stats for.
            layout (Layout): The layout of the table (schema and offsets).
            tx (Transaction): The current transaction.

        Returns:
            StatInfo: The statistical information (block count, record count) for the table.
        """
        self.__calls_num += 1
        if self.__calls_num > self.__MAX_CALLS_NUM:
            self.__refresh_stats(tx)

        info = self.__table_stats.get(table_name)
        if info is None:
            info = self.__calc_table_stats(table_name, layout, tx)
            self.__table_stats[table_name] = info
        return info

    def __refresh_stats(self, tx: Transaction):
        """
        Refresh statistics for all tables by scanning the 'table_cat' catalog table.

        Args:
            tx (Transaction): The current transaction.
        """
        self.__table_stats.clear()
        self.__calls_num = 0

        tcat_layout = self.__tm.get_layout("table_cat", tx)
        ts = TableScan(tx, "table_cat", tcat_layout)

        while ts.next():
            tbl_name = ts.get_string("table_name")
            layout = self.__tm.get_layout(tbl_name, tx)
            info = self.__calc_table_stats(tbl_name, layout, tx)
            self.__table_stats[tbl_name] = info

        ts.close()

    @staticmethod
    def __calc_table_stats(table_name: str, layout: Layout, tx: Transaction) -> StatInfo:
        """
        Calculate the number of blocks and records for a given table by scanning it.

        Args:
            table_name (str): The name of the table to scan.
            layout (Layout): The layout describing the structure of the table.
            tx (Transaction): The current transaction.

        Returns:
            StatInfo: The computed statistical information for the table.
        """
        records_count = 0
        blocks_count = 0

        ts = TableScan(tx, table_name, layout)
        while ts.next():
            records_count += 1
            blocks_count = max(blocks_count, ts.get_rid().block_number + 1)
        ts.close()

        return StatInfo(blocks_count, records_count)