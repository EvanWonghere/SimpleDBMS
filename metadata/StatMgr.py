# -*- coding: utf-8 -*-
# @Time    : 2024/12/2 17:16
# @Author  : EvanWong
# @File    : StatMgr.py
# @Project : TestDB
from metadata.StatInfo import StatInfo
from metadata.TableMgr import TableMgr
from record.Layout import Layout
from record.TableScan import TableScan
from tx.Transaction import Transaction


class StatMgr:
    """The manager to manage the statistical information.
    To keep and refresh the information rather than store them, and there's no need to.

    Attributes:
        __MAX_CALLS_NUM (int): The maximum number of calls to execute.

        __tm (TableMgr): The TableMgr instance.
        __calls_num (int): The number of calls to execute.
        __table_stats (dict[str, StatInfo]): The table statistics.
    """

    __MAX_CALLS_NUM = 100

    def __init__(self, tm: TableMgr, tx: Transaction):
        self.__tm: TableMgr = tm
        self.__calls_num = 0
        self.__table_stats: dict[str, StatInfo] = {}
        self.__refresh_stats(tx)

    def get_stat_info(self, table_name: str, layout: Layout, tx: Transaction) -> StatInfo:
        self.__calls_num += 1
        if self.__calls_num > self.__MAX_CALLS_NUM:
            self.__refresh_stats(tx)

        info = self.__table_stats.get(table_name)
        if info is not None:
            return info
        # print(f"No such table: {table_name}, currents are {self.__table_stats.keys()}")
        info = self.__calc_table_stats(table_name, layout, tx)
        self.__table_stats[table_name] = info
        # print(f"Calculated table stats: {info}")
        return info

    def __refresh_stats(self, tx: Transaction):
        # print("Get into refresh stats")

        self.__table_stats = {}
        self.__calls_num = 0

        tcat_layout = self.__tm.get_layout("table_cat", tx)
        # print("******************************************************888")
        # print("Start refresh stats")
        ts = TableScan(tx, "table_cat", tcat_layout)
        # print("******************************************************888\n before while")
        while ts.next():
            # print("************************************************************88\n while started")
            tbl_name = ts.get_string("table_name")
            layout = self.__tm.get_layout(tbl_name, tx)
            info = self.__calc_table_stats(tbl_name, layout, tx)
            self.__table_stats[tbl_name] = info
            # print("************************************************************88\n while ended")
        ts.close()

    @staticmethod
    def __calc_table_stats(table_name: str, layout: Layout, tx: Transaction) -> StatInfo:
        records_num = 0
        blocks_num = 0

        ts = TableScan(tx, table_name, layout)
        # print("before while")
        while ts.next():
            # print("in calc while")
            records_num += 1
            blocks_num = ts.get_rid().block_number + 1
        ts.close()
        # print("after while")
        return StatInfo(blocks_num, records_num)
