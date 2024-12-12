# -*- coding: utf-8 -*-
# @Time    : 2024/12/10 8:47
# @Author  : EvanWong
# @File    : SetFloatRecord.py
# @Project : TestDB
from typing import Optional

from buffer.BufferMgr import BufferMgr
from file.FileMgr import FileMgr
from log.LogMgr import LogMgr
from metadata.MetadataMgr import MetadataMgr
from plan.BasicQueryPlanner import BasicQueryPlanner
from plan.BasicUpdatePlanner import BasicUpdatePlanner
from plan.Planner import Planner
from tx.Transaction import Transaction


class SimpleDB:
    BLOCK_SIZE = 400
    BUFF_SIZE = 8
    LOG_FILE = "simpledb.log"

    def __init__(self, dirname: str, block_size: Optional[int] = None, buff_size: Optional[int] = None):
        if not block_size and not buff_size:
            self.__fm = FileMgr(dirname, self.BLOCK_SIZE)
            self.__lm = LogMgr(self.__fm, self.LOG_FILE)
            self.__bm = BufferMgr(self.__fm, self.__lm, self.BUFF_SIZE)
            tx = self.new_tx
            is_new = self.__fm.is_new

            if is_new:
                print("creating new database")
            else:
                print("recovering existing database")
                tx.recover()

            self.__mdm = MetadataMgr(is_new, tx)
            qp = BasicQueryPlanner(self.__mdm)
            up = BasicUpdatePlanner(self.__mdm)
            self.__planner = Planner(qp, up)

            tx.commit()
        else:
            self.__fm = FileMgr(dirname, block_size)
            self.__lm = LogMgr(self.__fm, self.LOG_FILE)
            self.__bm = BufferMgr(self.__fm, self.__lm, buff_size)

    @property
    def new_tx(self) -> Transaction:
        return Transaction(self.__fm, self.__lm, self.__bm)

    @property
    def metadata_mgr(self) -> MetadataMgr:
        return self.__mdm

    @property
    def planner(self) -> Planner:
        return self.__planner

    @property
    def file_mgr(self) -> FileMgr:
        return self.__fm

    @property
    def log_mgr(self) -> LogMgr:
        return self.__lm

    @property
    def buffer_mgr(self) -> BufferMgr:
        return self.__bm
