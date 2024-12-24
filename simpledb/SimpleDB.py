# -*- coding: utf-8 -*-
# @Time    : 2024/12/10 8:47
# @Author  : EvanWong
# @File    : SimpleDB.py
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
    """
    The main entry class that initializes file, log, buffer management,
    metadata, and query/update planner components.

    Typically, the database starts with a new or existing directory,
    recovers if not new, and prepares for transactions.
    """

    BLOCK_SIZE = 400
    BUFF_SIZE = 8
    LOG_FILE = "simpledb.log"

    def __init__(self, dirname: str,
                 block_size: Optional[int] = None,
                 buff_size: Optional[int] = None):
        """
        Initialize the database system with a directory name.
        If block_size and buff_size are not provided, use default constants.

        Args:
            dirname (str): Directory name for the database.
            block_size (Optional[int]): Overridden block size if provided.
            buff_size (Optional[int]): Overridden buffer size if provided.
        """
        # If no explicit block/buff size, use defaults
        if block_size is None and buff_size is None:
            self.__fm = FileMgr(dirname, self.BLOCK_SIZE)
            self.__lm = LogMgr(self.__fm, self.LOG_FILE)
            self.__bm = BufferMgr(self.__fm, self.__lm, self.BUFF_SIZE)
            tx = self.new_tx
            is_new = self.__fm.is_new

            if is_new:
                print("Creating new database.")
            else:
                print("Recovering existing database.")
                tx.recover()  # log-based crash recovery

            self.__mdm = MetadataMgr(is_new, tx)
            qp = BasicQueryPlanner(self.__mdm)
            up = BasicUpdatePlanner(self.__mdm)
            self.__planner = Planner(qp, up)

            tx.commit()
        else:
            # If user provided custom block/buff sizes
            self.__fm = FileMgr(dirname, block_size)
            self.__lm = LogMgr(self.__fm, self.LOG_FILE)
            self.__bm = BufferMgr(self.__fm, self.__lm, buff_size)
            # Possibly we skip metadata manager setup or do partial init

    @property
    def new_tx(self) -> Transaction:
        """
        Create a new transaction.

        Returns:
            Transaction: A newly started transaction.
        """
        return Transaction(self.__fm, self.__lm, self.__bm)

    @property
    def metadata_mgr(self) -> MetadataMgr:
        """
        Return the metadata manager.

        Returns:
            MetadataMgr: The metadata manager for table/index/view stats.
        """
        return self.__mdm

    @property
    def planner(self) -> Planner:
        """
        Return the query/update planner.

        Returns:
            Planner: The combined planner object.
        """
        return self.__planner

    @property
    def file_mgr(self) -> FileMgr:
        """
        Return the file manager.

        Returns:
            FileMgr: The manager for file-level operations.
        """
        return self.__fm

    @property
    def log_mgr(self) -> LogMgr:
        """
        Return the log manager.

        Returns:
            LogMgr: The manager for logging and recovery.
        """
        return self.__lm

    @property
    def buffer_mgr(self) -> BufferMgr:
        """
        Return the buffer manager.

        Returns:
            BufferMgr: The manager for buffer-pool operations.
        """
        return self.__bm