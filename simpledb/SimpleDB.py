# -*- coding: utf-8 -*-
# @Time    : 2024/12/2 17:22
# @Author  : EvanWong
# @File    : SimpleDB.py
# @Project : TestDB


from file.FileMgr import FileMgr
from buffer.BufferMgr import BufferMgr
from log.LogMgr import LogMgr
from tx.Transaction import Transaction


class SimpleDB:
    BLOCK_SIZE = 400
    BUFF_SIZE = 8
    LOG_FILE = "simpledb.log"

    def __init__(self, dirname: str, blocksize: int = None, buffsize: int = None):
        if blocksize is None and buffsize is None:
            self.__fm = FileMgr(dirname, self.BLOCK_SIZE)
            self.__lm = LogMgr(self.__fm, self.LOG_FILE)
            self.__bm = BufferMgr(self.__fm, self.__lm, self.BUFF_SIZE)
            tx = self.newTx()
            isnew = self.__fm.isNew()
            if isnew:
                print("creating new database")
            else:
                print("recovering existing database")
                tx.recover()
            tx.commit()
        else:
            self.__fm = FileMgr(dirname, blocksize)
            self.__lm = LogMgr(self.__fm, self.LOG_FILE)
            self.__bm = BufferMgr(self.__fm, self.__lm, buffsize)

    def newTx(self) -> Transaction:
        return Transaction(self.__fm, self.__lm, self.__bm)

    def fileMgr(self) -> FileMgr:
        return self.__fm

    def logMgr(self) -> LogMgr:
        return self.__lm

    def bufferMgr(self) -> BufferMgr:
        return self.__bm

