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

    def __init__(self, dirname: str, blocksize: int = None, buffsize: int = None):
        if blocksize is None and buffsize is None:
            self.__fm = FileMgr(dirname, self.BLOCK_SIZE)
            self.__lm = LogMgr(self.__fm, self.LOG_FILE)
            self.__bm = BufferMgr(self.__fm, self.__lm, self.BUFF_SIZE)
            tx = self.newTx()
            isnew = self.__fm.is_new
            if isnew:
                print("creating new database")
            else:
                print("recovering existing database")
                tx.recover()
            self.__mdm = MetadataMgr(isnew, tx)
            qp = BasicQueryPlanner(self.__mdm)
            up = BasicUpdatePlanner(self.__mdm)
            self.__planner = Planner(qp, up)
            tx.commit()
        else:
            self.__fm = FileMgr(dirname, blocksize)
            self.__lm = LogMgr(self.__fm, self.LOG_FILE)
            self.__bm = BufferMgr(self.__fm, self.__lm, buffsize)

    def newTx(self) -> Transaction:
        return Transaction(self.__fm, self.__lm, self.__bm)

    def mdMgr(self) -> MetadataMgr:
        return self.__mdm

    def planner(self) -> Planner:
        return self.__planner

    def fileMgr(self) -> FileMgr:
        return self.__fm

    def logMgr(self) -> LogMgr:
        return self.__lm

    def bufferMgr(self) -> BufferMgr:
        return self.__bm
