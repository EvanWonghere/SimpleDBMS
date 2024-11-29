# -*- coding: utf-8 -*-
# @Time    : 2024/11/29 16:17
# @Author  : EvanWong
# @File    : RecoveryTest.py
# @Project : TestDB


from tx.Transaction import Transaction
from file.FileMgr import FileMgr
from log.LogMgr import LogMgr
from buffer.BufferMgr import BufferMgr
from file.BlockID import BlockID
from file.Page import Page


def printValues(msg):
    print(msg)
    p0 = Page(fm.block_size)
    p1 = Page(fm.block_size)
    fm.read(blk0, p0)
    fm.read(blk1, p1)
    pos = 0
    for i in range(6):
        print(p0.get_int(pos), end=" ")
        print(p1.get_int(pos), end=" ")
        pos += 4
    print(p0.get_string(30), end=" ")
    print(p1.get_string(30), end=" ")
    print("\n")


def init():
    tx1 = Transaction(fm, lm, bm)
    tx2 = Transaction(fm, lm, bm)
    tx1.pin(blk0)
    tx2.pin(blk1)
    pos = 0
    for i in range(6):
        print(i)
        tx1.set_int(blk0, pos, pos, False)
        tx2.set_int(blk1, pos, pos, False)
        pos += 4
    tx1.set_string(blk0, 30, "abc", False)
    tx2.set_string(blk1, 30, "def", False)
    tx1.commit()
    tx2.commit()
    printValues("After Init")


def modify():
    tx3 = Transaction(fm, lm, bm)
    tx4 = Transaction(fm, lm, bm)
    tx3.pin(blk0)
    tx4.pin(blk1)
    pos = 0
    for i in range(6):
        print(i)
        tx3.set_int(blk0, pos, pos + 100, True)
        tx4.set_int(blk1, pos, pos + 100, True)
        pos += 4
    tx3.set_string(blk0, 30, "uvw", True)
    tx4.set_string(blk1, 30, "xyz", True)
    bm.flush_all(3)
    bm.flush_all(4)
    printValues("After modification")
    tx3.rollback()
    printValues("After rollback")


def recover():
    tx = Transaction(fm, lm, bm)
    tx.recover()
    printValues("After recovery")


if __name__ == "__main__":
    fm = FileMgr("recoveryTest", 400)
    lm = LogMgr(fm, "simpledb.log")
    bm = BufferMgr(fm, lm, 8)
    blk0 = BlockID("testfile", 0)
    blk1 = BlockID("testfile", 1)
    if fm.length("testfile") == 0:
        init()
        modify()
    else:
        recover()
