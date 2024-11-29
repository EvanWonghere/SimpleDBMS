# -*- coding: utf-8 -*-
# @Time    : 2024/11/28 20:55
# @Author  : EvanWong
# @File    : ConcurrencyTest.py
# @Project : TestDB
from threading import Thread

from file.BlockID import BlockID
from file.FileMgr import FileMgr
from log.LogMgr import LogMgr
from buffer.BufferMgr import BufferMgr
from tx.Transaction import Transaction
import time

"""java使用的是内核线程，可以实现真正的并发。
而Python使用的是解释器的线程，实际上并没有利用多核处理器；
所以无法真正实行并发测试，下面的代码运行时会发生错误。
"""
# ???


class A:
    def __init__(self):
        pass

    @staticmethod
    def run():
        try:
            txA = Transaction(fm, lm, bm)
            print("Before block")
            blk1 = BlockID("testfile", 1)
            blk2 = BlockID("testfile", 2)
            print("Before pin")
            txA.pin(blk1)
            txA.pin(blk2)
            print("Tx A: request slock 1")
            txA.get_int(blk1, 0)
            print("Tx A: receive slock 1")
            time.sleep(1)
            print("Tx A: request slock 2")
            txA.get_int(blk2, 0)
            print("Tx A: receive slock 2")
            txA.commit()
            print("Tx A: commit")
        except Exception as e:
            print("Dead Lock")


class B:
    def __init__(self):
        pass

    @staticmethod
    def run():
        try:
            txB = Transaction(fm, lm, bm)
            blk1 = BlockID("testfile", 1)
            blk2 = BlockID("testfile", 2)
            txB.pin(blk1)
            txB.pin(blk2)
            print("Tx B: request xlock 2")
            txB.set_int(blk2, 0, 0, False)
            print("Tx B: receive xlock 2")
            time.sleep(1)
            print("Tx B: request slock 1")
            txB.get_int(blk1, 0)
            print("Tx B: receive slock 1")
            txB.commit()
            print("Tx B: commit")
        except Exception as e:
            print("Dead Lock")


class C:
    def __init__(self):
        pass

    @staticmethod
    def run():
        try:
            txC = Transaction(fm, lm, bm)
            blk1 = BlockID("testfile", 1)
            blk2 = BlockID("testfile", 2)
            txC.pin(blk1)
            txC.pin(blk2)
            time.sleep(1)
            print("Tx C: request xlock 1")
            txC.set_int(blk1, 0, 0, False)
            print("Tx C: receive xlock 1")
            time.sleep(1)
            print("Tx C: request slock 2")
            txC.get_int(blk2, 0)
            print("Tx C: receive slock 2")
            txC.commit()
            print("Tx C: commit")
        except Exception as e:
            print("Dead Lock")


if __name__ == "__main__":
    fm = FileMgr("concurrencyTest", 400)
    lm = LogMgr(fm, "simpledb.log")
    bm = BufferMgr(fm, lm, 8)

    thread_a = Thread(target=A.run)
    thread_b = Thread(target=B.run)
    thread_c = Thread(target=C.run)

    thread_a.start()
    thread_b.start()
    thread_c.start()
