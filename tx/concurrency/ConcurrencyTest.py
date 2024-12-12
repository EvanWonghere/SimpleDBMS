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
            tx_a = Transaction(fm, lm, bm)
            print("Before block")
            blk1 = BlockID("testfile", 1)
            blk2 = BlockID("testfile", 2)
            print("Before pin")
            tx_a.pin(blk1)
            tx_a.pin(blk2)
            print("Tx A: request s_lock 1")
            tx_a.get_int(blk1, 0)
            print("Tx A: receive s_lock 1")
            time.sleep(1)
            print("Tx A: request s_lock 2")
            tx_a.get_int(blk2, 0)
            print("Tx A: receive s_lock 2")
            tx_a.commit()
            print("Tx A: commit")
        except InterruptedError:
            print("Dead Lock")


class B:
    def __init__(self):
        pass

    @staticmethod
    def run():
        try:
            tx_b = Transaction(fm, lm, bm)
            blk1 = BlockID("testfile", 1)
            blk2 = BlockID("testfile", 2)
            tx_b.pin(blk1)
            tx_b.pin(blk2)
            print("Tx B: request xlock 2")
            tx_b.set_int(blk2, 0, 0, False)
            print("Tx B: receive xlock 2")
            time.sleep(1)
            print("Tx B: request s_lock 1")
            tx_b.get_int(blk1, 0)
            print("Tx B: receive s_lock 1")
            tx_b.commit()
            print("Tx B: commit")
        except InterruptedError:
            print("Dead Lock")


class C:
    def __init__(self):
        pass

    @staticmethod
    def run():
        try:
            tx_c = Transaction(fm, lm, bm)
            blk1 = BlockID("testfile", 1)
            blk2 = BlockID("testfile", 2)
            tx_c.pin(blk1)
            tx_c.pin(blk2)
            time.sleep(1)
            print("Tx C: request xlock 1")
            tx_c.set_int(blk1, 0, 0, False)
            print("Tx C: receive xlock 1")
            time.sleep(1)
            print("Tx C: request s_lock 2")
            tx_c.get_int(blk2, 0)
            print("Tx C: receive s_lock 2")
            tx_c.commit()
            print("Tx C: commit")
        except InterruptedError:
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
