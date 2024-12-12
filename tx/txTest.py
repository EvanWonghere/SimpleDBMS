# -*- coding: utf-8 -*-
# @Time    : 2024/11/30 00:03
# @Author  : EvanWong
# @File    : txTest.py
# @Project : TestDB
from Transaction import Transaction
from buffer.BufferMgr import BufferMgr
from file.BlockID import BlockID
from file.FileMgr import FileMgr
from log.LogMgr import LogMgr

fm = FileMgr("txTest", 400)
lm = LogMgr(fm, "simpledb.log")
bm = BufferMgr(fm, lm, 8)

tx1 = Transaction(fm, lm, bm)
blk = BlockID("testfile", 1)
tx1.pin(blk)
tx1.set_int(blk, 80, 1, False)
tx1.set_string(blk, 40, "one", False)
tx1.commit()

tx2 = Transaction(fm, lm, bm)
tx2.pin(blk)
int_val = tx2.get_int(blk, 80)
sval = tx2.get_string(blk, 40)
print("initial value at location 80 = ", int_val)
print("initial value at location 40 = " + sval)
new_int_val = int_val + 1
newSval = sval + "!"
tx2.set_int(blk, 80, new_int_val, True)
tx2.set_string(blk, 40, newSval, True)
tx2.commit()

tx3 = Transaction(fm, lm, bm)
tx3.pin(blk)
print("new value at location 80 = ", tx3.get_int(blk, 80))
print("new value at location 40 = ", tx3.get_string(blk, 40))
tx3.set_int(blk, 80, 9999, True)
print("pre-rollback value at location 80 = ", tx3.get_int(blk, 80))
tx3.rollback()

tx4 = Transaction(fm, lm, bm)
tx4.pin(blk)
print("post-rollback at location 80 = ", tx4.get_int(blk, 80))
tx4.commit()
