# -*- coding: utf-8 -*-
# @Time    : 2024/11/28 14:38
# @Author  : EvanWong
# @File    : BufferTest.py
# @Project : TestDB
from buffer.Buffer import Buffer
from file.BlockID import BlockID
from file.FileMgr import FileMgr
from log.LogMgr import LogMgr


def test_buffer():
    fm = FileMgr("testdb", 400)
    lm = LogMgr(fm, "logfile.log")
    buffer = Buffer(fm, lm)

    # Test initialization
    assert buffer.block is None
    assert not buffer.is_pinned
    assert buffer.modifying_tx == -1

    # Assign a block and pin
    blk = BlockID("testfile", 1)
    buffer.assign_to_block(blk)
    assert buffer.block == blk

    buffer.pin()
    assert buffer.is_pinned

    buffer.unpin()
    assert not buffer.is_pinned

    # Modify buffer
    buffer.set_modified(1, 100)
    assert buffer.modifying_tx == 1
    buffer.flush()
    assert buffer.modifying_tx == -1

if __name__ == "__main__":
    test_buffer()
