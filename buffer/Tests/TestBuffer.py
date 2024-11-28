# -*- coding: utf-8 -*-
# @Time    : 2024/11/28 15:15
# @Author  : EvanWong
# @File    : TestBuffer.py
# @Project : TestDB

from buffer.BufferMgr import BufferMgr
from file.BlockID import BlockID
from file.FileMgr import FileMgr
from log.LogMgr import LogMgr


def test_buffer_operations():
    """
    Demonstrates basic operations on the buffer manager, including pinning, unpinning,
    modifying buffer contents, and verifying block assignment in the buffer pool.
    """
    # Initialize FileMgr, LogMgr, and BufferMgr
    fm = FileMgr("buffertest", 400)
    lm = LogMgr(fm, "simpledb.log")
    bm = BufferMgr(fm, lm, 3)  # Buffer pool with 3 buffers

    # Pin a block and modify its contents
    buff1 = bm.pin(BlockID("testfile", 1))  # Pin block 1
    p = buff1.contents
    n = p.get_int(80)  # Read an integer at offset 80
    p.set_int(80, n + 1)  # Increment the value at offset 80
    buff1.set_modified(1, 0)  # Mark the buffer as modified by transaction 1
    print("The new value is:", n + 1)

    # Unpin the buffer to make it available for replacement
    bm.unpin(buff1)

    # Pin another block, which causes the buffer pool to replace a buffer
    buff2 = bm.pin(BlockID("testfile", 2))  # Pin block 2
    bm.unpin(buff2)  # Unpin block 2

    # Re-pin block 1 to verify it was written back correctly
    buff2 = bm.pin(BlockID("testfile", 1))
    p2 = buff2.contents
    p2.set_int(80, 9999)  # Set a new value at offset 80
    buff2.set_modified(1, 0)  # Mark as modified

    print("TestBuffer operations completed successfully.")


if __name__ == "__main__":
    test_buffer_operations()
