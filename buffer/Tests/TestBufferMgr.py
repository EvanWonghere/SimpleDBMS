# -*- coding: utf-8 -*-
# @Time    : 2024/11/28 15:17
# @Author  : EvanWong
# @File    : TestBufferMgr.py
# @Project : TestDB
from buffer.BufferAbortException import BufferAbortException
from buffer.BufferMgr import BufferMgr
from file.BlockID import BlockID
from file.FileMgr import FileMgr
from log.LogMgr import LogMgr


def test_buffer_manager():
    """
    Tests the BufferMgr class with various operations, including pinning,
    unpinning, and handling scenarios with limited available buffers.

    This test demonstrates buffer replacement, available buffer tracking,
    and proper exception handling when buffers are exhausted.
    """
    # Initialize FileMgr, LogMgr, and BufferMgr
    fm = FileMgr("buffertest", 400)
    lm = LogMgr(fm, "simpledb.log")
    bm = BufferMgr(fm, lm, 3)  # Buffer pool with 3 buffers

    # Array to hold references to pinned buffers
    buff = [None] * 6

    # Pin three blocks into the buffer pool
    buff[0] = bm.pin(BlockID("testfile", 0))  # Pin block 0
    buff[1] = bm.pin(BlockID("testfile", 1))  # Pin block 1
    buff[2] = bm.pin(BlockID("testfile", 2))  # Pin block 2

    # Unpin block 1 and make it available
    bm.unpin(buff[1])
    buff[1] = None

    # Re-pin block 0, which should already be in the buffer
    buff[3] = bm.pin(BlockID("testfile", 0))

    # Attempt to pin block 1 again, using an available buffer
    buff[4] = bm.pin(BlockID("testfile", 1))

    print("Available buffers:", bm.available)

    # Attempt to pin block 3 when all buffers are pinned (should raise exception)
    try:
        print("Attempting to pin block 3...")
        buff[5] = bm.pin(BlockID("testfile", 3))
    except BufferAbortException as e:
        print("Exception: No available buffers\n")

    # Unpin block 2 to make a buffer available
    bm.unpin(buff[2])
    buff[2] = None

    # Pin block 3 successfully after freeing a buffer
    buff[5] = bm.pin(BlockID("testfile", 3))

    print("Final Buffer Allocation:")
    for i, buffer in enumerate(buff):
        if buffer is not None:
            print(f"buff[{i}] pinned to block {buffer.block}")


if __name__ == "__main__":
    test_buffer_manager()
