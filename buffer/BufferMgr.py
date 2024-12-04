# -*- coding: utf-8 -*-
# @Time    : 2024/11/28 14:10
# @Author  : EvanWong
# @File    : BufferMgr.py
# @Project : TestDB

import random
import time
from collections import OrderedDict

from buffer.Buffer import Buffer
from buffer.BufferAbortException import BufferAbortException
from file.BlockID import BlockID
from file.FileMgr import FileMgr
from log.LogMgr import LogMgr


class BufferMgr:
    """
    Manages a pool of buffers for a database system with an LRU (Least Recently Used) replacement strategy.

    This class manages a fixed number of buffers that are used to read/write blocks from/to disk.
    It supports pinning/unpinning blocks, flushing buffers, and uses the LRU strategy for buffer replacement.

    Attributes:
        __MAX_TIME (int): The maximum time (in seconds) a thread will wait for a buffer before aborting.
        __buffer_pool (OrderedDict): An ordered dictionary to store buffers in LRU order (most recent to least recent).
        __num_available (int): The number of available (unpinned) buffers in the pool.
    """

    __MAX_TIME: int = 10  # Maximum wait time for buffer pinning (seconds)

    def __init__(self, fm: FileMgr, lm: LogMgr, num_buffs: int):
        """
        Initializes the buffer manager with a fixed number of buffers and sets up the LRU cache.

        Args:
            fm (FileMgr): The file manager for reading/writing blocks.
            lm (LogMgr): The log manager for managing log records.
            num_buffs (int): The number of buffers to allocate in the pool.
        """
        self.__buffer_pool: OrderedDict = OrderedDict()  # OrderedDict for LRU
        self.__num_available: int = num_buffs

        # Initialize buffers and add them to the LRU pool
        for _ in range(num_buffs):
            buffer = Buffer(fm, lm)
            self.__buffer_pool[buffer] = None  # Value can be ignored, keys are buffers
        self.__num_available = num_buffs

    @property
    def available(self) -> int:
        """
        Returns the number of available (unpinned) buffers in the pool.

        Returns:
            int: The number of available buffers.
        """
        return self.__num_available

    def flush_all(self, tx_num: int):
        """
        Flushes all buffers modified by a specific transaction.

        Args:
            tx_num (int): The transaction ID whose buffers should be flushed.
        """
        for buffer in self.__buffer_pool:
            if buffer.modifying_tx == tx_num:
                buffer.flush()

    def unpin(self, buff: Buffer):
        """
        Unpins a buffer, making it eligible for replacement if no longer pinned.

        Args:
            buff (Buffer): The buffer to unpin.
        """
        print("Buffer Mgr called unpin")
        buff.unpin()
        if not buff.is_pinned:
            self.__num_available += 1
            # Move the buffer to the end to mark it as least recently used
            self.__buffer_pool.move_to_end(buff)

    def pin(self, blk: BlockID) -> Buffer:
        """
        Pins a block to a buffer, making it unavailable for replacement.

        If the block is not already in a buffer, this method attempts to allocate an available buffer
        or waits until a buffer becomes available within a maximum time limit.

        Args:
            blk (BlockID): The block to pin.

        Returns:
            Buffer: The buffer containing the pinned block.

        Raises:
            BufferAbortException: If no buffer becomes available within the maximum wait time.
        """
        start_time = time.time()
        buff = self.__try_pin(blk)

        while buff is None and not self.__waiting_too_long(start_time):
            time.sleep(0.1)  # Sleep for a while
            buff = self.__try_pin(blk)

        if buff is None:
            raise BufferAbortException("Buffer pinning failed: No buffer available within the maximum wait time.")

        return buff

    def __waiting_too_long(self, start_time: float) -> bool:
        """
        Checks if the waiting time for a buffer exceeds the maximum limit.

        Args:
            start_time (float): The timestamp when the waiting started.

        Returns:
            bool: True if the waiting time exceeds the limit, False otherwise.
        """
        return time.time() - start_time > self.__MAX_TIME

    def __try_pin(self, blk: BlockID) -> Buffer | None:
        """
        Tries to pin a block by finding an existing buffer or allocating a new one.

        Args:
            blk (BlockID): The block to pin.

        Returns:
            Buffer | None: The buffer containing the pinned block, or None if no buffer is available.
        """
        buff = self.__find_existing_buffer(blk)
        if buff is None:
            buff = self.__choose_unpinned_buffer()
            if buff is None:
                print("Try pin failed")
                return None  # No buffer available
            buff.assign_to_block(blk)  # Assign the block to the chosen buffer

        if not buff.is_pinned:
            self.__num_available -= 1
        buff.pin()
        # Move the buffer to the front to mark it as recently used
        self.__buffer_pool.move_to_end(buff, last=False)
        return buff

    def __find_existing_buffer(self, blk: BlockID) -> Buffer | None:
        """
        Finds a buffer that already contains the specified block.

        Args:
            blk (BlockID): The block to search for.

        Returns:
            Buffer | None: The buffer containing the block, or None if not found.
        """
        for buff in self.__buffer_pool:
            if buff.block == blk:
                # Move the accessed buffer to the front to mark it as recently used
                self.__buffer_pool.move_to_end(buff, last=False)
                return buff
        return None

    def __choose_unpinned_buffer(self) -> Buffer | None:
        """
        Selects an unpinned buffer for replacement using the LRU strategy.

        Returns:
            Buffer | None: An unpinned buffer, or None if no unpinned buffer is available.
        """
        # Find the least recently used buffer (the one at the end of the OrderedDict)
        for buff in self.__buffer_pool:
            if not buff.is_pinned:
                self.__buffer_pool.move_to_end(buff, last=False)  # Move it to the front after being used
                return buff
        return None
