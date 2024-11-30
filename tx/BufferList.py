# -*- coding: utf-8 -*-
# @Time    : 2024/11/29 15:04
# @Author  : EvanWong
# @File    : BufferList.py
# @Project : TestDB

from buffer.Buffer import Buffer
from buffer.BufferMgr import BufferMgr
from file.BlockID import BlockID


class BufferList:
    """ Manage the buffers pinned by a transaction.

    This class helps manage buffers that are "pinned" by the current transaction.
    A pinned buffer is one that the transaction has locked and is working with.
    """

    def __init__(self, bm: BufferMgr):
        self.__bm: BufferMgr = bm  # Reference to Buffer Manager
        self.__buffers: dict[BlockID, Buffer] = {}  # Stores pinned buffers by BlockID
        self.__pins: list[BlockID] = []  # Stores BlockIDs of pinned buffers

    def get_buffer(self, blk: BlockID) -> Buffer:
        """ Retrieve the buffer for a given BlockID if pinned.

        Args:
            blk (BlockID): The BlockID to get the buffer for.

        Returns:
            Buffer: The buffer corresponding to the BlockID, or None if not found.
        """
        return self.__buffers.get(blk, None)

    def pin(self, blk: BlockID):
        """ Pin a block into the buffer pool.

        If the buffer is not already pinned, it will be pinned and added to the buffers list.

        Args:
            blk (BlockID): The BlockID to pin.
        """
        buff = self.__bm.pin(blk)  # Retrieve the buffer for the block
        self.__buffers[blk] = buff
        self.__pins.append(blk)

    def unpin(self, blk: BlockID):
        """ Unpin a previously pinned block from the buffer pool.

        Args:
            blk (BlockID): The BlockID to unpin.
        """
        if blk in self.__buffers:
            buff = self.__buffers.pop(blk)
            self.__bm.unpin(buff)  # Unpin the buffer
            self.__pins.remove(blk)  # Remove the BlockID from the pinned list

    def unpin_all(self):
        """ Unpin all buffers managed by this transaction.

        This method will unpin all the buffers in the current transaction's buffer list.
        """
        for blk in self.__pins[:]:  # Copy the list to avoid modification during iteration
            self.unpin(blk)
