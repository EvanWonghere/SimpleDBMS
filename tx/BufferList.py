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
        # print(f"Buffer list's buffers is None? {self.__buffers == {}}")
        # print(f"Buffers keys ", end="")
        # for b in self.__buffers.keys():
        #     print(b)
        return self.__buffers.get(blk)

    def pin(self, blk: BlockID):
        """ Pin a block into the buffer pool.

        If the buffer is not already pinned, it will be pinned and added to the buffers list.

        Args:
            blk (BlockID): The BlockID to pin.
        """
        buff = self.__bm.pin(blk)  # Retrieve the buffer for the block
        # print(f"Block {blk} pinned to {buff}")
        self.__buffers[blk] = buff
        self.__pins.append(blk)

    def unpin(self, blk: BlockID):
        """ Unpin a previously pinned block from the buffer pool.

        Args:
            blk (BlockID): The BlockID to unpin.
        """
        buff = self.__buffers.pop(blk, None)  # 安全地移除 blk，并避免 KeyError
        if buff:
            self.__bm.unpin(buff)
        if blk in self.__pins:
            self.__pins.remove(blk)

    def unpin_all(self):
        """ Unpin all buffers managed by this transaction.

        This method will unpin all the buffers in the current transaction's buffer list.
        """
        for blk in self.__pins[:]:  # Copy the list to avoid modification during iteration
            self.unpin(blk)
