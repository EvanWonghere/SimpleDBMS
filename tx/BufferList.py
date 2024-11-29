# -*- coding: utf-8 -*-
# @Time    : 2024/11/29 15:04
# @Author  : EvanWong
# @File    : BufferList.py
# @Project : TestDB
from buffer.Buffer import Buffer
from buffer.BufferMgr import BufferMgr
from file.BlockID import BlockID


class BufferList:
    def __init__(self, bm: BufferMgr):
        self.__bm: BufferMgr = bm
        self.__buffers: dict[BlockID, Buffer] = dict()
        self.__pins: list[BlockID] = list()

    def get_buffer(self, blk: BlockID) -> Buffer:
        return self.__buffers.get(blk, None)

    def pin(self, blk: BlockID):
        buff = self.__bm.pin(blk)
        self.__buffers[blk] = buff
        self.__pins.append(blk)

    def unpin(self, blk: BlockID):
        buff = self.__buffers.get(blk, None)
        if buff:
            self.__bm.unpin(buff)
            self.__pins.remove(blk)
            if blk not in self.__pins:
                self.__buffers.pop(blk)

    def unpin_all(self):
        for blk in self.__pins:
            self.unpin(blk)
