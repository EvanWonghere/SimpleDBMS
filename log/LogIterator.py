# -*- coding: utf-8 -*-
# @Time    : 2024/9/19 10:52
# @Author  : EvanWong
# @File    : LogIterator.py
# @Project : TestDB

from file.BlockID import BlockID
from file.FileMgr import FileMgr
from file.Page import Page


class LogIterator:
    """
    A class for traversing log file records in reverse order.

    Attributes:
        __fm (FileMgr): The file manager used to read the log file.
        __blk (BlockID): The current block being read.
        __p (Page): The page buffer containing the log block data.
        __current_pos (int): The current position within the block.
        __boundary (int): The location of the last written record within the block.
    """

    def __init__(self, fm: FileMgr, blk: BlockID):
        """
        Initializes the LogIterator.

        Args:
            fm (FileMgr): The file manager to read the log file.
            blk (BlockID): The starting block to begin reading from.
        """
        self.__fm = fm
        self.__blk = blk
        self.__p = Page(bytearray(fm.block_size))  # Buffer to store log block data
        self.__current_pos = 0  # Start from the beginning of the block
        self.__boundary = 0  # The position of the last written record in the block
        # Move to the provided block and initialize its content
        self.__move_to_block(blk)

    def has_next(self) -> bool:
        """
        Determines if there are more log records to read.

        The iterator checks if there are more records in the current block or if it can move to the previous block.

        Returns:
            bool: True if there are more log records to read, otherwise False.
        """
        # There is more data if the current position is before the end of the current block,
        # or if there are more blocks to read (i.e., we're not at the first block).
        return self.__current_pos < self.__fm.block_size or self.__blk.number > 0

    def next(self) -> bytearray:
        """
        Retrieves the next log record.

        This method reads the next log record in the traversal order:
        - First, it checks the current block for more records.
        - If the current block is exhausted, it moves to the previous block and resets the reading position.

        Returns:
            bytes: The next log record.

        Raises:
            RuntimeError: If there is an error reading the log record.
        """
        # If we've exhausted the current block, move to the previous block.
        if self.__current_pos == self.__fm.block_size:
            # Move to the previous block and reset to the first record position.
            self.__blk = BlockID(self.__blk.filename, self.__blk.number - 1)
            self.__move_to_block(self.__blk)

        # Retrieve the current log record from the block's buffer.
        rec: bytearray = self.__p.get_bytes(self.__current_pos)
        # Move to the next record position (record length + 4 for boundary info).
        self.__current_pos += len(rec) + 4

        return rec

    def __move_to_block(self, blk: BlockID):
        """
        Moves to the specified block and prepares the page buffer for reading.

        Args:
            blk (BlockID): The block to move to.
        """
        self.__fm.read(blk, self.__p)  # Read the block data into the page buffer
        self.__boundary = self.__p.get_int(0)  # Get the boundary location of the last written record
        self.__current_pos = self.__boundary  # Start from the last written record in the block
