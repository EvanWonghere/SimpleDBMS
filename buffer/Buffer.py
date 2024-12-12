# -*- coding: utf-8 -*-
# @Time    : 2024/11/28 13:52
# @Author  : EvanWong
# @File    : Buffer.py
# @Project : TestDB
from typing import Optional

from file.BlockID import BlockID
from file.FileMgr import FileMgr
from file.Page import Page
from log.LogMgr import LogMgr


class Buffer:
    """
    A single buffer in the buffer pool.

    This class encapsulates a `Page` and provides operations for managing buffer states,
    including pinning/unpinning, flushing, and modifying the buffer content.

    Attributes:
        __blk (BlockID): The block currently allocated to this buffer.
        __pins (int): The number of times this buffer has been pinned (i.e., locked for use).
        __tx_num (int): The transaction ID of the transaction that last modified this buffer.
        __lsn (int): The Log Sequence Number (LSN) of the most recent modification to this buffer.
        __fm (FileMgr): The file manager for reading and writing blocks.
        __lm (LogMgr): The log manager for flushing log records to disk.
        __contents (Page): The content of the buffer, represented by a `Page` object.
    """

    def __init__(self, fm: FileMgr, lm: LogMgr):
        """
        Initializes a new buffer with an empty page.

        Args:
            fm (FileMgr): The file manager for block I/O operations.
            lm (LogMgr): The log manager for managing log records.
        """
        self.__blk: Optional[BlockID] = None
        self.__pins: int = 0
        self.__tx_num: int = -1  # Indicates no transaction is currently modifying this buffer
        self.__lsn: int = -1  # Indicates no log record has been written
        self.__fm: FileMgr = fm
        self.__lm: LogMgr = lm
        self.__contents: Page = Page(fm.block_size)  # Initialize with an empty page buffer

    @property
    def contents(self) -> Page:
        """
        Returns the page contents of this buffer.

        Returns:
            Page: The page object representing the buffer's contents.
        """
        return self.__contents

    @property
    def block(self) -> BlockID:
        """
        Returns the block allocated to this buffer.

        Returns:
            BlockID: The block currently allocated to this buffer, or None if unallocated.
        """
        return self.__blk

    def set_modified(self, tx_num: int, lsn: int):
        """
        Marks the buffer as modified by a specific transaction and log record.

        Args:
            tx_num (int): The transaction ID that modified this buffer.
            lsn (int): The Log Sequence Number of the modification.
        """
        self.__tx_num = tx_num
        if lsn > 0:
            self.__lsn = lsn

    @property
    def is_pinned(self) -> bool:
        """
        Checks if the buffer is currently pinned.

        Returns:
            bool: True if the buffer is pinned, False otherwise.
        """
        return self.__pins > 0

    @property
    def modifying_tx(self) -> int:
        """
        Returns the ID of the transaction that last modified this buffer.

        Returns:
            int: The transaction ID, or -1 if no transaction has modified this buffer.
        """
        return self.__tx_num

    def assign_to_block(self, b: BlockID):
        """
        Assigns a block to this buffer and reads its content.

        This method flushes the buffer's current contents (if modified), then assigns the
        specified block to this buffer and reads its data into the buffer.

        Args:
            b (BlockID): The block to assign to this buffer.
        """
        self.flush()  # Ensure the current content is written to disk if modified
        self.__blk = b
        self.__fm.read(self.__blk, self.__contents)  # Load the block's data into the buffer
        self.__pins = 0  # Reset the pin count

    def flush(self):
        """
        Flushes the buffer's contents to disk if it has been modified.

        If the buffer was modified by a transaction, its contents are written to the disk,
        and the log manager ensures that all related log records are flushed before the data.

        Returns:
            None
        """
        if self.__tx_num < 0:  # No transaction has modified this buffer
            return

        # Flush the log records before writing the buffer to disk
        self.__lm.flush(self.__lsn)
        # Write the buffer's contents to disk
        self.__fm.write(self.__blk, self.__contents)
        # Reset the transaction ID to indicate no pending modifications
        self.__tx_num = -1

    def pin(self):
        """
        Pins the buffer, increasing the pin count.

        Pinning a buffer indicates that it is being actively used and should not be
        replaced or written back to disk until unpinned.
        """
        self.__pins += 1

    def unpin(self):
        """
        Unpins the buffer, decreasing the pin count.

        Unpinning a buffer indicates that it is no longer actively used. If the pin count
        reaches zero, the buffer becomes eligible for replacement.

        Returns:
            None
        """
        if self.__pins > 0:
            self.__pins -= 1
