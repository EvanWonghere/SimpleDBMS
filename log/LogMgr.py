# -*- coding: utf-8 -*-
# @Time    : 2024/9/19 10:52
# @Author  : EvanWong
# @File    : LogMgr.py
# @Project : TestDB

from file.BlockID import BlockID
from file.FileMgr import FileMgr
from file.Page import Page
from log.LogIterator import LogIterator

class LogMgr:
    """
    Log manager responsible for managing log file entries.

    This class handles writing log entries to a log file, managing log blocks,
    and provides an iterator for traversing the log in reverse order.

    The structure of the log file is as follows:
        - The blocks to log follows increment order,
        - the log record inside the block follows reverse order.
        - The start position of each block stores an int value, representing the start position of the last log,
        - the start position of each log record stores an int value, representing the size of the log data in bytes.

    Attributes:
        __fm (FileMgr): The file manager used to manage the log file.
        __logfile (str): The name of the log file.
        __log_page (Page): The buffer for storing log entries.
        __current_blk (BlockID): The current block in the log file.
        __latest_LSN (int): The latest Log Sequence Number (LSN).
        __last_saved_LSN (int): The last saved Log Sequence Number (LSN).
    """

    def __init__(self, fm: FileMgr, logfile: str):
        """
        Initializes the LogMgr with a given file manager and log file.

        Args:
            fm (FileMgr): The file manager to manage the log file.
            logfile (str): The name of the log file.
        """
        self.__fm: FileMgr = fm
        self.__logfile: str = logfile
        self.__log_page: Page = Page(bytearray(fm.block_size))  # Buffer to hold log records

        # Check if the log file already exists.
        log_size: int = fm.block_num(logfile)
        if log_size != 0:
            # If the log file is empty,
            # then read the latest block to log page.
            self.__current_blk = BlockID(logfile, log_size - 1)
            fm.read(self.__current_blk, self.__log_page)
        else:
            # Otherwise, append a new block to the log file.
            self.__current_blk = self.append_new_block()

        self.__latest_LSN = 0  # The latest LSN (Log Sequence Number)
        self.__last_saved_LSN = 0  # The last saved LSN

    def flush(self, lsn: int):
        """
        Flushes the log file to disk if the LSN is greater than or equal to the latest LSN.

        This ensures that the log is saved up to the latest valid record.

        Args:
            lsn (int): The Log Sequence Number to check against.
        """
        if lsn >= self.__latest_LSN:
            self.__flush()  # Flush the current log page to disk

    @property
    def iterator(self) -> LogIterator:
        """
        Returns an iterator to traverse the log in reverse order.

        The log iterator allows for traversing the log blocks from the last written record
        to the first written record, with each block being traversed in reverse order.

        Returns:
            LogIterator: The log iterator.
        """
        self.__flush()  # Ensure that the current log page is flushed before iteration.
        return LogIterator(self.__fm, self.__current_blk)

    def append(self, log_rec: bytearray) -> int:
        """
        Appends a log record to the log file.

        This method ensures that the log record fits within the current block buffer,
        and if not, it flushes the current block and appends a new block.

        Args:
            log_rec (bytearray): The log record to append.

        Returns:
            int: The Log Sequence Number (LSN) of the appended record.
        """
        boundary = self.__log_page.get_int(0)  # Get the position of the last written record
        rec_size = len(log_rec)  # The size of the log record
        bytes_needed = rec_size + 4  # We need 4 extra bytes for boundary information

        if boundary - bytes_needed < 4:  # We need at least 4 bytes to store the position of last log.
            self.__flush()  # Flush the current block to disk
            self.__current_blk = self.append_new_block()  # Create a new block and get the new block ID
            boundary = self.__log_page.get_int(0)  # Get the new boundary location

        # Calculate the position to write the log record
        rec_pos = boundary - bytes_needed
        self.__log_page.set_bytes(rec_pos, log_rec)  # Write the log record to the page buffer
        self.__log_page.set_int(0, rec_pos)  # Update the boundary with the new position

        # Increment the LSN for the new log entry
        self.__latest_LSN += 1
        return self.__latest_LSN

    def append_new_block(self) -> BlockID:
        """
        Appends a new block to the log file and returns the new block ID.

        This method is called when a new block is needed to store more log records.

        Returns:
            BlockID: The ID of the newly appended block.
        """
        blk = self.__fm.append(self.__logfile)  # Append a new block to the log file
        self.__log_page.set_int(0, self.__fm.block_size)  # Initialize the boundary in the new block
        self.__fm.write(blk, self.__log_page)  # Write the initial state of the new block to disk
        return blk

    def __flush(self):
        """
        Flushes the current log page to the log file.

        This method ensures that the current log page is written to disk.
        """
        self.__fm.write(self.__current_blk, self.__log_page)
        self.__last_saved_LSN = self.__latest_LSN  # Update the last saved LSN
