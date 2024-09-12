# -*- coding: utf-8 -*-
# @Time    : 2024/9/12 20:19
# @Author  : EvanWong
# @File    : FileMgr.py
# @Project : TestDB
import io
import os.path

from file.BlockID import BlockID
from file.Page import Page


class FileMgr:
    """ File Manager
    Read the disk contents into the buffer, and read and store the data.

    Attributes:
        __opened_files: The dictionary used to record the opened files.
        __db_directory: The string used to represent the name of the database.
        __block_size:The size of a data block.
        __is_new: To represent whether a database is newly created.
    """

    def __init__(self, dbDirectory: str, blockSize: int):
        self.__db_directory = dbDirectory
        self.__block_size = blockSize
        self.__is_new = not os.path.exists(dbDirectory)
        self.__opened_files = {}

        # If the DB is new, make directory
        if self.__is_new:
            os.makedirs(dbDirectory)
        # Remove any remaining temporary tables
        for filename in os.listdir(dbDirectory):
            if filename.startswith('temp'):
                os.remove(os.path.join(dbDirectory, filename))

    def read(self, blk: BlockID, p: Page):
        """
        Read the contents of the block into buffer.

        Args:
            blk: The block to read.
            p: The buffer to read into.

        Returns: None

        Raises:
            IOError: An error occurred when unable to read into buffer.
        """
        try:
            f = self.__get_file(blk.filename)
            f.seek(blk.number * self.__block_size)
            f.readinto(p.content)
        except IOError as e:
            raise RuntimeError(f"Cannot read block {blk}") from e

    def write(self, blk: BlockID, p: Page):
        """
        Read the contents of the buffer into block.

        Args:
            blk: The block to write in.
            p: The buffer to write.

        Returns: None

        Raises:
            IOError: An error occurred when unable to write into block.
        """
        try:
            f = self.__get_file(blk.filename)
            f.seek(blk.number * self.__block_size)
            f.write(p.content)
        except IOError as e:
            raise RuntimeError(f"Cannot write block {blk}") from e

    def append(self, filename: str) -> BlockID:
        """
        To add a new block.

        Args:
            filename: The file's name to add into the new block.

        Returns: The new block.

        Raises:
            IOError: An error occurred when unable to add a new block.
        """
        new_blk_num: int = self.length(filename)
        blk: BlockID = BlockID(filename, new_blk_num)
        b: bytearray = bytearray(self.__block_size)

        try:
            f = self.__get_file(filename)
            f.seek(blk.number * self.__block_size)
            f.write(b)
        except IOError as e:
            raise RuntimeError(f"Cannot append block {blk}") from e
        return blk

    @property
    def is_new(self) -> bool:
        return self.__is_new

    @property
    def block_size(self) -> int:
        return self.__block_size

    def length(self, filename: str) -> int:
        """
        Calculate the number of blocks needed to store the file.

        Args:
            filename: The file to store.

        Returns: The number of blocks.
        """
        try:
            f = self.__get_file(filename)
            file_size: int = os.path.getsize(f.name)
            block_num: int = int(file_size / self.__block_size)
            return block_num
        except IOError as e:
            raise RuntimeError(f"Cannot access {filename}") from e

    def __get_file(self, filename: str) -> io.BufferedRandom:
        """
        Open file in a read/write synchronized manner

        Args:
            filename: The file to open.

        Returns: Opened file.
        """
        f = self.__opened_files.get(filename)
        if f is None:  # If the file was not opened
            db_table: str = os.path.join(self.__db_directory, filename)
            if not os.path.exists(db_table):  # If the file is not exists, creates it.
                f = open(db_table, 'w')
                f.close()
            f = open(db_table, 'rb+')
            self.__opened_files[filename] = f
        return f
