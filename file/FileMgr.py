# -*- coding: utf-8 -*-
# @Time    : 2024/9/12 20:19
# @Author  : EvanWong
# @File    : FileMgr.py
# @Project : TestDB

import io
import os

from file.BlockID import BlockID
from file.Page import Page


class FileMgr:
    """
    Manages file operations, including reading, writing, and appending blocks.

    Attributes:
        __opened_files (dict): A dictionary tracking opened file handles for reuse.
        __db_directory (str): The path to the database directory where files are stored.
        __block_size (int): The size of each block in bytes.
        __is_new (bool): A flag indicating whether the database was newly created.
        __cache (dict): A cache that stores recently read blocks to reduce disk I/O.
    """

    TEMP_PREFIX = 'temp'

    def __init__(self, db_directory: str, block_size: int):
        """
        Initializes a FileMgr instance to manage file I/O operations.

        Args:
            db_directory (str): The directory where the database files are stored.
            block_size (int): The size of each block in bytes.
        """
        self.__db_directory = db_directory
        self.__block_size = block_size
        self.__is_new = not os.path.exists(db_directory)
        self.__opened_files: [str, io.BufferedRandom] = {}
        self.__cache: [BlockID, bytearray] = {}  # Cache for recently read blocks

        if self.__is_new:
            os.makedirs(db_directory)  # Create the directory if it doesn't exist.
        self.__cleanup_temp_files()  # Cleanup any leftover temporary files.

    def __cleanup_temp_files(self):
        """
        Removes temporary files from the database directory that start with the 'temp' prefix.
        This ensures that no unnecessary temporary files are left behind.
        """
        for filename in os.listdir(self.__db_directory):
            if filename.startswith(self.TEMP_PREFIX):
                os.remove(os.path.join(self.__db_directory, filename))

    def read(self, blk: BlockID, p: Page):
        """
        Reads the contents of a block into the provided Page, with caching for faster access.

        This method retrieves the data of the specified block and writes it into the given
        page object for further processing. The method caches recently read blocks to reduce
        disk I/O operations.

        Args:
            blk (BlockID): The block ID representing the block to be read.
            p (Page): The page object that will hold the content of the block.

        Raises:
            RuntimeError: If an error occurs while reading from the file.
        """
        # Check cache first
        if blk in self.__cache:
            # If the block is in the cache, directly write it to the Page
            p.write_content(self.__cache[blk])
        else:
            # If not cached, perform disk read operation
            try:
                f = self.__get_file(blk.filename)
                f.seek(blk.number * self.__block_size)  # Seek to the block's position
                buffer = bytearray(f.read(self.__block_size))  # Read the block's data into a buffer
                p.write_content(buffer)  # Write the data into the Page object

                # Cache the read content for future access
                self.__cache[blk] = buffer
            except IOError as e:
                raise RuntimeError(f"Cannot read block {blk}") from e

    def write(self, blk: BlockID, p: Page):
        """
        Writes the contents of the provided Page into a block with delayed write strategy.

        This method uses a lazy write strategy to minimize frequent disk writes. The data is
        written to the file only when necessary (e.g., when the block is evicted from cache).

        Args:
            blk (BlockID): The block ID representing the block to write to.
            p (Page): The page object containing the data to write.

        Raises:
            RuntimeError: If an error occurs while writing to the file.
        """
        try:
            f = self.__get_file(blk.filename)
            f.seek(blk.number * self.__block_size)  # Seek to the block's position
            f.write(p.content)  # Write the content to the block

            # After write, we can remove this block from cache since it's been flushed to disk
            if blk in self.__cache:
                self.__cache.pop(blk)

        except IOError as e:
            raise RuntimeError(f"Cannot write block {blk}") from e

    def append(self, filename: str) -> BlockID:
        """
        Appends a new block to the specified file.

        This method adds a new block to the file by appending a block of empty space
        and returning the new block ID.

        Args:
            filename (str): The name of the file to append the new block to.

        Returns:
            BlockID: The ID of the newly appended block.

        Raises:
            IOError: If an error occurs when unable to append a new block.
        """
        new_blk_num: int = self.block_num(filename)  # Get the current number of blocks in the file
        blk: BlockID = BlockID(filename, new_blk_num)  # Create a new block ID
        b: bytearray = bytearray(self.__block_size)  # Create an empty byte array for the block content

        try:
            f = self.__get_file(filename)
            f.seek(blk.number * self.__block_size)  # Seek to the position of the new block
            f.write(b)  # Write the empty byte array to the file to append the block
        except IOError as e:
            raise RuntimeError(f"Cannot append block {blk}") from e

        return blk

    def __get_file(self, filename: str) -> io.BufferedRandom:
        """
        Opens a file for reading and writing.

        This method checks if the file is already opened. If it is not, the file is opened in
        'rb+' mode (read-write binary mode). If the file does not exist, it will be created.

        Args:
            filename (str): The name of the file to open.

        Returns:
            io.BufferedRandom: A file object for reading and writing.
        """
        f = self.__opened_files.get(filename)
        if f is None:
            db_table: str = os.path.join(self.__db_directory, filename)
            # print(f"table path is {db_table}, exists? {os.path.exists(db_table)}")
            if not os.path.exists(db_table):
                f = open(db_table, 'w')  # Create a new file if it doesn't exist
                f.close()  # Close the file to create it
            f = open(db_table, 'rb+')  # Open the file in read-write binary mode
            self.__opened_files[filename] = f
        return f

    def block_num(self, filename: str) -> int:
        """
        Returns the number of blocks required to store the specified file.

        This method calculates the number of blocks based on the file size and block size.

        Args:
            filename (str): The name of the file.

        Returns:
            int: The number of blocks needed to store the file.

        Raises:
            RuntimeError: If an error occurs while accessing the file.
        """
        try:
            f = self.__get_file(filename)
            file_size: int = os.path.getsize(f.name)  # Get the file size
            block_num: int = int(file_size / self.__block_size)  # Calculate the number of blocks
            return block_num
        except IOError as e:
            raise RuntimeError(f"Cannot access {filename}") from e

    @property
    def is_new(self) -> bool:
        """
        Returns whether the database is newly created.

        Returns:
            bool: True if the database is newly created, False otherwise.
        """
        return self.__is_new

    @property
    def block_size(self) -> int:
        """
        Returns the block size.

        Returns:
            int: The block size in bytes.
        """
        return self.__block_size
