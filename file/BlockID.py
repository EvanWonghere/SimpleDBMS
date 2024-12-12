# -*- coding: utf-8 -*-
# @Time    : 2024/9/12 20:20
# @Author  : EvanWong
# @File    : BlockID.py
# @Project : TestDB

class BlockID:
    """Represent a block in a file with its filename and block number.

    A BlockID stores the information about the file and the block number where a block is located.

    Attributes:
        __filename (str): The name of the file where the block is stored.
        __blk_num (int): The block number within the file.

    """

    def __init__(self, filename: str, blk_num: int):
        """
        Initializes a BlockID with the given filename and block number.

        Args:
            filename (str): The name of the file where the block is stored.
            blk_num (int): The block number within the file.
        """
        self.__filename = filename
        self.__blk_num = blk_num

    @property
    def filename(self) -> str:
        """Returns the filename of the block.

        Returns:
            str: The filename where the block is located.
        """
        return self.__filename

    @property
    def number(self) -> int:
        """Returns the block number.

        Returns:
            int: The block number.
        """
        return self.__blk_num

    def __eq__(self, other):
        """Checks if this BlockID is equal to another.

        Args:
            other (BlockID): Another BlockID to compare with.

        Returns:
            bool: True if the BlockID is the same, False otherwise.
        """
        if isinstance(other, BlockID):
            return self.__filename == other.__filename and self.__blk_num == other.__blk_num
        return False

    def __str__(self):
        """Returns a string representation of the BlockID.

        Returns:
            str: String representation in the format "[file: {filename}, block: {blknum}]".
        """
        return f"[file: {self.__filename}, block: {self.__blk_num}]"

    def __hash__(self):
        """Returns the hash value of the BlockID.

        Returns:
            int: The hash value for the BlockID.
        """
        return hash(str(self))
