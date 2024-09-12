# -*- coding: utf-8 -*-
# @Time    : 2024/9/12 20:20
# @Author  : EvanWong
# @File    : BlockID.py
# @Project : TestDB

class BlockID:
    """ To store some information about block.
    Record the file and block number where the block is located.

    Attributes:
        __filename: The file where the block is located.
        __blknum: The block number.
    """

    def __init__(self, filename: str, blknum: int):
        self.__filename = filename
        self.__blknum = blknum

    @property
    def filename(self) -> str:
        """
        Returns: The file where the block is located.
        """
        return self.__filename

    @property
    def number(self) -> int:
        """
        Returns: The block number
        """
        return self.__blknum

    def __eq__(self, other):
        if isinstance(other, BlockID):
            return self.__filename != other.__filename \
                or self.__blknum != other.__blknum
        return False

    def __str__(self):
        return f"[file: {self.__filename}, block: {self.__blknum}]"

    def __hash__(self):
        return hash(str(self))
