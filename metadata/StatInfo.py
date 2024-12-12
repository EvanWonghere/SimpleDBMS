# -*- coding: utf-8 -*-
# @Time    : 2024/12/2 17:15
# @Author  : EvanWong
# @File    : StatInfo.py
# @Project : TestDB


class StatInfo:
    """The statistical information of a table.
    Stores the number of blocks and the number of records in the table.

    Attributes:
        __blocks_num (int): The number of blocks in the table.
        __records_num (int): The number of records in the table.
    """
    def __init__(self, blocks_num: int, records_num: int):
        self.__blocks_num = blocks_num
        self.__records_num = records_num

    @property
    def accessed_blocks(self) -> int:
        return self.__blocks_num

    @property
    def output_records(self) -> int:
        return self.__records_num

    @property
    def distinct_values(self) -> int:
        """
        How many distinct values are in the table.

        Returns:
            int: The estimated number of distinct values, it's completely a guess.
        """
        return 1 + self.__records_num // 3
