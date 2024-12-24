# -*- coding: utf-8 -*-
# @Time    : 2024/12/2 17:15
# @Author  : EvanWong
# @File    : StatInfo.py
# @Project : TestDB

class StatInfo:
    """
    Holds statistical information about a table,
    such as the number of blocks and records.

    Attributes:
        __blocks_num (int): The number of blocks in the table.
        __records_num (int): The number of records in the table.
    """

    def __init__(self, blocks_num: int, records_num: int):
        """
        Initialize StatInfo with the given number of blocks and records.

        Args:
            blocks_num (int): The total number of blocks in the table.
            records_num (int): The total number of records in the table.
        """
        self.__blocks_num: int = blocks_num
        self.__records_num: int = records_num

    @property
    def accessed_blocks(self) -> int:
        """
        Get the number of blocks that might be accessed for scanning the table.

        Returns:
            int: The number of blocks in the table.
        """
        return self.__blocks_num

    @property
    def output_records(self) -> int:
        """
        Get the total number of records in the table.

        Returns:
            int: The number of records.
        """
        return self.__records_num

    @property
    def distinct_values(self) -> int:
        """
        Estimate the number of distinct values in the table.

        This is a naive guess for demonstration:
        For every 3 records, assume 1 distinct value increment.

        Returns:
            int: An estimated count of distinct values.
        """
        return 1 + self.__records_num // 3