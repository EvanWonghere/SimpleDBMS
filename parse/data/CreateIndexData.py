# -*- coding: utf-8 -*-
# @Time    : 2024/12/7 17:05
# @Author  : EvanWong
# @File    : CreateIndexData.py
# @Project : TestDB

class CreateIndexData:
    """
    Holds the information needed to create an index on a table field.

    Attributes:
        __index_name (str): The name of the index to be created.
        __table_name (str): The name of the table on which the index is built.
        __field_name (str): The field of the table to be indexed.
    """

    def __init__(self, index_name: str, table_name: str, field_name: str):
        """
        Initialize with the names of the index, table, and field.

        Args:
            index_name (str): The name of the new index.
            table_name (str): The name of the table.
            field_name (str): The field to index.
        """
        self.__index_name = index_name
        self.__table_name = table_name
        self.__field_name = field_name

    @property
    def index_name(self) -> str:
        """
        Returns:
            str: The index name.
        """
        return self.__index_name

    @property
    def table_name(self) -> str:
        """
        Returns:
            str: The table name.
        """
        return self.__table_name

    @property
    def field_name(self) -> str:
        """
        Returns:
            str: The field name to be indexed.
        """
        return self.__field_name