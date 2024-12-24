# -*- coding: utf-8 -*-
# @Time    : 2024/12/7 16:42
# @Author  : EvanWong
# @File    : CreateTableData.py
# @Project : TestDB

from record.Schema import Schema

class CreateTableData:
    """
    Holds the information needed to create a table with a specified schema.

    Attributes:
        __table_name (str): The name of the new table.
        __schema (Schema): The schema describing the table's structure.
    """

    def __init__(self, table_name: str, schema: Schema):
        """
        Initialize with the table name and schema.

        Args:
            table_name (str): The name of the new table.
            schema (Schema): The schema defining the table's fields.
        """
        self.__table_name = table_name
        self.__schema = schema

    @property
    def table_name(self) -> str:
        """
        Returns:
            str: The name of the table.
        """
        return self.__table_name

    @property
    def schema(self) -> Schema:
        """
        Returns:
            Schema: The schema for the table.
        """
        return self.__schema