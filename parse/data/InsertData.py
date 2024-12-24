# -*- coding: utf-8 -*-
# @Time    : 2024/12/7 16:40
# @Author  : EvanWong
# @File    : InsertData.py
# @Project : TestDB

from query.Constant import Constant

class InsertData:
    """
    Holds the information needed to insert a new record into a table.

    Attributes:
        __table_name (str): The table into which the record is inserted.
        __fields (list[str]): The list of fields to be populated.
        __values (list[Constant]): The list of values corresponding to the fields.
    """

    def __init__(self, table_name: str, fields: list[str], values: list[Constant]):
        """
        Initialize with table name, fields, and values.

        Args:
            table_name (str): The name of the table.
            fields (list[str]): A list of field names to insert into.
            values (list[Constant]): A list of values corresponding to the fields.

        Raises:
            ValueError: If `fields` and `values` do not have the same length.
        """
        if len(fields) != len(values):
            raise ValueError("Number of fields does not match number of values for InsertData.")
        self.__table_name = table_name
        self.__fields = fields
        self.__values = values

    @property
    def table_name(self) -> str:
        """
        Returns:
            str: The name of the table.
        """
        return self.__table_name

    @property
    def fields(self) -> list[str]:
        """
        Returns:
            list[str]: The list of field names to be inserted.
        """
        return self.__fields

    @property
    def values(self) -> list[Constant]:
        """
        Returns:
            list[Constant]: The values corresponding to the fields.
        """
        return self.__values