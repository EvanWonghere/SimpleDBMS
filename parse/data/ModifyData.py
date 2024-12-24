# -*- coding: utf-8 -*-
# @Time    : 2024/12/7 16:41
# @Author  : EvanWong
# @File    : ModifyData.py
# @Project : TestDB

from query.Expression import Expression
from query.Predicate import Predicate

class ModifyData:
    """
    Holds the information needed to modify certain records in a table.

    Attributes:
        __table_name (str): The table to be updated.
        __field_name (str): The field to be modified.
        __new_value (Expression): The new value assigned to the field.
        __predicate (Predicate): The condition to filter which records to update.
    """

    def __init__(self, table_name: str, field_name: str, new_value: Expression, predicate: Predicate):
        """
        Initialize with table name, field, new value, and predicate.

        Args:
            table_name (str): The name of the table.
            field_name (str): The field to be updated.
            new_value (Expression): The new value expression for the field.
            predicate (Predicate): The condition to filter which records are modified.
        """
        self.__table_name = table_name
        self.__field_name = field_name
        self.__new_value = new_value
        self.__predicate = predicate

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
            str: The field name to be modified.
        """
        return self.__field_name

    @property
    def new_value(self) -> Expression:
        """
        Returns:
            Expression: The new value expression for the field.
        """
        return self.__new_value

    @property
    def predicate(self) -> Predicate:
        """
        Returns:
            Predicate: The condition used to determine which records to modify.
        """
        return self.__predicate