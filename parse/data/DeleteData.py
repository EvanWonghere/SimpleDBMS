# -*- coding: utf-8 -*-
# @Time    : 2024/12/7 16:41
# @Author  : EvanWong
# @File    : DeleteData.py
# @Project : TestDB

from query.Predicate import Predicate

class DeleteData:
    """
    Holds the information needed to delete records from a table based on a predicate.

    Attributes:
        __table_name (str): The table from which records will be deleted.
        __predicate (Predicate): The condition used to filter records for deletion.
    """

    def __init__(self, table_name: str, predicate: Predicate):
        """
        Initialize with the table name and deletion predicate.

        Args:
            table_name (str): The table name.
            predicate (Predicate): The condition to determine which records to delete.
        """
        self.__table_name = table_name
        self.__predicate = predicate

    @property
    def table_name(self) -> str:
        """
        Returns:
            str: The name of the table.
        """
        return self.__table_name

    @property
    def predicate(self) -> Predicate:
        """
        Returns:
            Predicate: The condition used for deleting records.
        """
        return self.__predicate