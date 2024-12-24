# -*- coding: utf-8 -*-
# @Time    : 2024/12/7 16:42
# @Author  : EvanWong
# @File    : QueryData.py
# @Project : TestDB

from typing import Collection
from query.Predicate import Predicate

class QueryData:
    """
    Holds the information needed for a SELECT query, including fields, tables, and a predicate.

    Attributes:
        __fields (list[str]): The fields to be selected.
        __tables (Collection[str]): The tables involved in the query.
        __predicate (Predicate): The WHERE condition, if any.
    """

    def __init__(self, fields: list[str], tables: Collection[str], predicate: Predicate):
        """
        Initialize with fields, tables, and predicate.

        Args:
            fields (list[str]): The list of fields to select.
            tables (Collection[str]): The tables from which data is selected.
            predicate (Predicate): The condition for filtering records.
        """
        self.__fields = fields
        self.__tables = tables
        self.__predicate = predicate

    def __str__(self) -> str:
        """
        Return a string representation of the query, e.g.
        "select f1, f2 from t1, t2 where <predicate>"

        Returns:
            str: The string representation of the query.
        """
        fields_str = ", ".join(self.__fields)
        tables_str = ", ".join(self.__tables)
        if self.__predicate.is_empty():
            return f"select {fields_str} from {tables_str}"
        else:
            return f"select {fields_str} from {tables_str} where {self.__predicate}"

    @property
    def fields(self) -> list[str]:
        """
        Returns:
            list[str]: The list of fields to be selected.
        """
        return self.__fields

    @property
    def tables(self) -> Collection[str]:
        """
        Returns:
            Collection[str]: The set or list of table names.
        """
        return self.__tables

    @property
    def predicate(self) -> Predicate:
        """
        Returns:
            Predicate: The condition for filtering records.
        """
        return self.__predicate