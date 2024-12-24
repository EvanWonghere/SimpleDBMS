# -*- coding: utf-8 -*-
# @Time    : 2024/12/7 16:42
# @Author  : EvanWong
# @File    : CreateViewData.py
# @Project : TestDB

from parse.data.QueryData import QueryData

class CreateViewData:
    """
    Holds the information needed to create a view.

    Attributes:
        __view_name (str): The name of the new view.
        __query_data (QueryData): The query that defines the view.
    """

    def __init__(self, view_name: str, query_data: QueryData):
        """
        Initialize with the view name and query data.

        Args:
            view_name (str): The name of the view to be created.
            query_data (QueryData): The query defining the view content.
        """
        self.__view_name = view_name
        self.__query_data = query_data

    @property
    def view_name(self) -> str:
        """
        Returns:
            str: The name of the view.
        """
        return self.__view_name

    @property
    def query_data(self) -> str:
        """
        Returns:
            str: The string representation of the query data.
        """
        return str(self.__query_data)