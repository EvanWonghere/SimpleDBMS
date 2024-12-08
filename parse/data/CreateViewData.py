# -*- coding: utf-8 -*-
# @Time    : 2024/12/7 16:42
# @Author  : EvanWong
# @File    : CreateViewData.py
# @Project : TestDB
from parse.data.QueryData import QueryData


class CreateViewData:
    def __init__(self, view_name: str, query_data: QueryData):
        self.__view_name = view_name
        self.__query_data = query_data

    @property
    def view_name(self) -> str:
        return self.__view_name

    @property
    def query_data(self) -> str:
        return str(self.__query_data)
