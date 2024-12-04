# -*- coding: utf-8 -*-
# @Time    : 2024/12/3 11:43
# @Author  : EvanWong
# @File    : Index.py
# @Project : TestDB
from abc import ABC, abstractmethod

from query.Constant import Constant
from record.RID import RID


class Index(ABC):
    @abstractmethod
    def before_first(self, search_key: str):
        pass

    @abstractmethod
    def next(self) -> bool:
        pass

    @abstractmethod
    def get_data_rid(self) -> RID:
        pass

    @abstractmethod
    def insert(self, data_value: Constant, data_rid: RID):
        pass

    @abstractmethod
    def delete(self, data_value: Constant, data_rid: RID):
        pass

    @abstractmethod
    def close(self):
        pass
