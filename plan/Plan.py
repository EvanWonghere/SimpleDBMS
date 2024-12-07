# -*- coding: utf-8 -*-
# @Time    : 2024/12/6 09:54
# @Author  : EvanWong
# @File    : Plan.py
# @Project : TestDB
from abc import ABC, abstractmethod

from record.Schema import Schema


class Plan(ABC):
    @abstractmethod
    def open(self):
        pass

    @abstractmethod
    def accessed_blocks(self) -> int:
        pass

    @abstractmethod
    def output_records(self) -> int:
        pass

    @abstractmethod
    def distinct_values(self, field_name: str) -> int:
        pass

    @abstractmethod
    def schema(self) -> Schema:
        pass
