# -*- coding: utf-8 -*-
# @Time    : 2024/12/6 09:54
# @Author  : EvanWong
# @File    : Plan.py
# @Project : TestDB

from abc import ABC, abstractmethod
from record.Schema import Schema


class Plan(ABC):
    """
    An abstract interface representing a plan for accessing data.
    This interface describes the cost and schema of the plan,
    and can open a Scan for reading the records.

    Methods:
        open() -> Scan:
            Open a scan corresponding to this plan's query or operation.
        accessed_blocks() -> int:
            Estimate the number of blocks accessed by this plan.
        output_records() -> int:
            Estimate the number of output records produced by this plan.
        distinct_values(field_name: str) -> int:
            Estimate the number of distinct values for a given field in the plan's output.
        schema() -> Schema:
            Get the schema of the records produced by this plan.
    """

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