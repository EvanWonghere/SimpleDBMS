# -*- coding: utf-8 -*-
# @Time    : 2024/12/8 20:23
# @Author  : EvanWong
# @File    : SelectPlan.py
# @Project : TestDB
from plan.Plan import Plan
from query.Predicate import Predicate
from query.SelectScan import SelectScan
from record.Schema import Schema


class SelectPlan(Plan):
    def __init__(self, plan: Plan, predicate: Predicate):
        self.__plan: Plan = plan
        self.__predicate: Predicate = predicate

    def open(self) -> SelectScan:
        scan = self.__plan.open()
        return SelectScan(scan, self.__predicate)

    def accessed_blocks(self) -> int:
        return self.__plan.accessed_blocks()

    @property
    def output_records(self) -> int:
        return self.__plan.output_records() // self.__predicate.reduction_factor(self.__plan)

    def distinct_values(self, field_name: str) -> int:
        if self.__predicate.equates_with_constant(field_name) is not None:
            return 1
        new_field_name = self.__predicate.equates_with_field(field_name)
        if new_field_name is None:
            return self.__plan.distinct_values(field_name)
        return min(self.__plan.distinct_values(field_name), self.__plan.distinct_values(new_field_name))

    def schema(self) -> Schema:
        return self.__plan.schema()