# -*- coding: utf-8 -*-
# @Time    : 2024/12/8 20:53
# @Author  : EvanWong
# @File    : ProductPlan.py
# @Project : TestDB
from typing import Optional

from plan.Plan import Plan
from query.ProductScan import ProductScan
from record.Schema import Schema


class ProductPlan(Plan):
    def __init__(self, plan1: Plan, plan2: Plan):
        self.__plan1 = plan1
        self.__plan2 = plan2
        self.__schema = Schema()
        self.__schema.add_all(self.__plan1.schema().fields, self.__plan1.schema().infos)
        self.__schema.add_all(self.__plan2.schema().fields, self.__plan2.schema().infos)

    def open(self) -> ProductScan:
        scan1 = self.__plan1.open()
        scan2 = self.__plan2.open()
        return ProductScan(scan1, scan2)

    def accessed_blocks(self) -> int:
        return self.__plan1.accessed_blocks() + (self.__plan1.output_records() * self.__plan2.accessed_blocks())

    def output_records(self) -> int:
        return self.__plan1.output_records() * self.__plan2.output_records()

    def distinct_values(self, field_name: str) -> Optional[int]:
        if self.__plan1.schema().has_field(field_name):
            return self.__plan1.distinct_values(field_name)
        elif self.__plan2.schema().has_field(field_name):
            return self.__plan2.distinct_values(field_name)
        return None

    def schema(self) -> Schema:
        return self.__schema