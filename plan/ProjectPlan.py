# -*- coding: utf-8 -*-
# @Time    : 2024/12/8 20:52
# @Author  : EvanWong
# @File    : ProjectPlan.py
# @Project : TestDB
from plan.Plan import Plan
from query.ProjectScan import ProjectScan
from record.Schema import Schema


class ProjectPlan(Plan):
    def __init__(self, plan: Plan, fields: list[str]):
        self.__plan = plan
        self.__schema = Schema()
        if len(fields) == 1 and fields[0] == '*':
            for field in self.__plan.schema().fields:
                self.schema().add_field(field, self.__plan.schema().get_field_info(field))
        else:
            for field in fields:
                self.__schema.add_field(field, self.__plan.schema().get_field_info(field))

    def open(self) -> ProjectScan:
        scan = self.__plan.open()
        return ProjectScan(scan, self.__schema.fields)

    @property
    def accessed_blocks(self) -> int:
        return self.__plan.accessed_blocks()

    @property
    def output_records(self) -> int:
        return self.__plan.output_records()

    def distinct_values(self, field_name: str) -> int:
        return self.__plan.distinct_values(field_name)

    def schema(self) -> Schema:
        return self.__schema