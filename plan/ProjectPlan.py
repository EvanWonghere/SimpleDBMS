# -*- coding: utf-8 -*-
# @Time    : 2024/12/8 20:52
# @Author  : EvanWong
# @File    : ProjectPlan.py
# @Project : TestDB

from plan.Plan import Plan
from query.ProjectScan import ProjectScan
from record.Schema import Schema


class ProjectPlan(Plan):
    """
    A plan that projects a specified list of fields from an underlying plan.

    Only the specified fields (or all fields if ['*']) are included in the result.
    """

    def __init__(self, plan: Plan, fields: list[str]):
        """
        Initialize a ProjectPlan.

        Args:
            plan (Plan): The underlying plan.
            fields (list[str]): The list of fields to project.
                                If fields == ['*'], project all fields from the underlying plan.
        """
        self.__plan: Plan = plan
        self.__schema = Schema()

        # If user specified only '*', project all fields from underlying plan
        if len(fields) == 1 and fields[0] == '*':
            for f in self.__plan.schema().fields:
                fi = self.__plan.schema().get_field_info(f)
                self.__schema.add_field(f, fi)
        else:
            # Project only the specified fields
            for f in fields:
                fi = self.__plan.schema().get_field_info(f)
                self.__schema.add_field(f, fi)

    def open(self) -> ProjectScan:
        """
        Open a ProjectScan on the underlying plan's scan,
        returning only the projected fields.

        Returns:
            ProjectScan: A scan that returns only the projected fields.
        """
        base_scan = self.__plan.open()
        return ProjectScan(base_scan, self.__schema.fields)

    def accessed_blocks(self) -> int:
        """
        The cost in blocks is the same as the underlying plan.

        Returns:
            int: The number of blocks accessed by the underlying plan.
        """
        return self.__plan.accessed_blocks()

    def output_records(self) -> int:
        """
        The number of output records is the same as the underlying plan.

        Returns:
            int: The plan's output record count.
        """
        return self.__plan.output_records()

    def distinct_values(self, field_name: str) -> int:
        """
        Delegate distinct value calculation to the underlying plan.

        Args:
            field_name (str): The field to check.

        Returns:
            int: The distinct value count for the field.
        """
        return self.__plan.distinct_values(field_name)

    def schema(self) -> Schema:
        """
        Return the schema of this projection.

        Returns:
            Schema: A schema containing only the projected fields.
        """
        return self.__schema