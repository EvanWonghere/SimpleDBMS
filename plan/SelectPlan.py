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
    """
    A plan that applies a selection predicate on an underlying plan.

    The resulting scan will only return records satisfying the predicate.
    """

    def __init__(self, plan: Plan, predicate: Predicate):
        """
        Initialize a SelectPlan with an underlying plan and a predicate.

        Args:
            plan (Plan): The underlying plan.
            predicate (Predicate): The selection predicate.
        """
        self.__plan = plan
        self.__predicate = predicate

    def open(self) -> SelectScan:
        """
        Open a SelectScan over the underlying plan's scan, filtering by the predicate.

        Returns:
            SelectScan: A scan that filters the underlying records.
        """
        base_scan = self.__plan.open()
        return SelectScan(base_scan, self.__predicate)

    def accessed_blocks(self) -> int:
        """
        The cost in blocks is the same as the underlying plan.

        Returns:
            int: The number of blocks accessed.
        """
        return self.__plan.accessed_blocks()

    def output_records(self) -> int:
        """
        Estimate the number of output records by dividing
        the underlying plan's output records by the predicate's reduction factor.

        Returns:
            int: The estimated number of output records.
        """
        # use "self.__plan.output_records()"
        # and "self.__predicate.reduction_factor(self.__plan)"
        original = self.__plan.output_records()
        factor = self.__predicate.reduction_factor(self.__plan)
        # Avoid division by zero or factor=∞
        if factor == 0 or factor == float('inf'):
            return 0
        return original // factor

    def distinct_values(self, field_name: str) -> int:
        """
        Estimate distinct values for a field after predicate is applied.

        If the predicate equates this field to a constant, distinct is 1.
        If equates this field to another field, distinct is the min
        of distinct values of both fields. Otherwise, delegate to underlying plan.

        Args:
            field_name (str): The field to check.

        Returns:
            int: The estimated number of distinct values.
        """
        c = self.__predicate.equates_with_constant(field_name)
        if c is not None:
            return 1

        f2 = self.__predicate.equates_with_field(field_name)
        if f2 is not None:
            return min(
                self.__plan.distinct_values(field_name),
                self.__plan.distinct_values(f2)
            )
        return self.__plan.distinct_values(field_name)

    def schema(self) -> Schema:
        """
        Return the schema of the underlying plan.

        Returns:
            Schema: The schema of the records.
        """
        return self.__plan.schema()