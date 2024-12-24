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
    """
    A plan that implements the Cartesian product of two sub-plans.

    The resulting scan will iterate over every record of plan1 for each record of plan2.
    """

    def __init__(self, plan1: Plan, plan2: Plan):
        """
        Initialize a ProductPlan from two sub-plans.

        Args:
            plan1 (Plan): The first sub-plan (LHS).
            plan2 (Plan): The second sub-plan (RHS).
        """
        self.__plan1: Plan = plan1
        self.__plan2: Plan = plan2
        self.__schema = Schema()

        # Merge the schemas
        self.__schema.add_all(self.__plan1.schema().fields, self.__plan1.schema().infos)
        self.__schema.add_all(self.__plan2.schema().fields, self.__plan2.schema().infos)

    def open(self) -> ProductScan:
        """
        Open a ProductScan by opening both sub-plans' scans.

        Returns:
            ProductScan: The Cartesian product scan.
        """
        scan1 = self.__plan1.open()
        scan2 = self.__plan2.open()
        return ProductScan(scan1, scan2)

    def accessed_blocks(self) -> int:
        """
        Estimate the block accesses:
        The cost typically = plan1.cost + (plan1.output_records * plan2.cost).
        This is a naive formula for block nested loops.

        Returns:
            int: The estimated number of block accesses.
        """
        return self.__plan1.accessed_blocks() + \
               (self.__plan1.output_records() * self.__plan2.accessed_blocks())

    def output_records(self) -> int:
        """
        The output record count = plan1.records * plan2.records.

        Returns:
            int: The estimated record count of the product.
        """
        return self.__plan1.output_records() * self.__plan2.output_records()

    def distinct_values(self, field_name: str) -> Optional[int]:
        """
        The distinct values for a field is the same as whichever sub-plan
        contains the field. If the field doesn't exist, return None or raise error.

        Args:
            field_name (str): The field name.

        Returns:
            Optional[int]: The distinct value count if found, otherwise None.
        """
        if self.__plan1.schema().has_field(field_name):
            return self.__plan1.distinct_values(field_name)
        if self.__plan2.schema().has_field(field_name):
            return self.__plan2.distinct_values(field_name)
        # Optionally raise KeyError or return None.
        return None

    def schema(self) -> Schema:
        """
        Return the merged schema of both sub-plans.

        Returns:
            Schema: The combined schema from plan1 and plan2.
        """
        return self.__schema