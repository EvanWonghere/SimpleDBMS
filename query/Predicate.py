# -*- coding: utf-8 -*-
# @Time    : 2024/12/6 15:57
# @Author  : EvanWong
# @File    : Predicate.py
# @Project : TestDB

import math
from typing import Optional

from plan.Plan import Plan
from query.Constant import Constant
from query.Term import Term
from query.Scan import Scan
from record.Schema import Schema


class Predicate:
    """
    Represents a logical combination of terms in a WHERE clause, possibly with AND/OR operators.
    """

    def __init__(self, t: Term = None):
        """
        Initialize a Predicate with an optional single term.

        Args:
            t (Term, optional): An initial term to add to the predicate.
        """
        self.__terms: list[Term] = []
        self.logic_ops: list[str] = []  # Parallel list storing "and" / "or" between terms
        if t is not None:
            self.__terms.append(t)

    def __str__(self) -> str:
        """
        Return a string representation of the predicate (e.g., "term1 AND term2 OR term3").

        Returns:
            str: The combined string of all terms with logical operators.
        """
        if not self.__terms:
            return ""
        result = str(self.__terms[0])
        for i in range(1, len(self.__terms)):
            op = self.logic_ops[i - 1]
            result += f" {op} {str(self.__terms[i])}"
        return result

    def is_empty(self) -> bool:
        """
        Check if the predicate has no terms.

        Returns:
            bool: True if the predicate is empty, otherwise False.
        """
        return len(self.__terms) == 0

    def conjoin_with(self, predicate: "Predicate"):
        """
        Conjoin this predicate with another one using logical 'AND' for each newly added term.

        Args:
            predicate (Predicate): The other predicate to be combined.
        """
        for term in predicate.__terms:
            self.__terms.append(term)
            self.logic_ops.append("and")

    def is_satisfied(self, s: Scan) -> bool:
        """
        Check if the predicate is satisfied by the current record in the scan,
        respecting the logical operators (AND/OR).

        Args:
            s (Scan): The current scan.

        Returns:
            bool: True if the predicate is satisfied, False otherwise.

        Raises:
            ValueError: If an unrecognized logical operator is encountered.
        """
        if len(self.__terms) == 0:
            return True  # Empty predicate is trivially satisfied

        cur_result = self.__terms[0].is_satisfied(s)
        for i in range(1, len(self.__terms)):
            new_result = self.__terms[i].is_satisfied(s)
            op = self.logic_ops[i - 1]
            if op == "and":
                cur_result = cur_result and new_result
            elif op == "or":
                cur_result = cur_result or new_result
            else:
                raise ValueError(f"Unrecognized logical operator '{op}'")

        return cur_result

    def reduction_factor(self, p: Plan) -> int:
        """
        Compute the reduction factor for applying this predicate to a plan.

        For simplicity, it multiplies the reduction factor of each term.
        (This is a naive approach; more accurate heuristics may be needed.)

        Args:
            p (Plan): The plan to estimate on.

        Returns:
            int: The estimated reduction factor.
        """
        factor = 1
        for term in self.__terms:
            factor *= term.reduction_factor(p)
        return factor

    def select_sub_prediction(self, schema: Schema) -> Optional["Predicate"]:
        """
        Return a sub-predicate containing terms that apply to a given schema (for selection).

        Args:
            schema (Schema): The schema to check.

        Returns:
            Optional[Predicate]: A new Predicate with terms that apply to the schema, or None if empty.
        """
        sub_pred = Predicate()
        ops_for_sub = []
        for i, term in enumerate(self.__terms):
            if term.applies_to(schema):
                # add the term
                if not sub_pred.is_empty():
                    sub_pred.logic_ops.append(self.logic_ops[i - 1])  # replicate the logic operator
                sub_pred.__terms.append(term)

        return sub_pred if not sub_pred.is_empty() else None

    def join_sub_prediction(self, schema_1: Schema, schema_2: Schema) -> Optional["Predicate"]:
        """
        Return a sub-predicate containing terms that involve both schemas (for join).

        Only terms that don't apply solely to one schema but do apply to the combined schema
        are included (i.e., cross-schema references).

        Args:
            schema_1 (Schema): The first schema.
            schema_2 (Schema): The second schema.

        Returns:
            Optional[Predicate]: A new Predicate for join conditions, or None if empty.
        """
        sub_pred = Predicate()
        combined_schema = Schema()
        combined_schema.add_all(schema_1.fields, schema_1.infos)
        combined_schema.add_all(schema_2.fields, schema_2.infos)

        for i, term in enumerate(self.__terms):
            applies_to_1 = term.applies_to(schema_1)
            applies_to_2 = term.applies_to(schema_2)
            applies_to_combined = term.applies_to(combined_schema)

            # Condition: not solely 1 or solely 2, but yes to combined => cross-schema
            if (not applies_to_1 or not applies_to_2) and applies_to_combined:
                if not sub_pred.is_empty():
                    sub_pred.logic_ops.append(self.logic_ops[i - 1])
                sub_pred.__terms.append(term)

        return sub_pred if not sub_pred.is_empty() else None

    def equates_with_constant(self, field_name: str) -> Optional[Constant]:
        """
        Check all terms to find if any term equates the specified field to a constant.

        Args:
            field_name (str): The field to check.

        Returns:
            Optional[Constant]: The constant if found, else None.
        """
        for term in self.__terms:
            c = term.equates_with_constant(field_name)
            if c is not None:
                return c
        return None

    def equates_with_field(self, field_name: str) -> Optional[str]:
        """
        Check all terms to find if any term equates the specified field to another field.

        Args:
            field_name (str): The field to check.

        Returns:
            Optional[str]: The other field name if found, else None.
        """
        for term in self.__terms:
            c = term.equates_with_field(field_name)
            if c is not None:
                return c
        return None