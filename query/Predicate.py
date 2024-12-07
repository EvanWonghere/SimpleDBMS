# -*- coding: utf-8 -*-
# @Time    : 2024/12/6 15:57
# @Author  : EvanWong
# @File    : Predicate.py
# @Project : TestDB
from plan.Plan import Plan
from query.Constant import Constant
from query.Scan import Scan
from query.Term import Term
from record.Schema import Schema


class Predicate:
    def __init__(self, t: Term=None):
        self.__terms: list[Term] = []
        if t is not None:
            self.__terms.append(t)

    def __str__(self):
        if not self.__terms:
            return ""
        return " and ".join(str(term) for term in self.__terms)

    def conjoin_with(self, predicate):
        self.__terms.extend(predicate.__terms)

    def is_satisfied(self, s: Scan) -> bool:
        for term in self.__terms:
            if not term.is_satisfied(s):
                return False
        return True

    def reduction_factor(self, p: Plan) -> int:
        factor: int = 1
        for term in self.__terms:
            factor *= term.reduction_factor(p)
        return factor

    def select_sub_prediction(self, schema: Schema):
        res: Predicate = Predicate()
        for term in self.__terms:
            if term.applies_to(schema):
                res.__terms.append(term)
        return res if len(res.__terms) > 0 else None

    def join_sub_prediction(self, schema_1: Schema, schema_2: Schema):
        res: Predicate = Predicate()
        new_schema = Schema()
        new_schema.add_all(schema_1.fields, schema_1.infos)
        new_schema.add_all(schema_2.fields, schema_2.infos)

        for term in self.__terms:
            if (not term.applies_to(schema_1) and (not term.applies_to(schema_2))
                    and term.applies_to(new_schema)):
                res.__terms.append(term)

        return res if len(res.__terms) > 0 else None

    def equates_with_constant(self, field_name: str) -> Constant | None:
        for term in self.__terms:
            c: Constant = term.equates_with_constant(field_name)
            if c is not None:
                return c
        return None

    def equates_with_field(self, field_name: str) -> str | None:
        for term in self.__terms:
            c: str = term.equates_with_field(field_name)
            if c is not None:
                return c
        return None
