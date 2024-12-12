# -*- coding: utf-8 -*-
# @Time    : 2024/12/9 19:06
# @Author  : EvanWong
# @File    : EmbeddedResultSet.py
# @Project : TestDB
from sqlite3 import Error

from jdbc.embedded.EmbeddedConnection import EmbeddedConnection
from jdbc.embedded.EmbeddedMetadata import EmbeddedMetadata
from plan.Plan import Plan
from query.Scan import Scan


class EmbeddedResultSet:
    def __init__(self, plan: Plan, ec: EmbeddedConnection):
        self.__scan: Scan = plan.open()
        self.__schema = plan.schema()
        self.__embedded_connection: EmbeddedConnection = ec

    def next(self) -> bool:
        try:
            return self.__scan.next()
        except RuntimeError as e:
            self.__embedded_connection.rollback()
            raise Error(e)

    def get_int(self, field_name: str) -> int:
        try:
            field_name = field_name.lower()
            return self.__scan.get_int(field_name)
        except RuntimeError as e:
            self.__embedded_connection.rollback()
            raise Error(e)

    def get_string(self, field_name: str) -> str:
        try:
            field_name = field_name.lower()
            return self.__scan.get_string(field_name)
        except RuntimeError as e:
            self.__embedded_connection.rollback()
            raise Error(e)

    def get_float(self, field_name: str) -> float:
        try:
            field_name = field_name.lower()
            return self.__scan.get_float(field_name)
        except RuntimeError as e:
            self.__embedded_connection.rollback()
            raise Error(e)

    def get_metadata(self) -> EmbeddedMetadata:
        return EmbeddedMetadata(self.__schema)

    def close(self):
        self.__scan.close()
        self.__embedded_connection.commit()
