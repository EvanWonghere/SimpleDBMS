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
    """
    A simplified ResultSet-like class for iterating over query results.

    Attributes:
        __scan (Scan): The scan opened by the plan.
        __schema (Schema): The schema describing the scan's output.
        __embedded_connection (EmbeddedConnection): The connection handling transaction.
    """

    def __init__(self, plan: Plan, ec: EmbeddedConnection):
        """
        Initialize by opening a scan from the plan.

        Args:
            plan (Plan): The query plan producing a scan.
            ec (EmbeddedConnection): The connection for transaction and error handling.
        """
        self.__scan: Scan = plan.open()
        self.__schema = plan.schema()
        self.__embedded_connection = ec

    def next(self) -> bool:
        """
        Move to the next record in the scan.

        Returns:
            bool: True if there is another record, False otherwise.

        Raises:
            Error: If a runtime error occurs, triggers a rollback.
        """
        try:
            return self.__scan.next()
        except RuntimeError as e:
            self.__embedded_connection.rollback()
            raise Error(e)

    def get_int(self, field_name: str) -> int:
        """
        Retrieve an integer from the specified field in the current record.

        Args:
            field_name (str): The field name.

        Returns:
            int: The integer value.

        Raises:
            Error: If a runtime error occurs, triggers a rollback.
        """
        try:
            return self.__scan.get_int(field_name.lower())
        except RuntimeError as e:
            self.__embedded_connection.rollback()
            raise Error(e)

    def get_float(self, field_name: str) -> float:
        """
        Retrieve a float from the specified field in the current record.

        Args:
            field_name (str): The field name.

        Returns:
            float: The float value.
        """
        try:
            return self.__scan.get_float(field_name.lower())
        except RuntimeError as e:
            self.__embedded_connection.rollback()
            raise Error(e)

    def get_string(self, field_name: str) -> str:
        """
        Retrieve a string from the specified field in the current record.

        Args:
            field_name (str): The field name.

        Returns:
            str: The string value.
        """
        try:
            return self.__scan.get_string(field_name.lower())
        except RuntimeError as e:
            self.__embedded_connection.rollback()
            raise Error(e)

    def get_metadata(self) -> EmbeddedMetadata:
        """
        Retrieve a metadata object describing the columns.

        Returns:
            EmbeddedMetadata: The metadata for columns in this result set.
        """
        return EmbeddedMetadata(self.__schema)

    def close(self):
        """
        Close the scan and commit the transaction.
        """
        self.__scan.close()
        self.__embedded_connection.commit()