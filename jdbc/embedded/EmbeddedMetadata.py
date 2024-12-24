# -*- coding: utf-8 -*-
# @Time    : 2024/12/9 19:05
# @Author  : EvanWong
# @File    : EmbeddedMetadata.py
# @Project : TestDB

from record.FieldType import FieldType
from record.Schema import Schema

class EmbeddedMetadata:
    """
    A simplified ResultSetMetaData-like class providing metadata for each column
    in a query result or table schema.

    Attributes:
        __schema (Schema): The schema describing the columns.
    """

    def __init__(self, schema: Schema):
        """
        Initialize with a schema.

        Args:
            schema (Schema): The schema describing the result set or table fields.
        """
        self.__schema = schema

    def get_column_count(self) -> int:
        """
        Returns:
            int: The total number of columns in the schema.
        """
        return len(self.__schema.fields)

    def get_column_name(self, column: int) -> str:
        """
        Get the name of a column by its 1-based index.

        Args:
            column (int): The 1-based column index.

        Returns:
            str: The column name.
        """
        return self.__schema.fields[column - 1]

    def get_column_type(self, column: int) -> FieldType:
        """
        Get the FieldType of a column by its 1-based index.

        Args:
            column (int): The 1-based column index.

        Returns:
            FieldType: The column's FieldType.
        """
        field_name = self.get_column_name(column)
        return self.__schema.get_field_type(field_name)

    def get_column_display_size(self, column: int) -> int:
        """
        Estimate a display size for the column based on type and length.

        Args:
            column (int): The 1-based column index.

        Returns:
            int: A display size, for example in a console-based DB client.
        """
        field_name = self.get_column_name(column)
        field_type = self.get_column_type(column)
        if field_type == FieldType.INT:
            field_length = 6
        elif field_type == FieldType.FLOAT:
            field_length = 12
        else:
            # For string type, get the defined length from schema
            field_length = self.__schema.get_field_length(field_name)

        # Ensure there's space for at least the column name
        return max(len(field_name), field_length) + 1