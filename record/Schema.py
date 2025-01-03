# -*- coding: utf-8 -*-
# @Time    : 2024/11/30 11:40
# @Author  : EvanWong
# @File    : Schema.py
# @Project : TestDB
from dataclasses import dataclass
from typing import List, Dict

from record.FieldType import FieldType


@dataclass
class FieldInfo:
    """
    Represents information about a single field in a table schema.

    Attributes:
        type (FieldType): The type of the field (e.g., INT, STRING).
        length (int): The length of the field. For fixed-size types like INT, this can be set to 0.
                      For variable-size types like STRING, this specifies the maximum length.
    """
    type: FieldType
    length: int


class Schema:
    """
    Represents the schema of a database table.

    A schema defines the structure of a table, including each field's name, type, and length.
    This class provides methods to add fields and query schema information.
    """

    def __init__(self):
        """
        Initialize an empty schema.

        """
        self.__fields: List[str] = []
        self.__infos: Dict[str, FieldInfo] = {}

    def add_field(self, field_name: str, field_info: FieldInfo):
        """
        Add a field to the schema.

        If the field's type is INT or FLOAT, its length is set to 0 by default.

        Args:
            field_name (str): The name of the field.
            field_info (FieldInfo): The information about the field.

        Raises:
            ValueError: If the field name already exists in the schema.
        """
        if field_name in self.__infos:
            raise ValueError(f"Field '{field_name}' already exists in the schema.")
        # print(f"Field {field_name} add to schema")
        self.__fields.append(field_name)
        self.__infos[field_name] = field_info

    def add_int_field(self, field_name: str):
        """
        Add an integer field to the schema.

        Args:
            field_name (str): The name of the integer field.
        """
        self.add_field(field_name, FieldInfo(type=FieldType.INT, length=0))

    def add_float_field(self, field_name: str):
        """
        Add a float field to the schema.

        Args:
            field_name (str): The name of the float field.
        """
        self.add_field(field_name, FieldInfo(type=FieldType.FLOAT, length=0))

    def add_string_field(self, field_name: str, length: int):
        """
        Add a string field to the schema with a specified maximum length.

        Args:
            field_name (str): The name of the string field.
            length (int): The maximum length of the string field.

        Raises:
            ValueError: If the specified length is non-positive.
        """
        if length <= 0:
            raise ValueError("String field length must be positive.")
        self.add_field(field_name, FieldInfo(type=FieldType.STRING, length=length))

    def add_all(self, fields: List[str], infos: Dict[str, FieldInfo]):
        """
        Add multiple fields to the schema at once.

        Args:
            fields (List[str]): A list of field names.
            infos (Dict[str, FieldInfo]): A dictionary mapping field names to their FieldInfo.

        Raises:
            ValueError: If any field name in `fields` already exists in the schema.
        """
        for field in fields:
            if field not in infos.keys():
                raise KeyError(f"Field info for '{field}' is missing.")
            self.add_field(field, infos[field])

    @property
    def fields(self) -> List[str]:
        """
        Get the list of field names in the schema.

        Returns:
            List[str]: A list of field names.
        """
        return self.__fields

    @property
    def infos(self) -> Dict[str, FieldInfo]:
        """
        Get the mapping of field names to their FieldInfo.

        Returns:
            Dict[str, FieldInfo]: A dictionary mapping field names to FieldInfo.
        """
        return self.__infos

    def has_field(self, field_name: str) -> bool:
        """
        Check if the schema contains a specific field.

        Args:
            field_name (str): The name of the field to check.

        Returns:
            bool: True if the field exists in the schema, False otherwise.
        """
        return field_name in self.__fields

    def get_field_info(self, field_name: str) -> FieldInfo:
        """
        Retrieve the type of one specified field.

        Args:
            field_name (str): The name of the field.

        Returns:
            FieldInfo: The information about the field.

        """
        if field_name in self.__infos.keys():
            return self.__infos[field_name]
        raise KeyError(f"Field info for '{field_name}' is missing.")

    def get_field_type(self, field_name: str) -> FieldType:
        """
        Retrieve the type of one specified field.

        Args:
            field_name (str): The name of the field.

        Returns:
            FieldType: The type of the field.

        Raises:
            KeyError: If the field does not exist in the schema.
        """
        if self.has_field(field_name):
            # print(f"Field {field_name}'s type is: {self.__infos[field_name]}")
            return self.__infos[field_name].type
        else:
            raise KeyError(f"Field '{field_name}' not found in the schema.")

    def get_field_length(self, field_name: str) -> int:
        """
        Retrieve the length of a specified field.

        Args:
            field_name (str): The name of the field.

        Returns:
            int: The length of the field.

        Raises:
            KeyError: If the field does not exist in the schema.
        """
        if self.has_field(field_name):
            return self.__infos[field_name].length
        else:
            raise KeyError(f"Field '{field_name}' not found in the schema.")
