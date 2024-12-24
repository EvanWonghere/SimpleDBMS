# -*- coding: utf-8 -*-
# @Time    : 2024/12/3 19:56
# @Author  : EvanWong
# @File    : MetadataMgr.py
# @Project : TestDB

from typing import Dict
from metadata.IndexInfo import IndexInfo
from metadata.IndexMgr import IndexMgr
from metadata.StatInfo import StatInfo
from metadata.StatMgr import StatMgr
from metadata.TableMgr import TableMgr
from metadata.ViewMgr import ViewMgr
from record.Layout import Layout
from record.Schema import Schema
from tx.Transaction import Transaction


class MetadataMgr:
    """
    A facade for managing all database metadata, including tables, views, indexes, and statistics.

    This class hides the internal managers (TableMgr, ViewMgr, IndexMgr, StatMgr) and provides
    simplified interfaces for creating and retrieving metadata.

    Attributes:
        __tm (TableMgr): The manager for table creation and layout retrieval.
        __vm (ViewMgr): The manager for creating and retrieving view definitions.
        __sm (StatMgr): The manager for table statistics.
        __im (IndexMgr): The manager for creating and retrieving index information.
    """

    def __init__(self, is_new: bool, tx: Transaction):
        """
        Initialize the MetadataMgr with references to all internal managers.

        Args:
            is_new (bool): True if this is a new database; False otherwise.
            tx (Transaction): The current transaction.
        """
        self.__tm = TableMgr(is_new, tx)
        self.__vm = ViewMgr(is_new, self.__tm, tx)
        self.__sm = StatMgr(self.__tm, tx)
        self.__im = IndexMgr(is_new, self.__tm, self.__sm, tx)

    def create_table(self, table_name: str, schema: Schema, tx: Transaction):
        """
        Create a new table with the given schema.

        Args:
            table_name (str): The name of the new table.
            schema (Schema): The schema defining the table's fields.
            tx (Transaction): The current transaction.
        """
        self.__tm.create_table(table_name, schema, tx)

    def get_layout(self, table_name: str, tx: Transaction) -> Layout:
        """
        Retrieve the layout for the specified table.

        Args:
            table_name (str): The table name.
            tx (Transaction): The current transaction.

        Returns:
            Layout: The layout describing the table's structure.
        """
        return self.__tm.get_layout(table_name, tx)

    def create_view(self, view_name: str, view_definition: str, tx: Transaction):
        """
        Create a new view with the given definition.

        Args:
            view_name (str): The name of the view.
            view_definition (str): The textual definition for the view.
            tx (Transaction): The current transaction.
        """
        self.__vm.create_view(view_name, view_definition, tx)

    def get_view_definition(self, view_name: str, tx: Transaction) -> str:
        """
        Retrieve the definition of a specified view.

        Args:
            view_name (str): The name of the view.
            tx (Transaction): The current transaction.

        Returns:
            str: The definition of the view, or None if the view does not exist.
        """
        return self.__vm.get_view_definition(view_name, tx)

    def create_index(self, index_name: str, table_name: str, field_name: str, tx: Transaction):
        """
        Create a new index for a specified table and field.

        Args:
            index_name (str): The name of the new index.
            table_name (str): The name of the table.
            field_name (str): The name of the field to index.
            tx (Transaction): The current transaction.
        """
        self.__im.create_index(index_name, table_name, field_name, tx)

    def get_index_info(self, table_name: str, tx: Transaction) -> Dict[str, IndexInfo]:
        """
        Retrieve a dictionary of field-to-IndexInfo mappings for the specified table.

        Args:
            table_name (str): The table name.
            tx (Transaction): The current transaction.

        Returns:
            Dict[str, IndexInfo]: A mapping from field names to IndexInfo objects.
        """
        return self.__im.get_index_info(table_name, tx)

    def get_stat_info(self, table_name: str, layout: Layout, tx: Transaction) -> StatInfo:
        """
        Retrieve statistical information for the specified table.

        Args:
            table_name (str): The table name.
            layout (Layout): The layout describing the table's structure.
            tx (Transaction): The current transaction.

        Returns:
            StatInfo: Statistical information about the table (number of blocks, records, etc.).
        """
        return self.__sm.get_stat_info(table_name, layout, tx)