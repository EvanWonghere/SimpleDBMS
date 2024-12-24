# -*- coding: utf-8 -*-
# @Time    : 2024/12/2 17:15
# @Author  : EvanWong
# @File    : ViewMgr.py
# @Project : TestDB

from typing import Optional

from metadata.TableMgr import TableMgr
from record.Schema import Schema
from record.TableScan import TableScan
from tx.Transaction import Transaction


class ViewMgr:
    """
    Manages the creation and retrieval of views in the database.

    A view is defined by a name and a textual definition (SQL query or similar).
    This manager stores these views in the 'view_cat' table.

    Attributes:
        __MAX_VIEW_DEFINITION (int): The maximum length of a view's definition.
        __tm (TableMgr): The TableMgr instance used to create and retrieve table schemas.
    """

    __MAX_VIEW_DEFINITION = 100

    def __init__(self, is_new: bool, tm: TableMgr, tx: Transaction):
        """
        Initialize the ViewMgr. If the database is new, create the 'view_cat' table.

        Args:
            is_new (bool): Indicates whether the database is newly created.
            tm (TableMgr): The TableMgr instance.
            tx (Transaction): The current transaction.
        """
        self.__tm: TableMgr = tm
        if is_new:
            schema = Schema()
            schema.add_string_field("view_name", tm.MAX_NAME_LENGTH)
            schema.add_string_field("view_definition", self.__MAX_VIEW_DEFINITION)
            self.__tm.create_table("view_cat", schema, tx)

    def create_view(self, view_name: str, view_definition: str, tx: Transaction):
        """
        Create a new view record in the 'view_cat' table.

        Args:
            view_name (str): The name of the view.
            view_definition (str): The textual definition of the view.
            tx (Transaction): The current transaction.
        """
        layout = self.__tm.get_layout("view_cat", tx)
        ts = TableScan(tx, "view_cat", layout)
        ts.insert()
        ts.set_string("view_name", view_name)
        ts.set_string("view_definition", view_definition)
        ts.close()

    def get_view_definition(self, view_name: str, tx: Transaction) -> Optional[str]:
        """
        Retrieve the definition of a specified view.

        Args:
            view_name (str): The name of the view to look up.
            tx (Transaction): The current transaction.

        Returns:
            Optional[str]: The view definition if found, or None if the view does not exist.
        """
        layout = self.__tm.get_layout("view_cat", tx)
        ts = TableScan(tx, "view_cat", layout)

        definition: Optional[str] = None
        while ts.next():
            if ts.get_string("view_name") == view_name:
                definition = ts.get_string("view_definition")
                break
        ts.close()
        return definition