# -*- coding: utf-8 -*-
# @Time    : 2024/12/7 18:18
# @Author  : EvanWong
# @File    : Parser.py
# @Project : TestDB

from typing import Collection, Union

from parse.Lexer import Lexer
from parse.data.CreateIndexData import CreateIndexData
from parse.data.CreateTableData import CreateTableData
from parse.data.CreateViewData import CreateViewData
from parse.data.DeleteData import DeleteData
from parse.data.InsertData import InsertData
from parse.data.ModifyData import ModifyData
from parse.data.QueryData import QueryData
from query.Constant import Constant
from query.Expression import Expression
from query.Predicate import Predicate
from query.Term import Term
from record.Schema import Schema

class Parser:
    """
    A simple SQL-like parser to parse queries, updates, and commands
    such as CREATE TABLE/INDEX/VIEW, INSERT, DELETE, UPDATE.

    Attributes:
        __lexer (Lexer): The lexer that provides tokens for parsing.
    """

    def __init__(self, input_string: str):
        """
        Initialize the Parser with an SQL-like string.

        Args:
            input_string (str): The SQL-like string to parse.
        """
        self.__lexer = Lexer(input_string)

    @property
    def field(self) -> str:
        """
        Parse and return an identifier representing a field name.

        Returns:
            str: The field name.
        """
        return self.__lexer.eat_id()

    @property
    def constant(self) -> Constant:
        """
        Parse and return a Constant, which could be int, float, or string.

        Returns:
            Constant: The parsed constant.

        Raises:
            ValueError: If the token isn't recognized as a constant.
        """
        if self.__lexer.match_int_constant():
            return Constant(self.__lexer.eat_int_constant())
        elif self.__lexer.match_str_constant():
            return Constant(self.__lexer.eat_str_constant())
        elif self.__lexer.match_float_constant():
            return Constant(self.__lexer.eat_float_constant())
        else:
            raise ValueError("Unknown constant token type.")

    @property
    def expression(self) -> Expression:
        """
        Parse an Expression: either a field reference or a constant.

        Returns:
            Expression: The parsed expression.
        """
        if self.__lexer.match_id():
            return Expression(field_name=self.field)
        else:
            return Expression(value=self.constant)

    @property
    def term(self) -> Term:
        """
        Parse a Term of the form <expression> = <expression>.

        Returns:
            Term: The parsed term.
        """
        lhs = self.expression
        self.__lexer.eat_delim('=')
        rhs = self.expression
        return Term(lhs, rhs)

    @property
    def predicate(self) -> Predicate:
        """
        Parse a Predicate which may be a combination of Terms joined by AND/OR.

        Returns:
            Predicate: The parsed predicate.
        """
        pred = Predicate(self.term)
        if self.__lexer.match_keyword("and"):
            self.__lexer.eat_keyword("and")
            pred.logic_ops.append("and")
            pred.conjoin_with(self.predicate)  # recursive parse for the next term
        elif self.__lexer.match_keyword("or"):
            self.__lexer.eat_keyword("or")
            pred.logic_ops.append("or")
            pred.conjoin_with(self.predicate)
        return pred

    def query_data(self) -> QueryData:
        """
        Parse a SELECT query of the form: SELECT <fields> FROM <tables> [WHERE <predicate>]

        Returns:
            QueryData: The parsed query data object.
        """
        self.__lexer.eat_keyword("select")
        fields = self.__select_list
        self.__lexer.eat_keyword("from")
        tables = self.__table_list

        pred = Predicate()
        if self.__lexer.match_keyword("where"):
            self.__lexer.eat_keyword("where")
            pred = self.predicate

        return QueryData(fields, tables, pred)

    @property
    def __select_list(self) -> list[str]:
        """
        Parse a comma-separated list of fields.

        Returns:
            list[str]: The field names.
        """
        fields = [self.field]
        if self.__lexer.match_delim(','):
            self.__lexer.eat_delim(',')
            fields.extend(self.__select_list)
        return fields

    @property
    def __table_list(self) -> Collection[str]:
        """
        Parse a comma-separated list of table names.

        Returns:
            Collection[str]: The table names.
        """
        tables = [self.__lexer.eat_id()]
        if self.__lexer.match_delim(','):
            self.__lexer.eat_delim(',')
            tables.extend(self.__table_list)
        return tables

    def update_command(self) -> Union[InsertData, DeleteData, ModifyData, CreateTableData, CreateViewData, CreateIndexData]:
        """
        Parse an update command, which may be INSERT, DELETE, UPDATE, or CREATE.

        Returns:
            Union[InsertData, DeleteData, ModifyData, CreateTableData, CreateViewData, CreateIndexData]:
            The corresponding data object.

        Raises:
            ValueError: If the command is unknown.
        """
        if self.__lexer.match_keyword("insert"):
            return self.insert_data
        elif self.__lexer.match_keyword("delete"):
            return self.delete_data
        elif self.__lexer.match_keyword("update"):
            return self.modify_data
        elif self.__lexer.match_keyword("create"):
            return self.__create_data
        else:
            raise ValueError("Unknown command type in update_command")

    @property
    def __create_data(self) -> Union[CreateTableData, CreateViewData, CreateIndexData]:
        """
        Parse the 'CREATE' subcommand: CREATE TABLE / VIEW / INDEX.

        Returns:
            Union[CreateTableData, CreateViewData, CreateIndexData]: The parsed creation command data.

        Raises:
            ValueError: If the subcommand is unknown.
        """
        self.__lexer.eat_keyword("create")
        if self.__lexer.match_keyword("table"):
            return self.create_table_data
        elif self.__lexer.match_keyword("view"):
            return self.create_view_data
        elif self.__lexer.match_keyword("index"):
            return self.create_index_data
        else:
            raise ValueError("Unknown create subcommand")

    @property
    def delete_data(self) -> DeleteData:
        """
        Parse a DELETE statement of the form: DELETE FROM <table> [WHERE <predicate>]

        Returns:
            DeleteData: The parsed delete data.
        """
        self.__lexer.eat_keyword("delete")
        self.__lexer.eat_keyword("from")
        table_name = self.__lexer.eat_id()

        pred = Predicate()
        if self.__lexer.match_keyword("where"):
            self.__lexer.eat_keyword("where")
            pred = self.predicate

        return DeleteData(table_name, pred)

    @property
    def insert_data(self) -> InsertData:
        """
        Parse an INSERT statement of the form:
        INSERT INTO <table>(<fields>) VALUES (<values>)

        Returns:
            InsertData: The parsed insert data.
        """
        self.__lexer.eat_keyword("insert")
        self.__lexer.eat_keyword("into")

        table_name = self.__lexer.eat_id()
        self.__lexer.eat_delim('(')
        fields = self.__field_list
        self.__lexer.eat_delim(')')

        self.__lexer.eat_keyword("values")
        self.__lexer.eat_delim('(')
        values = self.__constant_list
        self.__lexer.eat_delim(')')

        if len(fields) != len(values):
            raise ValueError("Fields and values must have the same length for INSERT.")

        return InsertData(table_name, fields, values)

    @property
    def __field_list(self) -> list[str]:
        """
        Parse a comma-separated list of field names.

        Returns:
            list[str]: The field names.
        """
        fields = [self.field]
        if self.__lexer.match_delim(','):
            self.__lexer.eat_delim(',')
            fields.extend(self.__field_list)
        return fields

    @property
    def __constant_list(self) -> list[Constant]:
        """
        Parse a comma-separated list of constants.

        Returns:
            list[Constant]: The list of parsed constants.
        """
        consts = [self.constant]
        if self.__lexer.match_delim(','):
            self.__lexer.eat_delim(',')
            consts.extend(self.__constant_list)
        return consts

    @property
    def modify_data(self) -> ModifyData:
        """
        Parse an UPDATE statement of the form:
        UPDATE <table> SET <field> = <expression> [WHERE <predicate>]

        Returns:
            ModifyData: The parsed modify data.
        """
        self.__lexer.eat_keyword("update")
        table_name = self.__lexer.eat_id()

        self.__lexer.eat_keyword("set")
        field_name = self.field
        self.__lexer.eat_delim('=')
        new_value = self.expression

        pred = Predicate()
        if self.__lexer.match_keyword("where"):
            self.__lexer.eat_keyword("where")
            pred = self.predicate

        return ModifyData(table_name, field_name, new_value, pred)

    @property
    def create_table_data(self) -> CreateTableData:
        """
        Parse a CREATE TABLE statement of the form:
        CREATE TABLE <table>(<field definitions>)

        Returns:
            CreateTableData: The parsed table creation data.
        """
        self.__lexer.eat_keyword("table")
        table_name = self.__lexer.eat_id()

        self.__lexer.eat_delim("(")
        schema = self.__field_definitions
        self.__lexer.eat_delim(")")

        return CreateTableData(table_name, schema)

    @property
    def __field_definitions(self) -> Schema:
        """
        Parse multiple field definitions separated by commas.

        Returns:
            Schema: A schema containing all parsed fields.
        """
        schema = self.__field_definition
        if self.__lexer.match_delim(','):
            self.__lexer.eat_delim(',')
            new_schema = self.__field_definitions
            schema.add_all(new_schema.fields, new_schema.infos)
        return schema

    @property
    def __field_definition(self) -> Schema:
        """
        Parse a single field definition: <field> <type>...
        """
        field_name = self.field
        return self.__field_type(field_name)

    def __field_type(self, field_name: str) -> Schema:
        """
        Parse the type of a field and return a Schema containing it.

        Raises:
            RuntimeError: If the field type is unrecognized or missing.
        """
        schema = Schema()
        if self.__lexer.match_keyword("int"):
            self.__lexer.eat_keyword("int")
            schema.add_int_field(field_name)
        elif self.__lexer.match_keyword("varchar"):
            self.__lexer.eat_keyword("varchar")
            self.__lexer.eat_delim('(')
            str_len = self.__lexer.eat_int_constant()
            self.__lexer.eat_delim(')')
            schema.add_string_field(field_name, str_len)
        elif self.__lexer.match_keyword("float"):
            self.__lexer.eat_keyword("float")
            schema.add_float_field(field_name)
        else:
            raise RuntimeError(f"Unknown field type for field '{field_name}'.")
        return schema

    @property
    def create_view_data(self) -> CreateViewData:
        """
        Parse a CREATE VIEW statement of the form:
        CREATE VIEW <view_name> AS <query>

        Returns:
            CreateViewData: The parsed view creation data.
        """
        self.__lexer.eat_keyword("view")
        view_name = self.__lexer.eat_id()
        self.__lexer.eat_keyword("as")
        qd = self.query_data()
        return CreateViewData(view_name, qd)

    @property
    def create_index_data(self) -> CreateIndexData:
        """
        Parse a CREATE INDEX statement of the form:
        CREATE INDEX <index_name> ON <table>(<field>)

        Returns:
            CreateIndexData: The parsed index creation data.
        """
        self.__lexer.eat_keyword("index")
        index_name = self.__lexer.eat_id()

        self.__lexer.eat_keyword("on")
        table_name = self.__lexer.eat_id()
        self.__lexer.eat_delim('(')
        field_name = self.field
        self.__lexer.eat_delim(')')

        return CreateIndexData(index_name, table_name, field_name)