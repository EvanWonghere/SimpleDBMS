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
    def __init__(self, input_string: str):
        self.__lexer = Lexer(input_string)

    @property
    def field(self) -> str:
        return self.__lexer.eat_id()

    @property
    def constant(self) -> Constant:
        if self.__lexer.match_int_constant():
            return Constant(self.__lexer.eat_int_constant())
        elif self.__lexer.match_str_constant():
            return Constant(self.__lexer.eat_str_constant())
        elif self.__lexer.match_float_constant():
            return Constant(self.__lexer.eat_float_constant())
        else:
            raise ValueError("Unknown constant")

    @property
    def expression(self) -> Expression:
        if self.__lexer.match_id():
            return Expression(field_name=self.field)
        return Expression(value=self.constant)

    @property
    def term(self) -> Term:
        lhs = self.expression
        self.__lexer.eat_delim('=')
        rhs = self.expression

        return Term(lhs, rhs)

    @property
    def predicate(self) -> Predicate:
        predicate = Predicate(self.term)
        if self.__lexer.match_keyword("and"):
            self.__lexer.eat_keyword("and")
            predicate.conjoin_with(self.predicate)
        return predicate

    def query_data(self) -> QueryData:
        self.__lexer.eat_keyword("select")
        fields = self.__select_list
        self.__lexer.eat_keyword("from")
        tables = self.__table_list

        predicate = Predicate()
        if self.__lexer.match_keyword("where"):
            self.__lexer.eat_keyword("where")
            predicate = self.predicate

        return QueryData(fields, tables, predicate)

    @property
    def __select_list(self) -> list[str]:
        sl: list[str] = [self.field]
        if self.__lexer.match_delim(','):
            self.__lexer.eat_delim(',')
            sl.extend(self.__select_list)
        return sl

    @property
    def __table_list(self) -> Collection[str]:
        tl = [self.__lexer.eat_id()]
        if self.__lexer.match_delim(','):
            self.__lexer.eat_delim(',')
            tl.extend(self.__table_list)
        return tl

    def update_command(self)\
            -> Union[InsertData, DeleteData, ModifyData, CreateTableData, CreateViewData, CreateIndexData]:
        # print("Start match keyword")
        if self.__lexer.match_keyword("insert"):
            return self.insert_data
        elif self.__lexer.match_keyword("delete"):
            return self.delete_data
        elif self.__lexer.match_keyword("update"):
            return self.modify_data
        elif self.__lexer.match_keyword("create"):
            # print("Into else")
            return self.__create_data
        else:
            raise ValueError("Unknown command")

    @property
    def __create_data(self) -> Union[CreateTableData, CreateViewData, CreateIndexData]:
        self.__lexer.eat_keyword("create")
        # print("Start create data")
        if self.__lexer.match_keyword("table"):
            # print("table matched")
            return self.create_table_data
        elif self.__lexer.match_keyword("view"):
            return self.create_view_data
        elif self.__lexer.match_keyword("index"):
            return self.create_index_data
        else:
            raise ValueError("Unknown command")

    @property
    def delete_data(self) -> DeleteData:
        self.__lexer.eat_keyword("delete")
        self.__lexer.eat_keyword("from")
        table_name = self.__lexer.eat_id()

        predicate = Predicate()
        if self.__lexer.match_keyword("where"):
            self.__lexer.eat_keyword("where")
            predicate = self.predicate

        return DeleteData(table_name, predicate)

    @property
    def insert_data(self) -> InsertData:
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

        return InsertData(table_name, fields, values)

    @property
    def __field_list(self) -> list[str]:
        fl = [self.field]
        if self.__lexer.match_delim(','):
            self.__lexer.eat_delim(',')
            fl.extend(self.__field_list)
        return fl

    @property
    def __constant_list(self) -> list[Constant]:
        cl = [self.constant]
        if self.__lexer.match_delim(','):
            self.__lexer.eat_delim(',')
            cl.extend(self.__constant_list)
        return cl

    @property
    def modify_data(self) -> ModifyData:
        self.__lexer.eat_keyword("update")
        table_name = self.__lexer.eat_id()

        self.__lexer.eat_keyword("set")
        field_name = self.field
        self.__lexer.eat_delim('=')
        new_value = self.expression

        predicate = Predicate()
        if self.__lexer.match_keyword("where"):
            self.__lexer.eat_keyword("where")
            predicate = self.predicate

        return ModifyData(table_name, field_name, new_value, predicate)

    @property
    def create_table_data(self) -> CreateTableData:
        self.__lexer.eat_keyword("table") # "create" was eaten.
        table_name = self.__lexer.eat_id()

        self.__lexer.eat_delim("(")
        # print("into definition")
        schema = self.__field_definitions
        # print(f"end definition, definitions are {schema.fields}, {schema.infos}")
        self.__lexer.eat_delim(')')
        # print("createTableData returned")
        return CreateTableData(table_name, schema)

    @property
    def __field_definitions(self) -> Schema:
        schema = self.__field_definition
        if self.__lexer.match_delim(','):
            self.__lexer.eat_delim(',')
            new_schema = self.__field_definitions
            schema.add_all(new_schema.fields, new_schema.infos)

        return schema

    @property
    def __field_definition(self) -> Schema:
        field_name = self.field
        return self.__field_type(field_name)

    def __field_type(self, field_name: str) -> Schema:
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

        return schema

    @property
    def create_view_data(self) -> CreateViewData:
        self.__lexer.eat_keyword("view") # "create" was eaten.
        view_name = self.__lexer.eat_id()
        self.__lexer.eat_keyword("as")
        query_data = self.query_data()

        return CreateViewData(view_name, query_data)

    @property
    def create_index_data(self) -> CreateIndexData:
        self.__lexer.eat_keyword("index") # "create" was eaten.
        index_name = self.__lexer.eat_id()

        self.__lexer.eat_keyword("on")
        table_name = self.__lexer.eat_id()
        self.__lexer.eat_delim('(')
        field_name = self.field
        self.__lexer.eat_delim(')')

        return CreateIndexData(index_name, table_name, field_name)
