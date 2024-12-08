# -*- coding: utf-8 -*-
# @Time    : 2024/12/7 18:12
# @Author  : EvanWong
# @File    : Lexer.py
# @Project : TestDB
from typing import Collection

from parse.BadSyntaxException import BadSyntaxException
from parse.Tokenizer import Tokenizer


class Lexer:
    def __init__(self, input_string: str):
        self.__keywords: Collection[str] = self.__init_keywords()
        self.__tokenizer = Tokenizer(input_string)
        self.__next_token()

    def match_delim(self, delim: str) -> bool:
        return delim == self.__tokenizer.token_type

    def match_int_constant(self) -> bool:
        return self.__tokenizer.token_type == self.__tokenizer.TT_NUMBER

    def match_str_constant(self) -> bool:
        return self.__tokenizer.token_type in ("'", '"')

    def match_keyword(self, word: str) -> bool:
        # print(f"Type matched? {self.__tokenizer.token_type == self.__tokenizer.TT_WORD}")
        # print(f"Current type is {self.__tokenizer.token_type}")
        # print(f"Current str value {self.__tokenizer.str_value}")
        return self.__tokenizer.token_type == self.__tokenizer.TT_WORD and self.__tokenizer.str_value == word

    def match_id(self) -> bool:
        # print(f"Token type matched? {self.__tokenizer.token_type == self.__tokenizer.TT_WORD}")
        # print(f"Value matched? {self.__tokenizer.str_value not in self.__keywords}")
        # print(f"Current value: {self.__tokenizer.str_value}")
        # print(f"Current keyword: {self.__keywords}")
        return self.__tokenizer.token_type == self.__tokenizer.TT_WORD and self.__tokenizer.str_value not in self.__keywords

    def eat_delim(self, delim: str):
        if not self.match_delim(delim):
            raise BadSyntaxException
        self.__next_token()

    def eat_int_constant(self) -> int:
        if not self.match_int_constant():
            raise BadSyntaxException
        int_value = self.__tokenizer.int_value
        self.__next_token()
        return int_value

    def eat_str_constant(self) -> str:
        if not self.match_str_constant():
            raise BadSyntaxException
        str_value = self.__tokenizer.str_value
        self.__next_token()
        return str_value

    def eat_keyword(self, word: str):
        if not self.match_keyword(word):
            raise BadSyntaxException
        self.__next_token()

    def eat_id(self) -> str:
        if not self.match_id():
            raise BadSyntaxException
        str_value = self.__tokenizer.str_value
        self.__next_token()
        return str_value

    def  __next_token(self):
        try:
            self.__tokenizer.next_token()
        except RuntimeError:
            raise BadSyntaxException

    def __init_keywords(self) -> list[str]:
        return ["select", "from", "where", "and",
                "insert", "into", "values", "delete", "update", "set",
                "create", "table", "int", "varchar",
                "view", "as", "index", "on", "tables", "show"]
