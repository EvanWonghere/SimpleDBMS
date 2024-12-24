# -*- coding: utf-8 -*-
# @Time    : 2024/12/7 18:12
# @Author  : EvanWong
# @File    : Lexer.py
# @Project : TestDB

from typing import Collection, Optional
from parse.BadSyntaxException import BadSyntaxException
from parse.Tokenizer import Tokenizer

class Lexer:
    """
    A higher-level lexer that interprets tokens from Tokenizer for basic SQL matching.

    Attributes:
        __keywords (Optional[Collection[str]]): A set or list of SQL keywords.
        __tokenizer (Tokenizer): The underlying tokenizer.
    """

    def __init__(self, input_string: str):
        """
        Initialize the Lexer with the input string, setup keywords, and read the first token.

        Args:
            input_string (str): The SQL-like query string to lex.
        """
        self.__keywords: Optional[Collection[str]] = None
        self.__init_keywords()
        self.__tokenizer = Tokenizer(input_string)
        self.__next_token()

    def match_delim(self, delim: str) -> bool:
        """
        Check if the current token matches a specific delimiter (like ',' or '=').

        Args:
            delim (str): The delimiter to match.

        Returns:
            bool: True if matched, False otherwise.
        """
        return self.__tokenizer.token_type == delim

    def match_int_constant(self) -> bool:
        return self.__tokenizer.token_type == Tokenizer.TT_NUMBER

    def match_str_constant(self) -> bool:
        return self.__tokenizer.token_type in ("'", '"')

    def match_float_constant(self) -> bool:
        return self.__tokenizer.token_type == Tokenizer.TT_FLOAT

    def match_keyword(self, word: str) -> bool:
        """
        Check if current token is a TT_WORD and equals the given keyword (case-insensitive).
        """
        return (self.__tokenizer.token_type == Tokenizer.TT_WORD
                and self.__tokenizer.str_value.lower() == word.lower())

    def match_id(self) -> bool:
        """
        Check if current token is an identifier (TT_WORD and not a known keyword)
        or a special token like '*'.
        """
        if (self.__tokenizer.token_type == Tokenizer.TT_WORD
                and self.__tokenizer.str_value.lower() not in self.__keywords):
            return True
        if self.__tokenizer.token_type == '*':
            return True
        return False

    def eat_delim(self, delim: str):
        if not self.match_delim(delim):
            raise BadSyntaxException(f"Expected delimiter '{delim}', found '{self.__tokenizer.str_value}'")
        self.__next_token()

    def eat_int_constant(self) -> int:
        if not self.match_int_constant():
            raise BadSyntaxException("Expected an integer constant.")
        value = self.__tokenizer.int_value
        self.__next_token()
        return value

    def eat_float_constant(self) -> float:
        if not self.match_float_constant():
            raise BadSyntaxException("Expected a float constant.")
        value = self.__tokenizer.float_value
        self.__next_token()
        return value

    def eat_str_constant(self) -> str:
        if not self.match_str_constant():
            raise BadSyntaxException("Expected a string constant.")
        value = self.__tokenizer.str_value
        self.__next_token()
        return value

    def eat_keyword(self, word: str):
        """
        Consume the current token if it's a matching keyword, else raise error.
        """
        if not self.match_keyword(word):
            raise BadSyntaxException(f"Expected keyword '{word}', got '{self.__tokenizer.str_value}'")
        self.__next_token()

    def eat_id(self) -> str:
        """
        Consume the current token if it's a valid identifier, else raise error.

        Returns:
            str: The identifier (or '*').
        """
        if not self.match_id():
            raise BadSyntaxException(f"Expected an identifier, got '{self.__tokenizer.str_value}'")
        if self.__tokenizer.token_type == '*':
            ident = '*'
        else:
            ident = self.__tokenizer.str_value
        self.__next_token()
        return ident

    def __next_token(self):
        """
        Advance to the next token using Tokenizer.
        """
        try:
            self.__tokenizer.next_token()
        except RuntimeError as e:
            raise BadSyntaxException("Lexer error: " + str(e))

    def __init_keywords(self):
        """
        Initialize the set/list of known keywords for SQL-like commands.
        """
        self.__keywords = {
            "select", "from", "where", "and", "or",
            "insert", "into", "values",
            "delete", "update", "set",
            "create", "table", "int",
            "varchar", "float",
            "view", "as",
            "index", "on",
            "tables", "show"
        }