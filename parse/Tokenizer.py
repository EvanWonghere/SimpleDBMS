import re
from typing import Union, Optional


class Tokenizer:
    # Token types
    TT_EOF = -1
    TT_NUMBER = -2
    TT_WORD = -3
    TT_FLOAT = -4

    def __init__(self, input_string: str):
        self.__tokens: list[str] = re.findall(r"\d+\.\d+|\w+|[^\w\s]", input_string)
        # print(f"intput: {input_string}, find: {self.__tokens}")
        self.__index: int = 0
        self.int_value: int = 0
        self.str_value: Optional[str] = None
        self.float_value: float = 0
        self.token_type: Union[int, str, None] = None

    def next_token(self) -> int:
        if self.__index < len(self.__tokens):
            token = self.__tokens[self.__index]
            self.__index += 1

            if token.isdigit():
                self.token_type = self.TT_NUMBER
                self.int_value = int(token)
                self.str_value = None
                self.float_value = 0
            elif self.__is_float(token):
                self.token_type = self.TT_FLOAT
                self.float_value = float(token)
                self.int_value = 0
                self.str_value = None
            elif self.__is_string(token):
                self.token_type = self.TT_WORD
                self.str_value = token
                self.int_value = 0
                self.float_value = 0
            elif token in ("'", '"'):
                self.token_type = token
                self.str_value = self.__extract_quoted_string(token)
                self.int_value = 0
                self.float_value = 0
            else:
                self.token_type = token
                self.str_value = None
                self.int_value = 0
                self.float_value = 0
        else:
            self.token_type = self.TT_EOF
            self.int_value = 0
            self.str_value = None
            self.float_value = 0

        return self.token_type

    @staticmethod
    def __is_float(token: str) -> bool:
        try:
            float(token)
            return '.' in token
        except ValueError:
            return False

    @staticmethod
    def __is_string(token: str) -> bool:
        pattern = re.compile(r"^[a-zA-Z0-9_]+$")
        return bool(pattern.match(token))

    def __extract_quoted_string(self, quote_char: str) -> str:
        result = ""
        while self.__index < len(self.__tokens):
            token = self.__tokens[self.__index]
            if token == quote_char:
                self.__index += 1
                break
            result += token
            self.__index += 1
        return result
