import re
from typing import Union

class Tokenizer:
    TT_EOF = -1
    TT_NUMBER = -2
    TT_WORD = -3

    def __init__(self, input_string: str):
        self.__tokens: list[str] = re.findall(r"\w+|[^\w\s]", input_string)
        # print(f"intput: {input_string}, find: {self.__tokens}")
        self.__index: int = 0
        self.int_value: int = 0
        self.str_value: str = None
        self.token_type: Union[int, str] = None

    def next_token(self) -> int:
        if self.__index < len(self.__tokens):
            token = self.__tokens[self.__index]
            self.__index += 1

            if token.isdigit():
                self.token_type = self.TT_NUMBER
                self.int_value = int(token)
                self.str_value = None
            elif self.__judge_alpha(token.replace('_', '')):
                self.token_type = self.TT_WORD
                self.str_value = token
                self.int_value = 0
            elif token in ("'", '"'):
                self.token_type = token
                self.str_value = self.__extract_quoted_string(token)
                self.int_value = 0
            else:
                self.token_type = token
                self.str_value = None
                self.int_value = 0
        else:
            self.token_type = self.TT_EOF
            self.int_value = 0
            self.str_value = None

        return self.token_type

    def __judge_alpha(self, token: str) -> bool:
        return token.isalnum() and not token.isdigit()

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
