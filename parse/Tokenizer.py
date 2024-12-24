import re
from typing import Union, Optional

class Tokenizer:
    """
    A simple tokenizer (lexer) that splits an input SQL-like string into tokens.

    Attributes:
        TT_EOF (int): Token type for end-of-file.
        TT_NUMBER (int): Token type for an integer.
        TT_WORD (int): Token type for an identifier/keyword.
        TT_FLOAT (int): Token type for a floating-point number.

        __tokens (List[str]): A list of raw token strings extracted from the input.
        __index (int): Current position in the token list.
        int_value (int): The integer value if the current token is an integer.
        str_value (Optional[str]): The string value if the current token is a string/identifier.
        float_value (float): The float value if the current token is a float.
        token_type (Union[int, str, None]): The type of the current token (could be an int for TT_*,
                                            or the actual delimiter/keyword symbol).
    """

    # Token types
    TT_EOF = -1
    TT_NUMBER = -2
    TT_WORD = -3
    TT_FLOAT = -4

    def __init__(self, input_string: str):
        """
        Initialize the Tokenizer with the given input string.

        Args:
            input_string (str): The SQL-like input to tokenize.
        """
        # Regex to capture floats, words, and single non-whitespace chars
        self.__tokens: list[str] = re.findall(r"\d+\.\d+|\w+|[^\w\s]", input_string)
        self.__index: int = 0

        # Current token info
        self.int_value: int = 0
        self.str_value: Optional[str] = None
        self.float_value: float = 0.0
        self.token_type: Union[int, str, None] = None

    def next_token(self) -> Union[int, str]:
        """
        Advance to the next token in the input and interpret its type.

        Returns:
            Union[int, str]: The token type (could be TT_* or the actual string symbol if it's a delimiter).
        """
        if self.__index < len(self.__tokens):
            token = self.__tokens[self.__index]
            self.__index += 1

            if token.isdigit():
                self.token_type = self.TT_NUMBER
                self.int_value = int(token)
                self.str_value = None
                self.float_value = 0.0
            elif self.__is_float(token):
                self.token_type = self.TT_FLOAT
                self.float_value = float(token)
                self.int_value = 0
                self.str_value = None
            elif self.__is_word(token):
                self.token_type = self.TT_WORD
                self.str_value = token
                self.int_value = 0
                self.float_value = 0.0
            elif token in ("'", '"'):
                # Quoted string: gather the content until the matching quote
                self.token_type = token
                self.str_value = self.__extract_quoted_string(token)
                self.int_value = 0
                self.float_value = 0.0
            else:
                # Assume it's a delimiter or symbol
                self.token_type = token
                self.str_value = None
                self.int_value = 0
                self.float_value = 0.0
        else:
            self.token_type = self.TT_EOF
            self.int_value = 0
            self.str_value = None
            self.float_value = 0.0

        return self.token_type

    @staticmethod
    def __is_float(token: str) -> bool:
        try:
            float(token)
            return '.' in token
        except ValueError:
            return False

    @staticmethod
    def __is_word(token: str) -> bool:
        """
        Check if a token should be interpreted as an identifier or keyword.
        Alphanumeric and underscore are treated as word characters.
        """
        pattern = re.compile(r"^[a-zA-Z0-9_]+$")
        return bool(pattern.match(token))

    def __extract_quoted_string(self, quote_char: str) -> str:
        """
        For a token recognized as a quote, gather characters until matching quote is found.

        Args:
            quote_char (str): The quote symbol, either ' or ".

        Returns:
            str: The content inside the quotes.
        """
        result = ""
        while self.__index < len(self.__tokens):
            token = self.__tokens[self.__index]
            self.__index += 1
            if token == quote_char:
                break
            result += token
        return result