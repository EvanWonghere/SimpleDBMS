# -*- coding: utf-8 -*-
# @Time    : 2024/12/8 15:12
# @Author  : EvanWong
# @File    : ParseTest.py
# @Project : TestDB


from parse.Parser import Parser
from parse.BadSyntaxException import BadSyntaxException

while True:
    userInput = input("Enter an SQL statement: ")
    if userInput == "exit":
        break

    p = Parser(userInput)
    try:
        if userInput.startswith("select"):
            p.query_data()
        else:
            p.update_command()
        print("yes")
    except BadSyntaxException:
        print("no")