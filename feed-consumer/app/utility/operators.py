from datetime import datetime,timedelta
from decimal import Decimal
from typing import List, Dict, Any
import json
import traceback
def parse_datetime(date_string):
    """
    Convert a date string into a datetime object by trying multiple formats.

    :param date_string: The date string to parse.
    :return: A datetime object if parsing is successful.
    :raises ValueError: If the date string cannot be parsed.
    """
    possible_formats = [
        "%Y-%m-%dt%H:%M:%S",
        "%Y-%m-%dt%H:%M:%S.%f",  # ISO 8601
        "%Y-%m-%d",           # Date only
        "%m/%d/%Y",           # US format
        "%d-%m-%Y",           # European format
        "%m/%d/%Y %I:%M %p",  # Datetime with AM/PM
        "%A, %B %d, %Y %H:%M:%S",  # Full datetime
    ]

    for fmt in possible_formats:
        try:
            return datetime.strptime(date_string, fmt)
        except ValueError:
            continue

    # If no format matches, raise an error
    raise ValueError(f"Unable to parse datetime string: {date_string}")
class ColumnOperators:
    """
    A class to define operations on columns categorized by their datatype.
    """

    class TypecastOperators:
        """
        A subclass to define typecasting operations.
        """

        @staticmethod
        def string_to_integer(value: str) -> int:
            """
            Convert a string to an integer.

            Example:
                Input: string_to_integer("123")
                Result: 123
            """
            try:
                return int(value)
            except ValueError:
                raise ValueError(f"Cannot convert string '{value}' to an integer.")

        @staticmethod
        def string_to_float(value: str) -> float:
            """
            Convert a string to a float.

            Example:
                Input: string_to_float("123.45")
                Result: 123.45
            """
            try:
                return float(value)
            except ValueError:
                raise ValueError(f"Cannot convert string '{value}' to a float.")

        @staticmethod
        def integer_to_string(value: int) -> str:
            """
            Convert an integer to a string.

            Example:
                Input: integer_to_string(123)
                Result: "123"
            """
            return str(value)

        @staticmethod
        def float_to_string(value: float) -> str:
            """
            Convert a float to a string.

            Example:
                Input: float_to_string(123.45)
                Result: "123.45"
            """
            return str(value)

        @staticmethod
        def string_to_boolean(value: str) -> bool:
            """
            Convert a string to a boolean.

            Example:
                Input: string_to_boolean("true")
                Result: True
            """
            lower_value = value.strip().lower()
            if lower_value in ["true", "1", "yes", "y"]:
                return True
            elif lower_value in ["false", "0", "no", "n"]:
                return False
            else:
                raise ValueError(f"Cannot convert string '{value}' to a boolean.")

        @staticmethod
        def boolean_to_string(value: bool) -> str:
            """
            Convert a boolean to a string.

            Example:
                Input: boolean_to_string(True)
                Result: "true"
            """
            return "true" if value else "false"
    class IntegerOperators:
        """
        A subclass to define operations specific to integer columns.
        """

        @staticmethod
        def add(a:int, b:int) -> int:
            return a + b

        @staticmethod
        def subtract(a:int, b):
            return a - b

        @staticmethod
        def multiply(a, b):
            return a * b

        @staticmethod
        def divide(a, b):
            if b == 0:
                raise ValueError("Division by zero is not allowed.")
            return a // b

        @staticmethod
        def modulo(a, b):
            return a % b

        @staticmethod
        def absolute(a):
            return abs(a)

    class FloatOperators:
        """
        A subclass to define operations specific to float columns.
        """

        @staticmethod
        def add(a, b):
            return a + b

        @staticmethod
        def subtract(a, b):
            return a - b

        @staticmethod
        def multiply(a, b):
            return a * b

        @staticmethod
        def divide(a, b):
            if b == 0.0:
                raise ValueError("Division by zero is not allowed.")
            return a / b

        @staticmethod
        def round(a, decimals=2):
            return round(a, decimals)

        @staticmethod
        def absolute(a):
            return abs(a)

    class DateOperators:
        """
        A subclass to define operations specific to date columns.
        """

        @staticmethod
        def add_days(date, days):
            date=parse_datetime(date) if isinstance(date, str) else date
            if not isinstance(date, datetime):
                raise ValueError("Input must be a datetime object or a valid date string.")
            return date + timedelta(days=days)

        @staticmethod
        def subtract_days(date, days):
            date=parse_datetime(date) if isinstance(date, str) else date
            if not isinstance(date, datetime):
                raise ValueError("Input must be a datetime object or a valid date string.")
            return date - timedelta(days=days)

        @staticmethod
        def difference_in_days(date1, date2):
            date1=parse_datetime(date1) if isinstance(date1, str) else date1
            if not isinstance(date1, datetime):
                raise ValueError("Input must be a datetime object or a valid date string.")
            date2=parse_datetime(date2) if isinstance(date2, str) else date2
            if not isinstance(date2, datetime):
                raise ValueError("Input must be a datetime object or a valid date string.")
            return (date1 - date2).days

        @staticmethod
        def extract_year(date):
            date=parse_datetime(date) if isinstance(date, str) else date
            if not isinstance(date, datetime):
                raise ValueError("Input must be a datetime object or a valid date string.")
            return date.year

        @staticmethod
        def extract_month(date):
            date=parse_datetime(date) if isinstance(date, str) else date
            if not isinstance(date, datetime):
                raise ValueError("Input must be a datetime object or a valid date string.")
            return date.month

        @staticmethod
        def extract_day(date):
            date=parse_datetime(date) if isinstance(date, str) else date
            if not isinstance(date, datetime):
                raise ValueError("Input must be a datetime object or a valid date string.")
            return date.day

        @staticmethod
        def format_date(date, format_string="%Y-%m-%d"):
            date=parse_datetime(date) if isinstance(date, str) else date
            if not isinstance(date, datetime):
                raise ValueError("Input must be a datetime object or a valid date string.")
            return date.strftime(format_string)

    class BooleanOperators:
        """
        A subclass to define operations specific to boolean columns.
        """

        @staticmethod
        def logical_and(a, b):
            return a and b

        @staticmethod
        def logical_or(a, b):
            return a or b

        @staticmethod
        def logical_not(a):
            return not a

        @staticmethod
        def logical_xor(a, b):
            return bool(a) ^ bool(b)

    class ArrayOperators:
        """
        A subclass to define operations specific to array columns.
        """

        @staticmethod
        def length(array):
            return len(array)

        @staticmethod
        def flatten(array):
            return [item for sublist in array for item in sublist]

        @staticmethod
        def join(array, delimiter=","):
            return delimiter.join(map(str, array))

        @staticmethod
        def filter(array, condition):
            return [item for item in array if condition(item)]

        @staticmethod
        def map(array, func):
            return [func(item) for item in array]

        @staticmethod
        def reduce(array, func, initial_value=None):
            from functools import reduce
            return reduce(func, array, initial_value)

    class ObjectOperators:
        """
        A subclass to define operations specific to object columns.
        """

        @staticmethod
        def access_key(obj, key):
            return obj.get(key)

        @staticmethod
        def merge(obj1, obj2):
            return {**obj1, **obj2}

        @staticmethod
        def flatten(obj, parent_key="", sep="."):
            items = []
            for k, v in obj.items():
                new_key = f"{parent_key}{sep}{k}" if parent_key else k
                if isinstance(v, dict):
                    items.extend(ColumnOperators.ObjectOperators.flatten(v, new_key, sep=sep).items())
                else:
                    items.append((new_key, v))
            return dict(items)

        @staticmethod
        def extract_keys(obj):
            return list(obj.keys())

        @staticmethod
        def extract_values(obj):
            return list(obj.values())
        
    class StringOperators:
        """
        A subclass to define operations specific to string columns.
        """
        
        @staticmethod
        def concatenate(*strings):
            """
            Concatenate two or more strings.

            :param strings: List of strings to concatenate.
            :return: Concatenated string.
            """
            if not all(isinstance(s, str) for s in strings):
                raise ValueError("All inputs must be strings.")
            return "".join(strings)

        @staticmethod
        def substring(string, start, end=None):
            """
            Extract a portion of a string.

            :param string: The input string.
            :param start: The starting index (inclusive).
            :param end: The ending index (exclusive). If None, goes to the end of the string.
            :return: Substring of the input string.
            """
            if not isinstance(string, str):
                raise ValueError("Input must be a string.")
            return string[start:end]

        @staticmethod
        def uppercase(string):
            """
            Convert all characters in a string to uppercase.

            :param string: The input string.
            :return: Uppercase version of the string.
            """
            if not isinstance(string, str):
                raise ValueError("Input must be a string.")
            return string.upper()

        @staticmethod
        def lowercase(string):
            """
            Convert all characters in a string to lowercase.

            :param string: The input string.
            :return: Lowercase version of the string.
            """
            if not isinstance(string, str):
                raise ValueError("Input must be a string.")
            return string.lower()

        @staticmethod
        def length(string):
            """
            Get the length of a string.

            :param string: The input string.
            :return: Length of the string.
            """
            if not isinstance(string, str):
                raise ValueError("Input must be a string.")
            return len(string)

        @staticmethod
        def replace(string, old, new):
            """
            Replace a substring with another substring.

            :param string: The input string.
            :param old: The substring to replace.
            :param new: The substring to replace with.
            :return: Modified string with replacements.
            """
            if not isinstance(string, str):
                raise ValueError("Input must be a string.")
            return string.replace(old, new)

        @staticmethod
        def trim(string):
            """
            Remove leading and trailing spaces from a string.

            :param string: The input string.
            :return: Trimmed string.
            """
            if not isinstance(string, str):
                raise ValueError("Input must be a string.")
            return string.strip()

        @staticmethod
        def split(string, delimiter=" "):
            """
            Split a string into a list based on a delimiter.

            :param string: The input string.
            :param delimiter: The delimiter to split by. Default is a space.
            :return: List of substrings.
            """
            if not isinstance(string, str):
                raise ValueError("Input must be a string.")
            return string.split(delimiter)
class ColumnOperatorsWrapper:
    """
    A wrapper class to provide a unified interface for column operations.
    This class categorizes operations based on the datatype of the column.
    """

    @staticmethod
    def float_operations():
        """
        Returns a list of operations that can be performed on float columns.
        """
        return [
            {
                "name": "Round",
                "description": "Round a floating-point number to the specified number of decimal places.",
                "formula_keyword": "ROUND",
                "example": {
                    "formula": "round(col1, 2)",
                    "columns": {"col1": 10.5678},
                    "result": 10.57
                }
            },
            ]

    @staticmethod
    def date_operations():
        """
        Returns a list of operations that can be performed on date columns.
        """
        return [
            {
                "name": "Add Days",
                "description": "Add a number of days to a date.",
                "formula_keyword": "ADD_DAYS",
                "example": {
                    "formula": "add_days(col1, 5)",
                    "columns": {"col1": "2023-10-01"},
                    "result": "2023-10-06"
                }
            },
            {
                "name": "Subtract Days",
                "description": "Subtract a number of days from a date.",
                "formula_keyword": "SUBTRACT_DAYS",
                "example": {
                    "formula": "subtract_days(col1, 5)",
                    "columns": {"col1": "2023-10-01"},
                    "result": "2023-09-26"
                }
            },
            {
                "name": "Difference in Days",
                "description": "Calculate the difference in days between two dates.",
                "formula_keyword": "DIFFERENCE_IN_DAYS",
                "example": {
                    "formula": "difference_in_days(col1, col2)",
                    "columns": {"col1": "2023-10-01", "col2": "2023-09-26"},
                    "result": 5
                }
            },
            {
                "name": "Extract Year",
                "description": "Extract the year from a date.",
                "formula_keyword": "EXTRACT_YEAR",
                "example": {
                    "formula": "extract_year(col1)",
                    "columns": {"col1": "2023-10-01"},
                    "result": 2023
                }
            },
            {
                "name": "Extract Month",
                "description": "Extract the month from a date.",
                "formula_keyword": "EXTRACT_MONTH",
                "example": {
                    "formula": "extract_month(col1)",
                    "columns": {"col1": "2023-10-01"},
                    "result": 10
                }
            },
            {
                "name": "Extract Day",
                "description": "Extract the day from a date.",
                "formula_keyword": "EXTRACT_DAY",
                "example": {
                    "formula": "extract_day(col1)",
                    "columns": {"col1": "2023-10-01"},
                    "result": 1
                }
            },
            {
                "name": "Format Date",
                "description": "Format a date into a specific string format.",
                "formula_keyword": "FORMAT_DATE",
                "example": {
                    "formula": "format_date(col1, '%d-%m-%Y')",
                    "columns": {"col1": "2023-10-01"},
                    "result": "01-10-2023"
                }
            },
        ]

    @staticmethod
    def boolean_operations():
        """
        Returns a list of operations that can be performed on boolean columns.
        """
        return [
            {
                "name": "Logical AND",
                "description": "Perform a logical AND operation.",
                "formula_keyword": "LOGICAL_AND",
                "example": {
                    "formula": "logical_and(col1, col2)",
                    "columns": {"col1": True, "col2": False},
                    "result": False
                }
            },
            {
                "name": "Logical OR",
                "description": "Perform a logical OR operation.",
                "formula_keyword": "LOGICAL_OR",
                "example": {
                    "formula": "logical_or(col1, col2)",
                    "columns": {"col1": True, "col2": False},
                    "result": True
                }
            },
            {
                "name": "Logical NOT",
                "description": "Perform a logical NOT operation.",
                "formula_keyword": "LOGICAL_NOT",
                "example": {
                    "formula": "logical_not(col1)",
                    "columns": {"col1": True},
                    "result": False
                }
            },
            {
                "name": "Logical XOR",
                "description": "Perform a logical XOR operation.",
                "formula_keyword": "LOGICAL_XOR",
                "example": {
                    "formula": "logical_xor(col1, col2)",
                    "columns": {"col1": True, "col2": False},
                    "result": True
                }
            },
        ]

    @staticmethod
    def array_operations():
        """
        Returns a list of operations that can be performed on array columns.
        """
        return [
            {
                "name": "Length",
                "description": "Get the length of an array.",
                "formula_keyword": "LENGTH",
                "example": {
                    "formula": "length(col1)",
                    "columns": {"col1": [1, 2, 3]},
                    "result": 3
                }
            },
            {
                "name": "Flatten",
                "description": "Flatten a nested array.",
                "formula_keyword": "FLATTEN",
                "example": {
                    "formula": "flatten(col1)",
                    "columns": {"col1": [[1, 2], [3, 4]]},
                    "result": [1, 2, 3, 4]
                }
            },
            {
                "name": "Join",
                "description": "Join array elements into a string with a delimiter.",
                "formula_keyword": "JOIN",
                "example": {
                    "formula": "join(col1, '-')",
                    "columns": {"col1": [1, 2, 3]},
                    "result": "1-2-3"
                }
            },
            {
                "name": "Map",
                "description": "Apply a function to each element in the array.",
                "formula_keyword": "MAP",
                "example": {
                    "formula": "map(col1, lambda x: x * 2)",
                    "columns": {"col1": [1, 2, 3]},
                    "result": [2, 4, 6]
                }
            },
            {
                "name": "Reduce",
                "description": "Reduce an array to a single value using a function.",
                "formula_keyword": "REDUCE",
                "example": {
                    "formula": "reduce(col1, lambda x, y: x + y, 0)",
                    "columns": {"col1": [1, 2, 3]},
                    "result": 6
                }
            },
        ]

    @staticmethod
    def object_operations():
        """
        Returns a list of operations that can be performed on object columns.
        """
        return [
            {
                "name": "Access Key",
                "description": "Access a value by its key in an object.",
                "formula_keyword": "ACCESS_KEY",
                "example": {
                    "formula": "access_key(col1, 'key1')",
                    "columns": {"col1": {"key1": "value1", "key2": "value2"}},
                    "result": "value1"
                }
            },
            {
                "name": "Merge",
                "description": "Merge two objects into one.",
                "formula_keyword": "MERGE",
                "example": {
                    "formula": "merge(col1, col2)",
                    "columns": {"col1": {"key1": "value1"}, "col2": {"key2": "value2"}},
                    "result": {"key1": "value1", "key2": "value2"}
                }
            },
            {
                "name": "Flatten Object",
                "description": "Flatten a nested object into a single-level object.",
                "formula_keyword": "FLATTEN_OBJECT",
                "example": {
                    "formula": "flatten_object(col1)",
                    "columns": {"col1": {"key1": "value1", "key2": {"key3": "value3"}}},
                    "result": {"key1": "value1", "key2.key3": "value3"}
                }
            },
            {
                "name": "Extract Keys",
                "description": "Extract all keys from an object.",
                "formula_keyword": "EXTRACT_KEYS",
                "example": {
                    "formula": "extract_keys(col1)",
                    "columns": {"col1": {"key1": "value1", "key2": "value2"}},
                    "result": ["key1", "key2"]
                }
            },
            {
                "name": "Extract Values",
                "description": "Extract all values from an object.",
                "formula_keyword": "EXTRACT_VALUES",
                "example": {
                    "formula": "extract_values(col1)",
                    "columns": {"col1": {"key1": "value1", "key2": "value2"}},
                    "result": ["value1", "value2"]
                }
            },
        ]
    @staticmethod
    def typecast_operations():
        """
        Returns a list of typecasting operations.
        """
        return [
            {
                "name": "String to Integer",
                "description": "Convert a string to an integer.",
                "formula_keyword": "STRING_TO_INTEGER",
                "example": {
                    "formula": "string_to_integer(col1)",
                    "columns": {"col1": "123"},
                    "result": 123
                }
            },
            {
                "name": "String to Float",
                "description": "Convert a string to a float.",
                "formula_keyword": "STRING_TO_FLOAT",
                "example": {
                    "formula": "string_to_float(col1)",
                    "columns": {"col1": "123.45"},
                    "result": 123.45
                }
            },
            {
                "name": "Integer to String",
                "description": "Convert an integer to a string.",
                "formula_keyword": "INTEGER_TO_STRING",
                "example": {
                    "formula": "integer_to_string(col1)",
                    "columns": {"col1": 123},
                    "result": "123"
                }
            },
            {
                "name": "Float to String",
                "description": "Convert a float to a string.",
                "formula_keyword": "FLOAT_TO_STRING",
                "example": {
                    "formula": "float_to_string(col1)",
                    "columns": {"col1": 123.45},
                    "result": "123.45"
                }
            },
            {
                "name": "String to Boolean",
                "description": "Convert a string to a boolean.",
                "formula_keyword": "STRING_TO_BOOLEAN",
                "example": {
                    "formula": "string_to_boolean(col1)",
                    "columns": {"col1": "true"},
                    "result": True
                }
            },
            {
                "name": "Boolean to String",
                "description": "Convert a boolean to a string.",
                "formula_keyword": "BOOLEAN_TO_STRING",
                "example": {
                    "formula": "boolean_to_string(col1)",
                    "columns": {"col1": True},
                    "result": "true"
                }
            },
        ]
    @staticmethod
    def string_operations():
        """
        Returns a list of operations that can be performed on string columns.
        """
        return [
            {
                "name": "Concatenate",
                "description": "Combine two or more strings.",
                "formula_keyword": "CONCATENATE",
                "example": {
                    "formula": "concatenate(col1, ' ', col2)",
                    "columns": {"col1": "Hello", "col2": "World"},
                    "result": "Hello World"
                }
            },
            {
                "name": "Substring",
                "description": "Extract a portion of a string.",
                "formula_keyword": "SUBSTRING",
                "example": {
                    "formula": "substring(col1, 0, 5)",
                    "columns": {"col1": "Hello World"},
                    "result": "Hello"
                }
            },
            {
                "name": "Uppercase",
                "description": "Convert all characters to uppercase.",
                "formula_keyword": "UPPERCASE",
                "example": {
                    "formula": "uppercase(col1)",
                    "columns": {"col1": "hello world"},
                    "result": "HELLO WORLD"
                }
            },
            {
                "name": "Lowercase",
                "description": "Convert all characters to lowercase.",
                "formula_keyword": "LOWERCASE",
                "example": {
                    "formula": "lowercase(col1)",
                    "columns": {"col1": "HELLO WORLD"},
                    "result": "hello world"
                }
            },
            {
                "name": "Length",
                "description": "Get the length of the string.",
                "formula_keyword": "LENGTH",
                "example": {
                    "formula": "length(col1)",
                    "columns": {"col1": "Hello World"},
                    "result": 11
                }
            },
            {
                "name": "Replace",
                "description": "Replace a substring with another substring.",
                "formula_keyword": "REPLACE",
                "example": {
                    "formula": "replace(col1, 'World', 'Python')",
                    "columns": {"col1": "Hello World"},
                    "result": "Hello Python"
                }
            },
            {
                "name": "Trim",
                "description": "Remove leading and trailing spaces.",
                "formula_keyword": "TRIM",
                "example": {
                    "formula": "trim(col1)",
                    "columns": {"col1": "   Hello World   "},
                    "result": "Hello World"
                }
            },
            {
                "name": "Split",
                "description": "Split a string into a list based on a delimiter.",
                "formula_keyword": "SPLIT",
                "example": {
                    "formula": "split(col1, ' ')",
                    "columns": {"col1": "Hello World"},
                    "result": ["Hello", "World"]
                }
            },
        ]


    @staticmethod
    def integer_operations():
        """
        Returns a list of operations that can be performed on integer columns.
        """
        return [
            {
                "name": "Add",
                "description": "Add two integers.",
                "formula_keyword": "ADD",
                "example": {
                    "formula": "add(col1, col2)",
                    "columns": {"col1": 10, "col2": 5},
                    "result": 15
                }
            },
            {
                "name": "Subtract",
                "description": "Subtract one integer from another.",
                "formula_keyword": "SUBTRACT",
                "example": {
                    "formula": "subtract(col1, col2)",
                    "columns": {"col1": 10, "col2": 5},
                    "result": 5
                }
            },
            {
                "name": "Multiply",
                "description": "Multiply two integers.",
                "formula_keyword": "MULTIPLY",
                "example": {
                    "formula": "multiply(col1, col2)",
                    "columns": {"col1": 10, "col2": 5},
                    "result": 50
                }
            },
            {
                "name": "Divide",
                "description": "Divide one integer by another.",
                "formula_keyword": "DIVIDE",
                "example": {
                    "formula": "divide(col1, col2)",
                    "columns": {"col1": 10, "col2": 5},
                    "result": 2
                }
            },
            {
                "name": "Modulo",
                "description": "Get the remainder of a division.",
                "formula_keyword": "MODULO",
                "example": {
                    "formula": "modulo(col1, col2)",
                    "columns": {"col1": 10, "col2": 3},
                    "result": 1
                }
            },
            {
                "name": "Absolute",
                "description": "Get the absolute value of an integer.",
                "formula_keyword": "ABSOLUTE",
                "example": {
                    "formula": "absolute(col1)",
                    "columns": {"col1": -10},
                    "result": 10
                }
            },
        ]
    @staticmethod
    def get_operations():
        """
        Returns a list of operations for a specific datatype.

        :param datatype: The datatype of the column (e.g., "string", "integer").
        :return: List of operations for the given datatype.
        """
        operations = {
            "string": ColumnOperatorsWrapper.string_operations(),
            "integer": ColumnOperatorsWrapper.integer_operations(),
            "float": ColumnOperatorsWrapper.float_operations(),
            "date": ColumnOperatorsWrapper.date_operations(),
            "boolean": ColumnOperatorsWrapper.boolean_operations(),
            "array": ColumnOperatorsWrapper.array_operations(),
            "object": ColumnOperatorsWrapper.object_operations(),
            "typecast": ColumnOperatorsWrapper.typecast_operations(),  # Added typecast operations
        }
        return operations

   
import re
from typing import Any
class FormulaInterpreter:
    """
    A class to interpret and evaluate formulas based on BODMAS rules.
    """

    @staticmethod
    def evaluate_formula(formula: str, column_values: dict) -> Any:
        """
        Evaluate a formula string using BODMAS rules.

        :param formula: The formula string (e.g., "add(multiply(col1, col2), 20) // 10").
        :param column_values: A dictionary of column names and their values (e.g., {"col1": 5, "col2": 10}).
        :return: The result of the evaluated formula.
        """
        

        # Define supported operations
        operations = {
            # Integer operations
            "add": ColumnOperators.IntegerOperators.add,
            "subtract": ColumnOperators.IntegerOperators.subtract,
            "multiply": ColumnOperators.IntegerOperators.multiply,
            "divide": ColumnOperators.FloatOperators.divide,
            "modulo": ColumnOperators.IntegerOperators.modulo,
            "absolute": ColumnOperators.IntegerOperators.absolute,

            # Typecast operations
            "string_to_integer": ColumnOperators.TypecastOperators.string_to_integer,
            "string_to_float": ColumnOperators.TypecastOperators.string_to_float,
            "integer_to_string": ColumnOperators.TypecastOperators.integer_to_string,
            "float_to_string": ColumnOperators.TypecastOperators.float_to_string,
            "string_to_boolean": ColumnOperators.TypecastOperators.string_to_boolean,
            "boolean_to_string": ColumnOperators.TypecastOperators.boolean_to_string,

            # Float operations
            "round": ColumnOperators.FloatOperators.round,

            # String operations
            "concatenate": ColumnOperators.StringOperators.concatenate,
            "substring": ColumnOperators.StringOperators.substring,
            "uppercase": ColumnOperators.StringOperators.uppercase,
            "lowercase": ColumnOperators.StringOperators.lowercase,
            "length": ColumnOperators.StringOperators.length,
            "replace": ColumnOperators.StringOperators.replace,
            "trim": ColumnOperators.StringOperators.trim,
            "split": ColumnOperators.StringOperators.split,

            # Date operations
            "add_days": ColumnOperators.DateOperators.add_days,
            "subtract_days": ColumnOperators.DateOperators.subtract_days,
            "difference_in_days": ColumnOperators.DateOperators.difference_in_days,
            "extract_year": ColumnOperators.DateOperators.extract_year,
            "extract_month": ColumnOperators.DateOperators.extract_month,
            "extract_day": ColumnOperators.DateOperators.extract_day,
            "format_date": ColumnOperators.DateOperators.format_date,

            # Boolean operations
            "logical_and": ColumnOperators.BooleanOperators.logical_and,
            "logical_or": ColumnOperators.BooleanOperators.logical_or,
            "logical_not": ColumnOperators.BooleanOperators.logical_not,
            "logical_xor": ColumnOperators.BooleanOperators.logical_xor,

            # Array operations
            "length": ColumnOperators.ArrayOperators.length,
            "flatten": ColumnOperators.ArrayOperators.flatten,
            "join": ColumnOperators.ArrayOperators.join,
            "filter": ColumnOperators.ArrayOperators.filter,
            "map": ColumnOperators.ArrayOperators.map,
            "reduce": ColumnOperators.ArrayOperators.reduce,

            # Object operations
            "access_key": ColumnOperators.ObjectOperators.access_key,
            "merge": ColumnOperators.ObjectOperators.merge,
            "flatten_object": ColumnOperators.ObjectOperators.flatten,
            "extract_keys": ColumnOperators.ObjectOperators.extract_keys,
            "extract_values": ColumnOperators.ObjectOperators.extract_values,
        }

        # Evaluate the formula using Python's eval
        try:
            pattern = r'(["\'])(.*?)\1'
            matches = re.findall(pattern, formula)
            string_values={}
            i=0
            # print(formula)
            for match in matches:
                string_values['string_value_'+str(i)]=match[0]+match[1]+match[0]
                
                
                formula=formula.replace(string_values['string_value_'+str(i)], 'string_value_'+str(i))
                i+=1
            
            # print(formula,string_values)
            # Replace operation names with Python-compatible function calls

            for op_name in operations.keys():
                
                # formula=formula.lower()
                

                
                formula = re.sub(rf"\b{op_name}\b", f"operations['{op_name}']", formula)
                # Replace column names with their values
            # print("Column values:", column_values)
            for column, value in column_values.items():
                # print(f"Processing formula '{formula} - 'column '{column}' with value '{value}'")
                escaped_column = re.escape(column)  # Escape single quotes in column names
                # print(column)
                pattern = rf"(?<!\w){escaped_column}(?!\w)"
                if   isinstance(value, str):
                    
                    # If not a datetime, treat it as a regular string
                    value = value.replace("'", "\\'")
                    formula = re.sub(pattern, f"'{value}'", formula)

                elif isinstance(value, (list, dict)):
                    # Convert lists and dicts to JSON strings
                    formula = re.sub(pattern, f"{json.dumps(value)}", formula) 
                elif isinstance(value, datetime):
                    # Convert datetime objects to ISO format strings
                    formula = re.sub(pattern, f"'{value.isoformat()}'", formula)
                    
                else:
                    # For other types (int, float, etc.), convert directly
                    formula = re.sub(pattern, f"{str(value)}", formula)
            for string_value in string_values:
                # print(string_value,string_values[string_value])
                formula=re.sub(string_value,string_values[string_value],formula)
            # print(f"Processed formula: {formula}")
            # print(f"Evaluating formula: {formula}")

            # Evaluate the formula
            result = eval(formula)
            datatype = type(result).__name__
            
            return result,datatype
        except Exception as e:
            print(traceback.format_exc())
            raise ValueError(f"Error evaluating formula: {e}")


# Example usage
if __name__ == "__main__":
    # Define column values
    column_values = {
        "col1": 10,
        "col2": 5,
        "col3": 20,
        "col4": "Hello World",
        "col5": [1, 2, 3],
        "col6": {"key1": "value1", "key2": "value2"},
        "col7": True,
    }

    # Example formulas
    formulas = [
        "add(col1, col2)",  # 10 + 5
        "subtract(multiply(col1, col2), col3)",  # (10 * 5) - 20
        "divide(multiply(col1, col2), 20)",  # (10 * 5) / 20
        "concatenate(col4, '!', ' How are you?')",  # "Hello World! How are you?"
        "substring(col4, 0, 5)",  # "Hello"
        "logical_and(col7, False)",  # False
        "length(col5)",  # 3
        "access_key(col6, 'key1')",  # "value1"
    ]

    for formula in formulas:
        try:
            result = FormulaInterpreter.evaluate_formula(formula, column_values)
            print(f"Formula: {formula} => Result: {result}")
        except ValueError as e:
            print(f"Error: {e}")

