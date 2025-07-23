from operators import ColumnOperators, FormulaInterpreter
from datetime import datetime

# Define column values for testing
column_values = {
    "col1": 10,
    "col2": 5,
    "col3": 20,
    "col4": "Hello World",
    "col5": [1, 2, 3],
    "col6": {"key1": "value1", "key2": {"key3": "value3"}},
    "col7": True,
    "col8": datetime(2023, 10, 1),

    "col11": "123",  # String to Integer
    "col12": "123.45",  # String to Float
    "col13": 123,  # Integer to String
    "col14": 123.45,  # Float to String
    "col15": "true",  # String to Boolean
    "col16": True,  # Boolean to String
    "col17": "false",  # String to Boolean
    "col18": False,  # Boolean to String
}

# Test cases for FormulaInterpreter
formulas = [
    # Integer Operations
    ("add(col1, col2)", 15),  # 10 + 5
    ("subtract(col1, col2)", 5),  # 10 - 5
    ("multiply(col1, col2)", 50),  # 10 * 5
    ("divide(col1, col2)", 2.0),  # 10 / 5
    ("modulo(col1, col2)", 0),  # 10 % 5
    ("absolute(-col1)", 10),  # Absolute value of -10

    # Float Operations
    ("round(divide(col1, col2), 2)", 2.0),  # Round (10 / 5) to 2 decimal places

    # String Operations
    ("concatenate(col4, '!', ' How are you?')", "Hello World! How are you?"),  # Concatenate strings
    ("substring(col4, 0, 5)", "Hello"),  # Extract substring
    ("uppercase(col4)", "HELLO WORLD"),  # Convert to uppercase
    ("lowercase(col4)", "hello world"),  # Convert to lowercase
    ("length(col4)", 11),  # Length of the string
    ("replace(col4, 'World', 'Python')", "Hello Python"),  # Replace substring
    ("trim('   Hello   ')", "Hello"),  # Trim spaces
    ("split(col4, ' ')", ["Hello", "World"]),  # Split string by space

    # Date Operations
    ("add_days(col8, 5)", datetime(2023, 10, 6)),  # Add 5 days to the date
    ("subtract_days(col8, 5)", datetime(2023, 9, 26)),  # Subtract 5 days from the date
    ("difference_in_days(col8, add_days(col8, 5))", -5),  # Difference in days
    ("extract_year(col8)", 2023),  # Extract year
    ("extract_month(col8)", 10),  # Extract month
    ("extract_day(col8)", 1),  # Extract day
    ("format_date(col8, '%d-%m-%Y')", "01-10-2023"),  # Format date

    # Boolean Operations
    ("logical_and(col7, False)", False),  # Logical AND
    ("logical_or(col7, False)", True),  # Logical OR
    ("logical_not(col7)", False),  # Logical NOT
    ("logical_xor(col7, False)", True),  # Logical XOR

    # Array Operations
    ("length(col5)", 3),  # Length of the array
    ("flatten([[1, 2], [3, 4]])", [1, 2, 3, 4]),  # Flatten nested arrays
    ("join(col5, '-')", "1-2-3"),  # Join array elements with a delimiter
    ("map(col5, lambda x: x * 2)", [2, 4, 6]),  # Map function to double each element
    ("reduce(col5, lambda x, y: x + y, 0)", 6),  # Reduce array to sum of elements

    # Object Operations
    ("access_key(col6, 'key1')", "value1"),  # Access value by key
    ("merge(col6, {'key3': 'value3'})", {"key1": "value1", "key2": {"key3": "value3"}, "key3": "value3"}),  # Merge objects
    ("flatten_object(col6)", {"key1": "value1", "key2.key3": "value3"}),  # Flatten nested object
    ("extract_keys(col6)", ["key1", "key2"]),  # Extract keys from object
    ("extract_values(col6)", ["value1", {"key3": "value3"}]),  # Extract values from object

    
# Typecast Operations
    ("string_to_integer(col11)", 123),  # Convert "123" to 123
    ("string_to_float(col12)", 123.45),  # Convert "123.45" to 123.45
    ("integer_to_string(col13)", "123"),  # Convert 123 to "123"
    ("float_to_string(col14)", "123.45"),  # Convert 123.45 to "123.45"
    ("string_to_boolean(col15)", True),  # Convert "true" to True
    ("boolean_to_string(col16)", "true"),  # Convert True to "true"
    ("string_to_boolean(col17)", False),  # Convert "false" to False
    ("boolean_to_string(col18)", "false"),  # Convert False to "false"
]


# Run test cases
for formula, expected in formulas:
    try:
        result = FormulaInterpreter.evaluate_formula(formula, column_values)
        assert result == expected, f"Test failed for formula: {formula}. Expected: {expected}, Got: {result}"
        # print(f"Test passed for formula: {formula}. Result: {result}")
    except Exception as e:
        print(f"Test failed for formula: {formula}. Error: {e}")