from usex_app.models import Enums

# Helper function to create or replace an enum
def create_or_replace_enum(name, options, datatype="str"):
    enum, created = Enums.objects.get_or_create(name=name)
    enum.options = options  # Replace options if the enum already exists
    enum.datatype = datatype  # Set the datatype of the enum set
    enum.save()
    return enum

# Hour of the Day Enum
create_or_replace_enum(
    name="Hour of the Day",
    options=[
        {"key": "00", "display_name": "Midnight"},
        {"key": "01", "display_name": "1 AM"},
        {"key": "02", "display_name": "2 AM"},
        {"key": "03", "display_name": "3 AM"},
        {"key": "04", "display_name": "4 AM"},
        {"key": "05", "display_name": "5 AM"},
        {"key": "06", "display_name": "6 AM"},
        {"key": "07", "display_name": "7 AM"},
        {"key": "08", "display_name": "8 AM"},
        {"key": "09", "display_name": "9 AM"},
        {"key": "10", "display_name": "10 AM"},
        {"key": "11", "display_name": "11 AM"},
        {"key": "12", "display_name": "Noon"},
        {"key": "13", "display_name": "1 PM"},
        {"key": "14", "display_name": "2 PM"},
        {"key": "15", "display_name": "3 PM"},
        {"key": "16", "display_name": "4 PM"},
        {"key": "17", "display_name": "5 PM"},
        {"key": "18", "display_name": "6 PM"},
        {"key": "19", "display_name": "7 PM"},
        {"key": "20", "display_name": "8 PM"},
        {"key": "21", "display_name": "9 PM"},
        {"key": "22", "display_name": "10 PM"},
        {"key": "23", "display_name": "11 PM"},
    ],
    datatype="str"
)

# Month of the Year Enum
create_or_replace_enum(
    name="Month of the Year",
    options=[
        {"key": "01", "display_name": "January"},
        {"key": "02", "display_name": "February"},
        {"key": "03", "display_name": "March"},
        {"key": "04", "display_name": "April"},
        {"key": "05", "display_name": "May"},
        {"key": "06", "display_name": "June"},
        {"key": "07", "display_name": "July"},
        {"key": "08", "display_name": "August"},
        {"key": "09", "display_name": "September"},
        {"key": "10", "display_name": "October"},
        {"key": "11", "display_name": "November"},
        {"key": "12", "display_name": "December"},
    ],
    datatype="str"
)

# Card Type Enum
create_or_replace_enum(
    name="Card Type",
    options=[
        {"key": "visa", "display_name": "Visa"},
        {"key": "mastercard", "display_name": "MasterCard"},
        {"key": "amex", "display_name": "American Express"},
        {"key": "discover", "display_name": "Discover"},
    ],
    datatype="str"
)

# Merchant Category Enum
create_or_replace_enum(
    name="Merchant Category",
    options=[
        {
            "key": "retail",
            "display_name": "Retail",
            "subcategories": [
                {"key": "clothing", "display_name": "Clothing"},
                {"key": "electronics", "display_name": "Electronics"},
                {"key": "groceries", "display_name": "Groceries"},
            ],
        },
        {
            "key": "services",
            "display_name": "Services",
            "subcategories": [
                {"key": "cleaning", "display_name": "Cleaning"},
                {"key": "repair", "display_name": "Repair"},
                {"key": "consulting", "display_name": "Consulting"},
            ],
        },
        {
            "key": "food_and_beverage",
            "display_name": "Food & Beverage",
            "subcategories": [
                {"key": "restaurants", "display_name": "Restaurants"},
                {"key": "cafes", "display_name": "Cafes"},
                {"key": "bars", "display_name": "Bars"},
            ],
        },
    ],
    datatype="str"
)
# Gender Enum
create_or_replace_enum(
    name="Gender",
    options=[
        {"key": "male", "display_name": "Male"},
        {"key": "female", "display_name": "Female"},
        {"key": "other", "display_name": "Other"},
    ],
    datatype="str"
)