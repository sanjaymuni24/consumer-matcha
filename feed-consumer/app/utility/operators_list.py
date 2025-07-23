from usex_app.models import Operators

# Helper function to create or replace an operator
def create_or_replace_operator(name, options):
    operator, created = Operators.objects.get_or_create(name=name)
    operator.options = options  # Replace options if the operator already exists
    operator.save()
    return operator

# More or Less Indicator Operator
create_or_replace_operator(
    name="More or Less Indicator",
    options=[
        {"key": ">=", "display_name": "More or equal to"},
        {"key": "<=", "display_name": "Less than or equal to"},
        {"key": ">", "display_name": "More than"},
        {"key": "<", "display_name": "Less than"},
    ]
)
# Equality Operator
create_or_replace_operator(
    name="Equality Indicator",
    options=[
        {"key": "==", "display_name": "Equal to"},
        {"key": "!=", "display_name": "Not equal to"},
    ]
)