import re

import click
from datetime import datetime

def validate_date(ctx, param, value):
    """Validates the date input format (YYYY-MM-DD)."""
    if value is not None:
        try:
            datetime.strptime(value, "%Y-%m-%d")
        except ValueError:
            raise click.BadParameter(
                f"Invalid date format: '{value}'. Expected format: YYYY-MM-DD."
            )
    return value
