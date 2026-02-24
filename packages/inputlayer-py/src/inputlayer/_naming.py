"""Naming convention utilities: CamelCase ↔ snake_case, column → Datalog variable."""

from __future__ import annotations

import re


def camel_to_snake(name: str) -> str:
    """Convert CamelCase class name to snake_case relation name.

    Examples:
        Employee -> employee
        UserProfile -> user_profile
        HTTPRequest -> http_request
        ABCDef -> abc_def
    """
    # Insert underscore between sequences of uppercase and a following lower/digit
    s = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1_\2", name)
    # Insert underscore between lowercase/digit and uppercase
    s = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", s)
    return s.lower()


def snake_to_camel(name: str) -> str:
    """Convert snake_case to CamelCase.

    Examples:
        employee -> Employee
        user_profile -> UserProfile
        http_request -> HttpRequest
    """
    return "".join(part.capitalize() for part in name.split("_"))


def column_to_variable(column_name: str) -> str:
    """Convert a snake_case column name to a Datalog variable (Capitalized).

    Examples:
        id -> Id
        name -> Name
        department_name -> DepartmentName
        x -> X
    """
    return snake_to_camel(column_name)
