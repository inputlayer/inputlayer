"""Tests for inputlayer._naming - camelCase/snake_case conversion, variable generation."""

from inputlayer._naming import camel_to_snake, column_to_variable, snake_to_camel


class TestCamelToSnake:
    def test_simple(self):
        assert camel_to_snake("Employee") == "employee"

    def test_two_words(self):
        assert camel_to_snake("UserProfile") == "user_profile"

    def test_acronym(self):
        assert camel_to_snake("HTTPRequest") == "http_request"

    def test_multi_acronym(self):
        assert camel_to_snake("ABCDef") == "abc_def"

    def test_all_caps(self):
        assert camel_to_snake("XML") == "xml"

    def test_already_lower(self):
        assert camel_to_snake("edge") == "edge"

    def test_single_char(self):
        assert camel_to_snake("A") == "a"

    def test_mixed(self):
        assert camel_to_snake("MyHTTPSServer") == "my_https_server"


class TestSnakeToCamel:
    def test_simple(self):
        assert snake_to_camel("employee") == "Employee"

    def test_two_words(self):
        assert snake_to_camel("user_profile") == "UserProfile"

    def test_three_words(self):
        assert snake_to_camel("first_middle_last") == "FirstMiddleLast"

    def test_single_char(self):
        assert snake_to_camel("x") == "X"


class TestColumnToVariable:
    def test_simple(self):
        assert column_to_variable("id") == "Id"

    def test_name(self):
        assert column_to_variable("name") == "Name"

    def test_compound(self):
        assert column_to_variable("department_name") == "DepartmentName"

    def test_single_char(self):
        assert column_to_variable("x") == "X"

    def test_multi_word(self):
        assert column_to_variable("first_name") == "FirstName"

    def test_already_camel(self):
        # If someone passes a camelCase string, it just capitalizes first letter
        assert column_to_variable("firstName") == "Firstname"
