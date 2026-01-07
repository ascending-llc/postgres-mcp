import pytest
from postgres_mcp.utils.url import fix_connection_url


@pytest.mark.parametrize(
    "input_url, expected_output",
    [
        ("postgresql://user:pass?word@localhost:5432/db", "postgresql://user:pass%3Fword@localhost:5432/db"),
        ("postgresql://user:pass%3Fword@localhost:5432/db", "postgresql://user:pass%3Fword@localhost:5432/db"),
    ],
)
def test_fix_connection_url_encoding(input_url: str, expected_output: str) -> None:
    """Verifies that passwords are encoded once and only once."""
    assert fix_connection_url(input_url) == expected_output


def test_fix_connection_url_no_mutation():
    """Ensure a standard safe URL is not changed."""
    url = "postgresql://readonly:securepassword123@db.example.com:5432/postgres"
    assert fix_connection_url(url) == url
