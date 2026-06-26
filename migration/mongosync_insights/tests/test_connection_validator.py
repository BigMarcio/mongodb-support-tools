"""Tests for connection string sanitization."""
from lib.connection_validator import sanitize_for_display


class TestSanitizeForDisplay:
    def test_strips_credentials_and_shows_host(self):
        uri = "mongodb://user:secret@host1.example.com:27017,host2.example.com:27018/mydb"
        result = sanitize_for_display(uri)
        assert "user" not in result
        assert "secret" not in result
        assert "host1.example.com:27017" in result
        assert "host2.example.com:27018" in result
        assert "database: mydb" in result

    def test_uri_without_database(self):
        uri = "mongodb://user:pass@host.example.com:27017/"
        result = sanitize_for_display(uri)
        assert "pass" not in result
        assert "host.example.com:27017" in result
        assert "database:" not in result

    def test_escapes_html_in_host(self):
        uri = "mongodb://user:pass@host<script>:27017/db"
        result = sanitize_for_display(uri)
        assert "<script>" not in result
        assert "&lt;script&gt;" in result

    def test_malformed_uri_returns_fallback(self):
        assert sanitize_for_display("not a valid uri") == "[Connection String Provided]"

    def test_empty_string_returns_fallback(self):
        assert sanitize_for_display("") == "[Connection String Provided]"
