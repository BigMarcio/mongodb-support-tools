"""Regression tests: untrusted namespace names must not escape the plot <script>.

A source-database user controls collection names, which mongosync copies verbatim
into its diagnostic logs. Those names reach the Plotly payloads rendered inside an
inline <script> in upload_results.html, so a name containing "</script>" would
close the block early and inject markup into the analyst's browser.
"""
import io
import json
import re
import uuid

import pytest

from lib import snapshot_store
from lib.log_store_registry import log_store_registry
from lib.plot_theme import inline_json

PAYLOAD = "</script><script>window.MI_XSS_PWNED=1</script>"


@pytest.fixture(autouse=True)
def _isolated_registry(monkeypatch, tmp_path):
    monkeypatch.setattr(snapshot_store, "LOG_STORE_DIR", str(tmp_path / "store"))
    (tmp_path / "store").mkdir(exist_ok=True)
    log_store_registry._entries.clear()
    yield
    log_store_registry._entries.clear()


def _upload(app_client, lines):
    body = "\n".join(lines).encode()
    response = app_client.post(
        "/logs/uploadLogs",
        data={"file": (io.BytesIO(body), "mongosync.log")},
        content_type="multipart/form-data",
    )
    return response, response.get_data(as_text=True)


def _script_block(html, anchor):
    """Return the inline script contents up to wherever a browser would end it."""
    start = html.find(anchor)
    assert start != -1, f"{anchor} not rendered"
    tail = html[start:]
    end = re.search(r"</script", tail, re.IGNORECASE)
    assert end is not None, "no closing script tag found"
    return tail[: end.start()]


def _assert_no_breakout(html, anchor):
    """The payload must stay inert data inside the script, not become markup.

    The name itself is expected to survive in escaped form -- that is the point
    of the fix. What must not happen is the block terminating early, so the
    block is required to reach its own last key.
    """
    assert PAYLOAD not in html, "raw payload reached the response"
    block = _script_block(html, anchor)
    assert "</script" not in block.lower(), "payload closed the script block"
    assert "\\u003c/script\\u003e" in block, "payload was not escaped in place"
    assert "searchLogsUrl" in block, "script block was truncated by the payload"


PROGRESS_LINE = (
    '{"time":"2026-01-01T10:00:00.000","level":"info",'
    '"message":"Replication progress update","totalEventsApplied":1}'
)


class TestInlineJsonFilter:
    def test_escapes_script_breakout_characters(self):
        assert inline_json('{"a":"<b>&c"}') == '{"a":"\\u003cb\\u003e\\u0026c"}'

    def test_escapes_js_line_terminators(self):
        assert inline_json('"\u2028\u2029"') == '"\\u2028\\u2029"'

    def test_is_idempotent(self):
        once = inline_json('{"a":"</script>"}')
        assert inline_json(once) == once

    @pytest.mark.parametrize("empty", ["", None])
    def test_empty_payloads(self, empty):
        assert inline_json(empty) == ""

    def test_output_is_still_valid_json_with_original_value(self):
        original = "db." + PAYLOAD
        escaped = inline_json(json.dumps({"name": original}))
        assert json.loads(escaped)["name"] == original


class TestUploadedLogSinks:
    def test_partition_table_collection_name(self, app_client):
        response, html = _upload(app_client, [
            PROGRESS_LINE,
            json.dumps({
                "time": "2026-01-01T10:00:01.000",
                "level": "info",
                "message": "Creating a single partition for whole collection",
                "database": "victimdb",
                "collection": PAYLOAD,
                "reason": "capped collection",
            }),
        ])
        assert response.status_code == 200
        _assert_no_breakout(html, "__MI_UPLOAD_PAGE__")

    def test_busiest_collections_trace_name(self, app_client):
        lines = [PROGRESS_LINE]
        for offset in range(3):
            lines.append(json.dumps({
                "time": f"2026-01-01T10:00:{offset * 10:02d}.000",
                "level": "info",
                "message": "Recent CRUD change event statistics.",
                "recentCRUDStatistics": {
                    "busiestCollections": [{
                        "namespace": "victimdb." + PAYLOAD,
                        "totalEvents": 100 + offset,
                        "totalEventsPerType": {"insert": 100 + offset},
                    }],
                },
            }))
        response, html = _upload(app_client, lines)
        assert response.status_code == 200
        _assert_no_breakout(html, "busiestCollectionsPlot")

    def test_legitimate_metacharacters_survive_round_trip(self, app_client):
        """Escaping must not corrupt names that legitimately contain & < >."""
        collection = "orders&archive<v2>"
        response, html = _upload(app_client, [
            PROGRESS_LINE,
            json.dumps({
                "time": "2026-01-01T10:00:01.000",
                "level": "info",
                "message": "Creating a single partition for whole collection",
                "database": "shop",
                "collection": collection,
                "reason": "capped collection",
            }),
        ])
        assert response.status_code == 200
        figure = json.loads(_plot_payload(html))
        table_values = [
            value
            for trace in figure.get("data", [])
            if "cells" in trace
            for column in trace["cells"].get("values", [])
            for value in column
        ]
        assert f"shop.{collection}" in table_values


def _plot_payload(html):
    """Return the JSON object assigned to the `plot` key of the page payload."""
    start = html.find("plot: ", html.find("__MI_UPLOAD_PAGE__")) + len("plot: ")
    depth = 0
    for index, char in enumerate(html[start:]):
        if char == "{":
            depth += 1
        elif char == "}":
            depth -= 1
            if depth == 0:
                return html[start:start + index + 1]
    raise AssertionError("could not delimit plot payload")


class TestStoredSnapshots:
    def test_snapshot_saved_before_the_fix_is_rendered_safely(
        self, app_client, minimal_template_data
    ):
        """Snapshots persist already-serialized JSON, so escaping must happen
        at render time to neutralise payloads written by older builds."""
        poisoned = dict(minimal_template_data)
        poisoned["plot_json"] = json.dumps({"data": [], "name": PAYLOAD})
        poisoned["has_logs_data"] = True

        snapshot_id = str(uuid.uuid4())
        snapshot_store.save_snapshot(
            snapshot_id, "mongosync.log", 100, 1, "", poisoned
        )

        response = app_client.get(f"/logs/load_snapshot/{snapshot_id}")
        assert response.status_code == 200
        _assert_no_breakout(response.get_data(as_text=True), "__MI_UPLOAD_PAGE__")
