"""Tests for Log Analyzer Summary tab payload built from mongosync logs."""
import json

from lib.logs_metrics import _merge_phase_events
from lib.log_summary import (
    backfill_commit_events_applied,
    build_log_metadata,
    build_log_summary_payload,
    extract_latest_progress,
    extract_mongosync_version,
    format_log_timestamp,
    merge_index_building_into_progress,
    merge_phase_events_into_progress,
    merge_replication_into_progress,
    merge_events_applied_into_progress,
    merge_verification_into_progress,
    merge_state_transitions_into_progress,
    build_summary_migration_state,
    SummaryMigrationState,
    natural_order_rows_from_log,
    phase_transition_rows_from_events,
)


def _sent_response(time, progress, uri="/api/v1/progress"):
    return {
        "time": time,
        "uri": uri,
        "body": json.dumps({"progress": progress}),
    }


class TestExtractLatestProgress:
    def test_returns_last_progress_body(self):
        responses = [
            _sent_response("2026-01-01T10:00:00.000Z", {"state": "RUNNING", "canCommit": False}),
            _sent_response("2026-01-01T11:00:00.000Z", {"state": "RUNNING", "canCommit": True}),
        ]
        progress, ts = extract_latest_progress(responses)
        assert progress["canCommit"] is True
        assert ts.startswith("2026-01-01T11:00:00")

    def test_skips_invalid_json_and_keeps_previous(self):
        responses = [
            _sent_response("2026-01-01T10:00:00.000Z", {"state": "RUNNING"}),
            {"time": "2026-01-01T10:30:00.000Z", "body": "not-json"},
        ]
        progress, ts = extract_latest_progress(responses)
        assert progress["state"] == "RUNNING"
        assert ts.startswith("2026-01-01T10:00:00")

    def test_skips_bodies_without_progress(self):
        responses = [
            {"time": "2026-01-01T10:00:00.000Z", "body": json.dumps({"ok": 1})},
            _sent_response("2026-01-01T10:01:00.000Z", {"state": "PAUSED"}),
        ]
        progress, _ts = extract_latest_progress(responses)
        assert progress["state"] == "PAUSED"

    def test_empty_returns_none(self):
        assert extract_latest_progress([]) == (None, None)
        assert extract_latest_progress(None) == (None, None)


class TestFormatLogTimestamp:
    def test_strips_fraction_and_z(self):
        assert format_log_timestamp("2026-01-01T12:00:00.123456Z") == "2026-01-01 12:00:00"

    def test_empty(self):
        assert format_log_timestamp(None) is None
        assert format_log_timestamp("") is None


class TestPhaseAndNaturalOrder:
    def test_phase_rows_capitalize_and_format_time(self):
        rows = phase_transition_rows_from_events(
            [("2026-01-01T10:00:00.000Z", "collection copy")]
        )
        assert rows == [
            {"phase": "Collection copy", "startedAt": "2026-01-01 10:00:00"}
        ]

    def test_natural_order_groups_by_database(self):
        rows = natural_order_rows_from_log(
            [
                {"database": "db1", "collection": "c1"},
                {"database": "db1", "collection": "c2"},
                {"database": "db2", "collection": "x"},
            ]
        )
        by_db = {r["database"]: r["collections"] for r in rows}
        assert by_db["db1"] == "c1, c2"
        assert by_db["db2"] == "x"

    def test_natural_order_empty(self):
        assert natural_order_rows_from_log([]) is None
        assert natural_order_rows_from_log(None) is None


class TestMergeReplicationIntoProgress:
    def test_fills_missing_lag_and_oplog(self):
        progress = {"state": "RUNNING"}
        replication = [
            {"lagTimeSeconds": 12, "totalEventsApplied": 100, "estimatedOplogTimeRemaining": "30 minutes"}
        ]
        merged = merge_replication_into_progress(progress, replication)
        assert merged["lagTimeSeconds"] == 12
        assert merged["estimatedOplogTimeRemaining"] == "30 minutes"
        assert "totalEventsApplied" not in merged

    def test_does_not_overwrite_progress_lag(self):
        progress = {"lagTimeSeconds": 5, "totalEventsApplied": 1}
        replication = [{"lagTimeSeconds": 99, "totalEventsApplied": 99}]
        merged = merge_replication_into_progress(progress, replication)
        assert merged["lagTimeSeconds"] == 5
        assert merged["totalEventsApplied"] == 1


class TestMergeEventsAppliedIntoProgress:
    def test_fills_missing_events(self):
        progress = {"state": "RUNNING"}
        replication = [{"totalEventsApplied": 100}]
        merged = merge_events_applied_into_progress(
            progress, replication_lines=replication
        )
        assert merged["totalEventsApplied"] == 100

    def test_backfills_zero_progress_events_from_replication(self):
        progress = {"info": "change event application", "totalEventsApplied": 0}
        replication = [
            {"totalEventsApplied": 100},
            {"totalEventsApplied": 120},
            {"totalEventsApplied": 0},
        ]
        merged = merge_events_applied_into_progress(
            progress, replication_lines=replication
        )
        assert merged["totalEventsApplied"] == 120


class TestSummaryMigrationState:
    def test_tracks_latest_phase_by_timestamp(self):
        state = SummaryMigrationState()
        state.note_progress(
            {"state": "RUNNING", "info": "collection copy"},
            "2026-04-20T15:51:01.869718Z",
        )
        state.note_phase(
            "change event application",
            "2026-04-20T15:51:16.174795Z",
        )
        progress = state.to_progress()
        assert progress["info"] == "change event application"

    def test_build_summary_migration_state_replays_log_events(self):
        state = build_summary_migration_state(
            sent_responses=[
                _sent_response(
                    "2026-04-20T15:51:01.869718Z",
                    {"state": "RUNNING", "info": "collection copy"},
                )
            ],
            phase_events=[
                ("2026-04-20T15:51:16.174795Z", "change event application"),
            ],
            index_creation_lines=[
                {
                    "time": "2026-04-20T15:52:00.000000Z",
                    "indexCreationProgress": {
                        "totalIndexesBuilt": 7,
                        "totalIndexesToBuild": 37,
                        "finishedCollections": 2,
                        "totalCollections": 21,
                    },
                }
            ],
        )
        progress = state.to_progress()
        assert progress["info"] == "change event application"
        assert progress["indexBuilding"]["indexesBuilt"] == 7


class TestMergePhaseEventsIntoProgress:
    def test_overlays_newer_phase_when_progress_is_stale(self):
        progress = {"state": "RUNNING", "info": "collection copy"}
        phase_events = [
            ("2026-04-20T15:20:49.967653", "collection copy"),
            ("2026-04-20T15:51:16.174795", "change event application"),
        ]
        merged = merge_phase_events_into_progress(
            progress,
            phase_events,
            progress_time="2026-04-20T15:51:01.869718Z",
        )
        assert merged["info"] == "change event application"

    def test_does_not_override_when_progress_is_newer(self):
        progress = {"state": "RUNNING", "info": "waiting for commit to complete"}
        phase_events = [
            ("2026-04-20T19:41:45.401715", "change event application"),
        ]
        merged = merge_phase_events_into_progress(
            progress,
            phase_events,
            progress_time="2026-04-20T19:42:35.726345Z",
        )
        assert merged["info"] == "waiting for commit to complete"

    def test_does_not_override_committed_state(self):
        progress = {"state": "COMMITTED", "info": "commit completed"}
        phase_events = [("2026-04-20T15:51:16.174795", "change event application")]
        merged = merge_phase_events_into_progress(
            progress,
            phase_events,
            progress_time="2026-04-20T15:51:01.869718Z",
        )
        assert merged["info"] == "commit completed"


class TestMergeIndexBuildingIntoProgress:
    def test_backfills_from_last_sent_response(self):
        progress = {"state": "COMMITTING", "info": "waiting for commit to complete"}
        sent_responses = [
            _sent_response(
                "2026-04-20T19:41:45.401715Z",
                {
                    "state": "RUNNING",
                    "info": "change event application",
                    "indexBuilding": {
                        "indexesBuilt": 37,
                        "totalIndexesToBuild": 37,
                        "collectionsFinished": 21,
                        "collectionsTotal": 21,
                    },
                },
            ),
            _sent_response(
                "2026-04-20T19:42:35.726345Z",
                {"state": "COMMITTING", "info": "waiting for commit to complete"},
            ),
        ]
        merged = merge_index_building_into_progress(
            progress, sent_responses=sent_responses
        )
        assert merged["indexBuilding"] == {
            "indexesBuilt": 37,
            "totalIndexesToBuild": 37,
            "collectionsFinished": 21,
            "collectionsTotal": 21,
        }

    def test_backfills_from_index_creation_progress(self):
        progress = {"state": "COMMITTING"}
        merged = merge_index_building_into_progress(
            progress,
            index_creation_lines=[
                {
                    "message": "Index creation progress.",
                    "indexCreationProgress": {
                        "totalIndexesBuilt": 10,
                        "totalIndexesToBuild": 12,
                        "finishedCollections": 3,
                        "totalCollections": 4,
                    },
                }
            ],
        )
        assert merged["indexBuilding"]["indexesBuilt"] == 10
        assert merged["indexBuilding"]["totalIndexesToBuild"] == 12
        assert merged["indexBuilding"]["collectionsTotal"] == 4

    def test_summary_shows_in_progress_index_build_during_collection_copy(self):
        payload = build_log_summary_payload(
            progress={
                "state": "RUNNING",
                "info": "collection copy",
            },
            start_options=[{"buildIndexes": "afterDataCopy"}],
            index_creation_lines=[
                {
                    "message": "Index creation progress.",
                    "indexCreationProgress": {
                        "totalIndexesBuilt": 7,
                        "totalIndexesToBuild": 37,
                        "finishedCollections": 2,
                        "totalCollections": 21,
                    },
                }
            ],
        )
        idx = payload["display"]["indexBuilding"]
        assert idx is not None
        assert idx.get("mode") != "info"
        assert idx["built"] == 7
        assert idx["total"] == 37
        by_label = {m["label"]: m["value"] for m in idx["metrics"]}
        assert by_label["Collections finished building indexes"] == "2 / 21"

    def test_summary_collections_copied_from_atlas_live_migrate_metrics(self):
        payload = build_log_summary_payload(
            progress={
                "state": "RUNNING",
                "info": "collection copy",
                "atlasLiveMigrateMetrics": {
                    "initialNumCollectionsCopied": 267,
                    "initialNumCollectionsTotal": 273,
                },
                "collectionCopy": {
                    "estimatedCopiedBytes": 100,
                    "estimatedTotalBytes": 200,
                },
            },
            partitions_copied=2601,
            partitions_total=2601,
            index_creation_lines=[
                {
                    "indexCreationProgress": {
                        "totalIndexesBuilt": 7,
                        "totalIndexesToBuild": 37,
                        "finishedCollections": 2,
                        "totalCollections": 21,
                    },
                }
            ],
        )
        sync = payload["display"]["sync"]
        assert sync["collectionsCopiedLabel"] == "Collections copied: 267 of 273"
        assert sync["partitionsCopiedLabel"] == "Partitions copied: 2,601 of 2,601"
        assert sync["copyPercent"] == 50.0

    def test_build_log_metadata_uses_atlas_collection_totals(self):
        metadata = build_log_metadata(
            progress={
                "state": "RUNNING",
                "info": "collection copy",
                "atlasLiveMigrateMetrics": {
                    "initialNumCollectionsCopied": 12,
                    "initialNumCollectionsTotal": 15,
                },
            },
        )
        assert metadata["collectionsCopied"] == 12
        assert metadata["collectionsTotal"] == 15

    def test_summary_payload_shows_cea_when_phase_event_is_newer_than_progress(self):
        payload = build_log_summary_payload(
            progress={"state": "RUNNING", "info": "collection copy"},
            progress_time="2026-04-20T15:51:01.869718Z",
            phase_events=[
                ("2026-04-20T15:20:49.967653", "collection copy"),
                ("2026-04-20T15:51:16.174795", "change event application"),
            ],
        )
        assert payload["display"]["sync"]["phase"] == "change event application"

    def test_prefers_sent_response_over_index_creation_progress(self):
        progress = {"state": "COMMITTING"}
        merged = merge_index_building_into_progress(
            progress,
            sent_responses=[
                _sent_response(
                    "2026-04-20T19:41:45.401715Z",
                    {
                        "indexBuilding": {
                            "indexesBuilt": 5,
                            "totalIndexesToBuild": 5,
                            "collectionsFinished": 2,
                            "collectionsTotal": 2,
                        }
                    },
                )
            ],
            index_creation_lines=[
                {
                    "indexCreationProgress": {
                        "totalIndexesBuilt": 99,
                        "totalIndexesToBuild": 99,
                        "finishedCollections": 9,
                        "totalCollections": 9,
                    }
                }
            ],
        )
        assert merged["indexBuilding"]["indexesBuilt"] == 5


class TestMergeVerificationIntoProgress:
    def test_backfills_when_latest_progress_omits_verification(self):
        verification = {
            "source": {"phase": "stream hashing", "scannedCollectionCount": 10},
            "destination": {"phase": "stream hashing", "scannedCollectionCount": 9},
        }
        merged = merge_verification_into_progress(
            {"state": "RUNNING", "info": "change event application"},
            sent_responses=[
                _sent_response(
                    "2026-04-20T15:51:00.000Z",
                    {"state": "RUNNING", "info": "collection copy"},
                ),
                _sent_response(
                    "2026-04-20T16:00:00.000Z",
                    {
                        "state": "RUNNING",
                        "info": "change event application",
                        "verification": verification,
                    },
                ),
                _sent_response(
                    "2026-04-20T16:30:00.000Z",
                    {"state": "RUNNING", "info": "change event application"},
                ),
            ],
        )
        assert merged["verification"] == verification

    def test_summary_shows_verifier_progress_during_cea(self):
        verification = {
            "source": {
                "phase": "initial hashing",
                "scannedCollectionCount": 5,
                "totalCollectionCount": 10,
            },
            "destination": {
                "phase": "initial hashing",
                "scannedCollectionCount": 4,
                "totalCollectionCount": 10,
            },
        }
        payload = build_log_summary_payload(
            sent_responses=[
                _sent_response(
                    "2026-04-20T16:00:00.000Z",
                    {
                        "state": "RUNNING",
                        "info": "change event application",
                        "verification": verification,
                    },
                ),
                _sent_response(
                    "2026-04-20T16:30:00.000Z",
                    {"state": "RUNNING", "info": "change event application"},
                ),
            ],
            start_options=[{"verification": {"enabled": True, "startAt": "cea"}}],
            phase_events=[
                ("2026-04-20T15:51:00.000Z", "collection copy"),
                ("2026-04-20T16:00:00.000Z", "change event application"),
            ],
        )
        card = payload["display"]["verification"]
        assert card is not None
        assert card.get("mode") != "info"
        assert card["source"][1]["value"] == "5 / 10"


class TestBackfillCommitEventsApplied:
    def test_uses_last_positive_replication_value_on_commit_completed(self):
        progress = {"info": "commit completed", "totalEventsApplied": 0}
        replication = [
            {"totalEventsApplied": 120},
            {"totalEventsApplied": 0},
        ]
        merged = backfill_commit_events_applied(progress, replication)
        assert merged["totalEventsApplied"] == 120

    def test_does_not_change_non_commit_phase(self):
        progress = {"info": "change event application", "totalEventsApplied": 0}
        replication = [{"totalEventsApplied": 120}]
        merged = backfill_commit_events_applied(progress, replication)
        assert merged["totalEventsApplied"] == 0


class TestBuildLogMetadata:
    def test_start_options_and_filters(self):
        metadata = build_log_metadata(
            phase_events=[
                ("2026-01-01T10:00:00.000Z", "initializing collections and indexes"),
                ("2026-01-01T11:00:00.000Z", "commit handler called"),
            ],
            start_options=[
                {
                    "reversible": True,
                    "enableUserWriteBlocking": "destinationOnly",
                    "buildIndexes": "afterDataCopy",
                    "verification": {"enabled": False},
                    "includeNamespaces": [{"database": "sales", "collections": ["orders"]}],
                    "excludeNamespaces": [{"database": "tmp", "collections": ["scratch"]}],
                }
            ],
            natural_order_items=[{"database": "sales", "collection": "orders"}],
            partitions_copied=10,
            partitions_total=20,
        )
        assert metadata["start"] == "2026-01-01 10:00:00"
        assert metadata["finish"] == "2026-01-01 11:00:00"
        assert metadata["reversible"] == "True"
        assert metadata["writeBlockingMode"] == "Destination Only"
        assert metadata["buildIndexes"] == "After Data Copy"
        assert metadata["verificationModeRaw"] == "disabled"
        assert metadata["namespaceFilterActive"] is True
        assert metadata["partitionsCopied"] == 10
        assert metadata["naturalOrderRows"][0]["database"] == "sales"

    def test_boolean_write_blocking_true(self):
        metadata = build_log_metadata(
            start_options=[{"enableUserWriteBlocking": True}]
        )
        assert metadata["writeBlockingMode"] == "Source and Destination"

    def test_write_blocking_default_from_hidden_flags(self):
        metadata = build_log_metadata(
            hidden_flags=[{"other": {"disableWriteBlocking": False}}]
        )
        assert metadata["writeBlockingMode"] == "Default"

    def test_write_blocking_disabled_from_hidden_flags(self):
        metadata = build_log_metadata(
            hidden_flags=[{"other": {"disableWriteBlocking": True}}]
        )
        assert metadata["writeBlockingMode"] == "Disabled"

    def test_hidden_flags_override_start_write_blocking(self):
        metadata = build_log_metadata(
            start_options=[{"enableUserWriteBlocking": "destinationOnly"}],
            hidden_flags=[{"other": {"disableWriteBlocking": False}}],
        )
        assert metadata["writeBlockingMode"] == "Default"

    def test_build_indexes_null_from_start_handler(self):
        metadata = build_log_metadata(
            start_handler_parameters=[{"BuildIndexes": None}]
        )
        assert metadata["buildIndexesRaw"] == "afterDataCopy"
        assert metadata["buildIndexes"] == "After Data Copy"

    def test_build_indexes_explicit_from_start_handler(self):
        metadata = build_log_metadata(
            start_handler_parameters=[{"BuildIndexes": "never"}]
        )
        assert metadata["buildIndexesRaw"] == "never"
        assert metadata["buildIndexes"] == "Never"

    def test_start_handler_overrides_start_options_build_indexes(self):
        metadata = build_log_metadata(
            start_options=[{"buildIndexes": "beforeDataCopy"}],
            start_handler_parameters=[{"BuildIndexes": None}],
        )
        assert metadata["buildIndexes"] == "After Data Copy"

    def test_detect_random_id_null_from_start_handler(self):
        metadata = build_log_metadata(
            start_handler_parameters=[{"DetectRandomId": None}]
        )
        assert metadata["detectRandomId"] == "True"

    def test_detect_random_id_true_from_start_handler(self):
        metadata = build_log_metadata(
            start_handler_parameters=[{"DetectRandomId": True}]
        )
        assert metadata["detectRandomId"] == "True"

    def test_detect_random_id_false_from_start_handler(self):
        metadata = build_log_metadata(
            start_handler_parameters=[{"DetectRandomId": False}]
        )
        assert metadata["detectRandomId"] == "False"

    def test_start_handler_overrides_start_options_detect_random_id(self):
        metadata = build_log_metadata(
            start_options=[{"detectRandomId": False}],
            start_handler_parameters=[{"DetectRandomId": None}],
        )
        assert metadata["detectRandomId"] == "True"

    def test_reversible_true_from_start_handler(self):
        metadata = build_log_metadata(
            start_handler_parameters=[{"Reversible": True}]
        )
        assert metadata["reversible"] == "True"

    def test_reversible_false_from_start_handler(self):
        metadata = build_log_metadata(
            start_handler_parameters=[{"Reversible": False}]
        )
        assert metadata["reversible"] == "False"

    def test_start_handler_overrides_start_options_reversible(self):
        metadata = build_log_metadata(
            start_options=[{"reversible": True}],
            start_handler_parameters=[{"Reversible": False}],
        )
        assert metadata["reversible"] == "False"

    def test_finish_from_committed_state_transition(self):
        metadata = build_log_metadata(
            state_transitions=[
                {
                    "time": "2026-07-09T04:27:29.065111Z",
                    "newState": "COMMITTED",
                    "message": "Transitioning the state.",
                }
            ]
        )
        assert metadata["finish"] == "2026-07-09 04:27:29"

    def test_committed_state_overrides_phase_finish(self):
        metadata = build_log_metadata(
            phase_events=[("2026-01-01T11:00:00.000Z", "commit handler called")],
            state_transitions=[
                {
                    "time": "2026-07-09T04:27:29.065111Z",
                    "newState": "COMMITTED",
                }
            ],
        )
        assert metadata["finish"] == "2026-07-09 04:27:29"

    def test_sync_phase_from_progress_info(self):
        metadata = build_log_metadata(
            progress={"info": "change event application", "state": "RUNNING"}
        )
        assert metadata["syncPhase"] == "change event application"
        assert metadata["phase"] == "Change event application"

    def test_committed_state_overrides_stale_progress_info(self):
        progress = {
            "state": "COMMITTING",
            "info": "waiting for index constraint enablement at the end of commit",
        }
        phase_events = [
            (
                "2026-04-20T01:18:47.641305",
                "waiting for index constraint enablement at the end of commit",
            ),
            ("2026-04-20T01:19:25.139116", "commit completed"),
        ]
        state_transitions = [
            {
                "time": "2026-04-20T01:19:25.139112Z",
                "newState": "COMMITTED",
                "message": "Transitioning the state.",
            }
        ]
        merged = merge_state_transitions_into_progress(
            progress, state_transitions, phase_events
        )
        assert merged["state"] == "COMMITTED"
        assert merged["info"] == "commit completed"

    def test_summary_payload_shows_committed_phase_after_last_progress(self):
        payload = build_log_summary_payload(
            progress={
                "state": "COMMITTING",
                "info": "waiting for index constraint enablement at the end of commit",
            },
            progress_time="2026-04-20T01:19:04.593714Z",
            phase_events=[
                (
                    "2026-04-20T01:18:47.641305",
                    "waiting for index constraint enablement at the end of commit",
                ),
                ("2026-04-20T01:19:25.139116", "commit completed"),
            ],
            state_transitions=[
                {
                    "time": "2026-04-20T01:19:25.139112Z",
                    "newState": "COMMITTED",
                }
            ],
        )
        display = payload["display"]
        assert display["state"] == "COMMITTED"
        assert display["sync"]["phase"] == "commit completed"

    def test_summary_payload_uses_last_known_events_on_commit_completed(self):
        payload = build_log_summary_payload(
            progress={
                "state": "COMMITTING",
                "info": "waiting for index constraint enablement at the end of commit",
                "totalEventsApplied": 0,
            },
            replication_lines=[
                {"time": "2026-04-20T01:18:30.000000Z", "totalEventsApplied": 125},
                {"time": "2026-04-20T01:19:09.000000Z", "totalEventsApplied": 0},
            ],
            phase_events=[
                (
                    "2026-04-20T01:18:47.641305",
                    "waiting for index constraint enablement at the end of commit",
                ),
                ("2026-04-20T01:19:25.139116", "commit completed"),
            ],
            state_transitions=[
                {
                    "time": "2026-04-20T01:19:25.139112Z",
                    "newState": "COMMITTED",
                }
            ],
        )
        display = payload["display"]
        assert display["sync"]["phase"] == "commit completed"
        by_label = {m["label"]: m["value"] for m in display["sync"]["metrics"]}
        assert by_label["Events applied"] == "125"

    def test_summary_payload_shows_index_building_when_latest_progress_omits_it(self):
        payload = build_log_summary_payload(
            progress={
                "state": "COMMITTING",
                "info": "waiting for commit to complete",
            },
            progress_time="2026-04-20T19:42:35.726345Z",
            start_options=[{"buildIndexes": "afterDataCopy"}],
            sent_responses=[
                _sent_response(
                    "2026-04-20T19:41:45.401715Z",
                    {
                        "state": "RUNNING",
                        "info": "change event application",
                        "indexBuilding": {
                            "indexesBuilt": 37,
                            "totalIndexesToBuild": 37,
                            "collectionsFinished": 21,
                            "collectionsTotal": 21,
                        },
                    },
                ),
                _sent_response(
                    "2026-04-20T19:42:35.726345Z",
                    {
                        "state": "COMMITTING",
                        "info": "waiting for commit to complete",
                    },
                ),
            ],
        )
        idx = payload["display"]["indexBuilding"]
        assert idx is not None
        assert idx.get("mode") != "info"
        assert idx["built"] == 37
        assert idx["total"] == 37
        by_label = {m["label"]: m["value"] for m in idx["metrics"]}
        assert by_label["Indexes built"] == "37 / 37"
        assert by_label["Collections finished building indexes"] == "21 / 21"


class TestExtractMongosyncVersion:
    def test_returns_first_version(self):
        lines = [
            {"message": "Version info", "version": "1.9.0"},
            {"message": "Version info", "version": "2.0.0"},
        ]
        assert extract_mongosync_version(lines) == "1.9.0"

    def test_missing_returns_none(self):
        assert extract_mongosync_version([]) is None
        assert extract_mongosync_version([{"message": "Version info"}]) is None


class TestBuildLogSummaryPayload:
    def test_progress_payload_matches_live_monitor_shape(self):
        progress = {
            "state": "RUNNING",
            "info": "collection copy",
            "canCommit": False,
            "canWrite": False,
            "lagTimeSeconds": 8,
            "totalEventsApplied": 42,
            "collectionCopy": {
                "estimatedCopiedBytes": 50,
                "estimatedTotalBytes": 100,
            },
            "directionMapping": {
                "Source": "src.example.com:27017",
                "Destination": "dst.example.com:27017",
            },
            "indexBuilding": {
                "indexesBuilt": 1,
                "totalIndexesToBuild": 4,
                "collectionsFinished": 0,
                "collectionsTotal": 2,
            },
            "verification": {
                "source": {"phase": "in progress", "hashedDocumentCount": 1, "estimatedDocumentCount": 10},
                "destination": {"phase": "in progress", "hashedDocumentCount": 1, "estimatedDocumentCount": 10},
            },
            "warnings": ["example warning"],
        }
        payload = build_log_summary_payload(
            progress=progress,
            progress_time="2026-01-01T12:00:00.000Z",
            phase_events=[("2026-01-01T10:00:00.000Z", "collection copy")],
            partitions_copied=50,
            partitions_total=100,
        )
        assert payload["error"] is None
        assert payload["pageTitle"] == "Migration Summary"
        assert "latest /progress" in payload["pageSubtitle"]
        assert payload["warnings"] == ["example warning"]
        display = payload["display"]
        assert display["state"] == "RUNNING"
        assert display["sync"]["phase"] == "collection copy"
        assert display["sync"]["cardTitle"] == "Latest Progress Status"
        assert display["sync"]["copyPercent"] == 50.0
        assert display["direction"]["source"]["address"] == "src.example.com:27017"
        assert display["indexBuilding"]["built"] == 1
        assert display["indexBuilding"]["title"] == "Latest Index building info"
        index_metrics = {
            m["label"]: m["value"] for m in display["indexBuilding"]["metrics"]
        }
        assert index_metrics["Collections finished building indexes"] == "0 / 2"
        assert display["verification"]["title"] == "Latest Embedded Verifier status"
        badges = {b["id"]: b for b in payload["dataSources"]["badges"]}
        assert badges["progress"]["label"] == "Latest /progress"
        assert badges["progress"]["status"] == "active"

    def test_summary_includes_mongosync_version(self):
        payload = build_log_summary_payload(
            progress={"state": "RUNNING", "info": "collection copy"},
            version_info_lines=[{"message": "Version info", "version": "1.18.0"}],
        )
        sync = payload["display"]["sync"]
        assert sync["showMongosyncVersion"] is True
        assert sync["mongosyncVersion"] == "1.18.0"

    def test_summary_missing_version_shows_hover_title(self):
        payload = build_log_summary_payload(
            progress={"state": "RUNNING", "info": "collection copy"},
        )
        sync = payload["display"]["sync"]
        assert sync["showMongosyncVersion"] is True
        assert "mongosyncVersion" not in sync
        assert "Missing initial log file for version capture.\n" in sync["mongosyncVersionMissingTitle"]
        assert "Upload all rotated mongosync files for full analysis" in sync["mongosyncVersionMissingTitle"]

    def test_metadata_only_when_no_progress(self):
        payload = build_log_summary_payload(
            phase_events=[("2026-01-01T10:00:00.000Z", "collection copy")],
            start_options=[{"reversible": False, "buildIndexes": "never"}],
        )
        assert payload["error"] is None
        assert payload["display"]["sync"]["phaseStartTimes"]["rows"][0]["phase"] == "Collection copy"
        assert payload["display"]["sync"]["metadataMetrics"]
        badges = payload["dataSources"]["badges"]
        assert len(badges) == 1
        assert badges[0]["id"] == "metadata"
        assert badges[0]["label"] == "Log events"

    def test_replication_fallback_fills_lag(self):
        payload = build_log_summary_payload(
            replication_lines=[
                {
                    "time": "2026-01-01T10:01:00.000Z",
                    "lagTimeSeconds": 15,
                    "totalEventsApplied": 9,
                }
            ],
            phase_events=[("2026-01-01T10:00:00.000Z", "change event application")],
        )
        metrics = {m["label"]: m["value"] for m in payload["display"]["sync"]["metrics"]}
        assert metrics["Lag time"] == "15s"
        assert metrics["Events applied"] == "9"
        assert payload["display"]["sync"]["phase"] == "change event application"

    def test_phase_rows_from_messages_with_trailing_period(self):
        payload = build_log_summary_payload(
            phase_events=_merge_phase_events(
                [
                    {
                        "time": "2026-07-20T17:53:11.775615Z",
                        "message": "Starting collection copy phase.",
                    },
                    {
                        "time": "2026-07-21T03:49:22.808282Z",
                        "message": "Starting change event application phase.",
                    },
                ],
                [],
            )
        )
        rows = {
            row["phase"]: row["startedAt"]
            for row in payload["display"]["sync"]["phaseStartTimes"]["rows"]
        }
        assert rows["Collection copy"] == "2026-07-20 17:53:11"
        assert rows["Change event application"] == "2026-07-21 03:49:22"

    def test_empty_payload(self):
        payload = build_log_summary_payload()
        assert payload["display"] is None
        assert payload["error"]
        assert payload["pageTitle"] == "Migration Summary"
