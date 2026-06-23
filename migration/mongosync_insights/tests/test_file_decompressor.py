"""Tests for archive decompression helpers."""
import io

import pytest

from lib.file_decompressor import decompress_zip_classified, should_skip_archive_member


class TestShouldSkipArchiveMember:
    @pytest.mark.parametrize("name,expected", [
        ("mongosync.log", False),
        ("mongosync_metrics-2026.log.gz", False),
        ("__MACOSX/._mongosync_metrics-2026.log.gz", True),
        ("folder/__MACOSX/._foo.log.gz", True),
        ("._mongosync.log.gz", True),
        (".DS_Store", True),
    ])
    def test_should_skip_archive_member(self, name, expected):
        assert should_skip_archive_member(name) is expected


class TestDecompressZipClassified:
    def test_skips_macos_metadata_and_reads_metrics_members(self, tmp_path):
        import zipfile

        zip_path = tmp_path / "test.zip"
        metrics_name = "mongosync_metrics-2026.log.gz"
        metrics_bytes = io.BytesIO()
        import gzip
        with gzip.GzipFile(fileobj=metrics_bytes, mode='wb') as gz:
            gz.write(b'{"time":"2026-01-01T00:00:00Z","message":"# HELP mongosync_phase\\n"}\n')

        with zipfile.ZipFile(zip_path, 'w') as zf:
            zf.writestr(metrics_name, metrics_bytes.getvalue())
            zf.writestr(f"__MACOSX/._{metrics_name}", b"\x00\x05mac-metadata")

        with zip_path.open('rb') as f:
            lines = list(decompress_zip_classified(f))

        assert len(lines) == 1
        line, file_type = lines[0]
        assert file_type == 'metrics'
        assert b'mongosync_phase' in line
