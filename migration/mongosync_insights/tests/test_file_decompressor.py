"""Tests for archive decompression helpers."""
import bz2
import gzip
import io
import tarfile
import zipfile

import pytest

from lib.file_decompressor import (
    decompress_bzip2_classified,
    decompress_file_classified,
    decompress_gzip_classified,
    decompress_tar_classified,
    decompress_zip_classified,
    get_file_extension,
    is_compressed_mime_type,
    should_skip_archive_member,
)


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


class TestGetFileExtension:
    @pytest.mark.parametrize(
        "filename,expected",
        [
            ("archive.tar.gz", ".tar.gz"),
            ("archive.tar.bz2", ".tar.bz2"),
            ("mongosync.log.gz", ".gz"),
            ("data.zip", ".zip"),
        ],
    )
    def test_get_file_extension(self, filename, expected):
        assert get_file_extension(filename) == expected


class TestIsCompressedMimeType:
    @pytest.mark.parametrize(
        "mime,expected",
        [
            ("application/gzip", True),
            ("application/zip", True),
            ("text/plain", False),
        ],
    )
    def test_is_compressed_mime_type(self, mime, expected):
        assert is_compressed_mime_type(mime) is expected


class TestDecompressGzipClassified:
    def test_decompresses_and_classifies_log(self):
        buf = io.BytesIO()
        with gzip.GzipFile(fileobj=buf, mode="wb") as gz:
            gz.write(b'{"level":"info"}\n')
        buf.seek(0)
        lines = list(decompress_gzip_classified(buf, "mongosync.log.gz"))
        assert len(lines) == 1
        assert lines[0][1] == "logs"


class TestDecompressBzip2Classified:
    def test_decompresses_bzip2_log(self):
        data = bz2.compress(b'{"level":"info"}\n')
        buf = io.BytesIO(data)
        lines = list(decompress_bzip2_classified(buf, "mongosync.log.bz2"))
        assert len(lines) == 1
        assert lines[0][1] == "logs"


class TestDecompressTarClassified:
    def test_decompresses_tar_gz_member(self, tmp_path):
        tar_path = tmp_path / "logs.tar.gz"
        content = b'{"level":"info"}\n'
        with tarfile.open(tar_path, "w:gz") as tf:
            info = tarfile.TarInfo(name="mongosync.log")
            info.size = len(content)
            tf.addfile(info, io.BytesIO(content))
        with tar_path.open("rb") as f:
            lines = list(decompress_tar_classified(f, compression="gz"))
        assert len(lines) == 1
        assert lines[0][1] == "logs"


class TestDecompressFileClassified:
    def test_routes_gzip_by_mime(self):
        buf = io.BytesIO()
        with gzip.GzipFile(fileobj=buf, mode="wb") as gz:
            gz.write(b"line\n")
        buf.seek(0)
        lines = list(decompress_file_classified(buf, "application/gzip", "mongosync.log.gz"))
        assert len(lines) == 1

    def test_routes_zip_by_mime(self, tmp_path):
        zip_path = tmp_path / "bundle.zip"
        with zipfile.ZipFile(zip_path, "w") as zf:
            zf.writestr("mongosync.log", b"line\n")
        with zip_path.open("rb") as f:
            lines = list(decompress_file_classified(f, "application/zip", "bundle.zip"))
        assert len(lines) == 1

    def test_unsupported_mime_raises(self):
        buf = io.BytesIO(b"data")
        with pytest.raises(ValueError, match="Unsupported compressed MIME type"):
            list(decompress_file_classified(buf, "text/plain", "file.txt"))
