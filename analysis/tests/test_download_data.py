"""Tests for the data download module."""

import io
import zipfile
from datetime import date
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from analysis.download_data import (
    build_filename,
    build_url,
    date_range,
    download_day,
)


class TestDateRange:
    def test_single_day(self):
        result = list(date_range(date(2024, 1, 1), date(2024, 1, 2)))
        assert result == [date(2024, 1, 1)]

    def test_multiple_days(self):
        result = list(date_range(date(2024, 1, 1), date(2024, 1, 4)))
        assert len(result) == 3
        assert result[0] == date(2024, 1, 1)
        assert result[-1] == date(2024, 1, 3)

    def test_empty_range(self):
        result = list(date_range(date(2024, 1, 2), date(2024, 1, 1)))
        assert result == []

    def test_same_day(self):
        result = list(date_range(date(2024, 1, 1), date(2024, 1, 1)))
        assert result == []


class TestBuildFilename:
    def test_format(self):
        assert build_filename(date(2024, 3, 15)) == "BTCUSDT-1s-2024-03-15.zip"

    def test_single_digit_month(self):
        assert build_filename(date(2024, 1, 5)) == "BTCUSDT-1s-2024-01-05.zip"


class TestBuildUrl:
    def test_url_format(self):
        url = build_url(date(2024, 3, 15))
        assert url.startswith("https://data.binance.vision/data/spot/daily/klines/BTCUSDT/1s/")
        assert url.endswith("BTCUSDT-1s-2024-03-15.zip")


class TestDownloadDay:
    def _make_zip_bytes(self, csv_content: str, csv_name: str = "test.csv") -> bytes:
        """Create a ZIP file in memory containing a CSV."""
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w") as zf:
            zf.writestr(csv_name, csv_content)
        return buf.getvalue()

    def test_skips_existing_csv(self, tmp_path):
        csv_path = tmp_path / "BTCUSDT-1s-2024-01-01.csv"
        csv_path.write_text("some,data\n1,2\n")

        session = MagicMock()
        result = download_day(date(2024, 1, 1), tmp_path, session)

        assert result["status"] == "skipped"
        session.get.assert_not_called()

    def test_handles_404(self, tmp_path):
        session = MagicMock()
        response = MagicMock()
        response.status_code = 404
        session.get.return_value = response

        result = download_day(date(2024, 1, 1), tmp_path, session)
        assert result["status"] == "not_available"

    def test_successful_download(self, tmp_path):
        csv_content = "1704067200000,42000,42001,41999,42000.5,1.5,1704067200999,63000,10,0.8,33600,0\n"
        zip_bytes = self._make_zip_bytes(csv_content, "BTCUSDT-1s-2024-01-01.csv")

        session = MagicMock()

        # Main download response
        download_response = MagicMock()
        download_response.status_code = 200
        download_response.content = zip_bytes

        # Checksum response (404 = not available, skip verification)
        checksum_response = MagicMock()
        checksum_response.status_code = 404

        session.get.side_effect = [download_response, checksum_response]

        result = download_day(date(2024, 1, 1), tmp_path, session)

        assert result["status"] == "ok"
        csv_path = tmp_path / "BTCUSDT-1s-2024-01-01.csv"
        assert csv_path.exists()
        assert csv_path.read_text() == csv_content

    def test_handles_corrupt_zip(self, tmp_path):
        session = MagicMock()

        download_response = MagicMock()
        download_response.status_code = 200
        download_response.content = b"not a zip file"

        checksum_response = MagicMock()
        checksum_response.status_code = 404

        session.get.side_effect = [download_response, checksum_response]

        result = download_day(date(2024, 1, 1), tmp_path, session)
        assert result["status"] == "error"
        assert "Corrupt ZIP" in result["message"]
