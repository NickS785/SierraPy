from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

from tests.utils_scid import write_scid_file


def test_scid_peek_cli_json(tmp_path: Path) -> None:
    timestamps = [1_700_000_000_000, 1_700_000_010_000]
    path = write_scid_file(tmp_path, "cli.scid", timestamps)

    result = subprocess.run(
        [sys.executable, "-m", "sierra_scid_peek", str(path), "--json"],
        check=True,
        capture_output=True,
        text=True,
    )

    payload = json.loads(result.stdout)
    assert payload["path"].endswith("cli.scid")
    assert payload["count"] == len(timestamps)
    assert payload["start"] == timestamps[0]
    assert payload["end"] == timestamps[-1]
