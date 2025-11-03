"""Lightweight utility to inspect Sierra Chart .scid files."""
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import List, Optional, Sequence

from sierrapy.parser.scid_parse import FastScidReader


def _peek_file(path: Path) -> dict:
    entry: dict = {"path": str(path)}
    if not path.exists():
        entry["error"] = "not found"
        return entry

    try:
        with FastScidReader(str(path), read_only=True).open() as reader:
            try:
                n, start, end = reader.peek_range()
            except AttributeError:
                n = reader.count
                if n == 0:
                    start = end = None
                else:
                    times = reader.times_epoch_ms()
                    start = int(times[0])
                    end = int(times[-1])
    except Exception as exc:  # pragma: no cover - defensive
        entry["error"] = str(exc)
        return entry

    entry.update({"count": n, "start": start, "end": end})
    return entry


def _gather_paths(root: Path, pattern: Optional[str]) -> Sequence[Path]:
    if root.is_dir():
        glob = pattern or "*.scid"
        return sorted(root.glob(glob))
    return [root]


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Peek at SCID timestamp ranges")
    parser.add_argument("path", help="Path to a .scid file or directory")
    parser.add_argument("--glob", help="Glob when --path is a directory")
    parser.add_argument("--json", action="store_true", help="Emit JSON output")
    args = parser.parse_args(argv)

    targets = _gather_paths(Path(args.path), args.glob)

    results: List[dict] = [_peek_file(path) for path in targets]

    if args.json:
        payload = results[0] if len(results) == 1 else results
        print(json.dumps(payload))
    else:
        for result in results:
            if "error" in result:
                print(f"{result['path']}: error={result['error']}")
            else:
                print(
                    f"{result['path']}: count={result['count']} start={result['start']} end={result['end']}"
                )

    return 0


if __name__ == "__main__":  # pragma: no cover - CLI entry point
    raise SystemExit(main())
