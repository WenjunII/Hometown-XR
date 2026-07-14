"""Streaming, partitioned Parquet export with optional deduplication."""

from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import uuid
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

from config import OUTPUT_DIR, OUTPUT_SCHEMA_VERSION, PARQUET_DIR
from dedupe import DedupIndex
from evaluation import iter_output_records
from record_identity import content_fingerprint, stable_record_id

_SAFE_PARTITION = re.compile(r"[^A-Za-z0-9._-]+")


def _partition_value(value: str) -> str:
    return _SAFE_PARTITION.sub("_", value or "unknown")[:120] or "unknown"


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _record_with_identity(record: dict) -> dict:
    paragraph = str(record.get("paragraph", ""))
    source_file = str(record.get("source_file", ""))
    record_id = record.get("record_id") or stable_record_id(
        str(record.get("crawl_id", "")),
        source_file,
        str(record.get("url", "")),
        str(record.get("warc_date", "")),
        paragraph,
    )
    return {
        "schema_version": int(record.get("schema_version", OUTPUT_SCHEMA_VERSION)),
        "record_id": str(record_id),
        "content_fingerprint": str(
            record.get("content_fingerprint")
            or content_fingerprint(str(record.get("url", "")), paragraph)
        ),
        "crawl_id": str(record.get("crawl_id", "") or "unknown"),
        "source_file": source_file,
        "url": str(record.get("url", "")),
        "warc_date": str(record.get("warc_date", "")),
        "language": str(record.get("language", "unknown") or "unknown"),
        "language_confidence": float(record.get("language_confidence", 0.0) or 0.0),
        "paragraph": paragraph,
        "matched_keywords": [str(value) for value in record.get("matched_keywords", [])],
        "semantic_score": float(record.get("semantic_score", 0.0) or 0.0),
        "concept_match": str(record.get("concept_match", "")),
    }


def export_parquet(
    output_dir: str | Path = OUTPUT_DIR,
    parquet_dir: str | Path = PARQUET_DIR,
    dedupe: str = "exact",
    near_distance: int = 3,
    batch_size: int = 1_000,
) -> dict:
    """Build a new dataset in staging and atomically replace the prior export."""
    if dedupe not in {"none", "exact", "near"}:
        raise ValueError("dedupe must be none, exact, or near")
    if batch_size <= 0:
        raise ValueError("batch_size must be positive")

    import pyarrow as pa
    import pyarrow.parquet as pq

    target = Path(parquet_dir)
    target.parent.mkdir(parents=True, exist_ok=True)
    staging = target.parent / f".{target.name}-staging-{uuid.uuid4().hex}"
    staging.mkdir(parents=True)
    backup = target.parent / f".{target.name}-backup-{uuid.uuid4().hex}"
    schema = pa.schema(
        [
            ("schema_version", pa.int16()),
            ("record_id", pa.string()),
            ("content_fingerprint", pa.string()),
            ("crawl_id", pa.string()),
            ("source_file", pa.string()),
            ("url", pa.string()),
            ("warc_date", pa.string()),
            ("language", pa.string()),
            ("language_confidence", pa.float32()),
            ("paragraph", pa.string()),
            ("matched_keywords", pa.list_(pa.string())),
            ("semantic_score", pa.float32()),
            ("concept_match", pa.string()),
        ]
    )
    buffers: dict[tuple[str, str], list[dict]] = defaultdict(list)
    part_numbers: dict[tuple[str, str], int] = defaultdict(int)
    partition_rows: dict[str, int] = defaultdict(int)
    rows_written = 0
    buffered_rows = 0
    duplicates = {"exact": 0, "near": 0}
    duplicate_path = staging / "_duplicates.jsonl"

    def flush_partition(key: tuple[str, str]) -> None:
        nonlocal buffered_rows, rows_written
        records = buffers[key]
        if not records:
            return
        crawl_id, language = key
        relative_dir = Path(f"crawl_id={_partition_value(crawl_id)}") / (
            f"language={_partition_value(language)}"
        )
        part_number = part_numbers[key]
        part_numbers[key] += 1
        path = staging / relative_dir / f"part-{part_number:05d}.parquet"
        path.parent.mkdir(parents=True, exist_ok=True)
        table = pa.Table.from_pylist(records, schema=schema)
        pq.write_table(table, path, compression="zstd")
        partition_rows[relative_dir.as_posix()] += len(records)
        rows_written += len(records)
        buffered_rows -= len(records)
        buffers[key] = []

    try:
        with DedupIndex(staging / "_dedupe.sqlite", near_distance=near_distance) as index:
            with duplicate_path.open("w", encoding="utf-8") as duplicate_handle:
                for source_record in iter_output_records(output_dir):
                    record = _record_with_identity(source_record)
                    duplicate = index.check_and_add(
                        record["record_id"],
                        record["content_fingerprint"],
                        record["paragraph"],
                        dedupe,
                    )
                    if duplicate:
                        duplicates[duplicate.kind] += 1
                        duplicate_handle.write(
                            json.dumps(
                                {
                                    "record_id": record["record_id"],
                                    "canonical_record_id": duplicate.canonical_record_id,
                                    "kind": duplicate.kind,
                                    "distance": duplicate.distance,
                                }
                            )
                            + "\n"
                        )
                        continue
                    key = (record["crawl_id"], record["language"])
                    buffers[key].append(record)
                    buffered_rows += 1
                    if len(buffers[key]) >= batch_size:
                        flush_partition(key)
                    elif buffered_rows >= batch_size * 10:
                        largest = max(buffers, key=lambda item: len(buffers[item]))
                        flush_partition(largest)

        for key in list(buffers):
            flush_partition(key)
        (staging / "_dedupe.sqlite").unlink(missing_ok=True)

        parquet_files = sorted(staging.rglob("*.parquet"))
        manifest = {
            "schema_version": OUTPUT_SCHEMA_VERSION,
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "dedupe_mode": dedupe,
            "near_distance": near_distance if dedupe == "near" else None,
            "rows": rows_written,
            "duplicates": duplicates,
            "partitions": dict(sorted(partition_rows.items())),
            "files": [
                {
                    "path": path.relative_to(staging).as_posix(),
                    "bytes": path.stat().st_size,
                    "sha256": _sha256(path),
                }
                for path in parquet_files
            ],
        }
        (staging / "_manifest.json").write_text(
            json.dumps(manifest, indent=2) + "\n",
            encoding="utf-8",
        )

        if target.exists():
            os.replace(target, backup)
        try:
            os.replace(staging, target)
        except Exception:
            if backup.exists():
                os.replace(backup, target)
            raise
        else:
            shutil.rmtree(backup, ignore_errors=True)
        return manifest
    except Exception:
        shutil.rmtree(staging, ignore_errors=True)
        raise
