"""Real-corpus sampling, human annotation, and filter evaluation."""

from __future__ import annotations

import gzip
import hashlib
import heapq
import json
import os
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Iterable, Iterator

from config import (
    EVALUATION_DIR,
    EVALUATION_MAX_SAMPLES_PER_SESSION,
    EVALUATION_SAMPLE_RATE,
    MIN_NARRATIVE_INDICATORS,
    OUTPUT_DIR,
)
from record_identity import stable_record_id

if TYPE_CHECKING:
    from language_detector import LanguageDetector
    from matcher import MatchDecision


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _atomic_jsonl(path: Path, rows: Iterable[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    with temporary.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False) + "\n")
    os.replace(temporary, path)


def _read_jsonl(path: Path) -> list[dict]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8") as handle:
        return [json.loads(line) for line in handle if line.strip()]


class DecisionSampler:
    """Persist a deterministic, bounded sample of live filter decisions."""

    def __init__(
        self,
        path: str | Path = EVALUATION_DIR / "candidate_samples.jsonl",
        sample_rate: float = EVALUATION_SAMPLE_RATE,
        max_samples: int = EVALUATION_MAX_SAMPLES_PER_SESSION,
    ):
        self.path = Path(path)
        self.sample_rate = sample_rate
        self.max_samples = max_samples
        self.written = 0
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._known = {row.get("sample_id") for row in _read_jsonl(self.path)}

    def observe(
        self,
        decisions: list[MatchDecision],
        language_detector: LanguageDetector,
    ) -> int:
        if self.written >= self.max_samples:
            return 0
        selected = []
        for decision in decisions:
            paragraph = decision.paragraph
            sample_id = stable_record_id(
                paragraph.crawl_id,
                paragraph.source_file,
                paragraph.url,
                paragraph.warc_date,
                paragraph.text,
            )
            rank = int(sample_id[:16], 16) / float(2**64)
            if rank >= self.sample_rate or sample_id in self._known:
                continue
            language, confidence = language_detector.detect(paragraph.text)
            selected.append(
                {
                    "schema_version": 1,
                    "sample_id": sample_id,
                    "collected_at": _utc_now(),
                    "crawl_id": paragraph.crawl_id,
                    "source_file": paragraph.source_file,
                    "url": paragraph.url,
                    "warc_date": paragraph.warc_date,
                    "language": language,
                    "language_confidence": round(confidence, 4),
                    "paragraph": paragraph.text,
                    "matched_keywords": decision.matched_keywords,
                    "semantic_score": round(decision.semantic_score, 6),
                    "concept_match": decision.concept_match,
                    "narrative_score": decision.narrative_score,
                    "predicted_accept": decision.accepted,
                    "rejection_reason": decision.rejection_reason,
                }
            )
            self._known.add(sample_id)
            if self.written + len(selected) >= self.max_samples:
                break

        if selected:
            with self.path.open("a", encoding="utf-8") as handle:
                for row in selected:
                    handle.write(json.dumps(row, ensure_ascii=False) + "\n")
            self.written += len(selected)
        return len(selected)


def iter_output_records(output_dir: str | Path = OUTPUT_DIR) -> Iterator[dict]:
    root = Path(output_dir)
    if not root.exists():
        return
    for language_dir in sorted(root.iterdir()):
        if not language_dir.is_dir() or language_dir.name.startswith((".", "_")):
            continue
        for path in sorted(language_dir.glob("*.jsonl.gz")):
            with gzip.open(path, "rt", encoding="utf-8") as handle:
                for line in handle:
                    if line.strip():
                        yield json.loads(line)


def _rank(row: dict) -> int:
    sample_id = str(row.get("sample_id") or row.get("record_id") or row.get("paragraph", ""))
    return int(hashlib.sha256(sample_id.encode("utf-8")).hexdigest()[:16], 16)


def _stratified_pick(rows: Iterable[dict], limit: int) -> list[dict]:
    if limit <= 0:
        return []
    buckets: dict[str, list[tuple[int, str, dict]]] = defaultdict(list)
    for sequence, row in enumerate(rows):
        stratum = str(row.get("language") or "unknown")
        sample_id = str(row.get("sample_id") or row.get("record_id") or _rank(row))
        item = (-_rank(row), sample_id, sequence, row)
        bucket = buckets[stratum]
        if len(bucket) < limit:
            heapq.heappush(bucket, item)
        elif item > bucket[0]:
            heapq.heapreplace(bucket, item)

    ordered = {
        language: [item[3] for item in sorted(items, key=lambda value: -value[0])]
        for language, items in buckets.items()
    }
    selected = []
    while len(selected) < limit:
        added = False
        for language in sorted(ordered):
            if ordered[language]:
                selected.append(ordered[language].pop(0))
                added = True
                if len(selected) == limit:
                    break
        if not added:
            break
    return selected


def _output_annotation_rows(output_dir: str | Path) -> Iterator[dict]:
    for record in iter_output_records(output_dir):
        source_file = record.get("source_file", "")
        sample_id = record.get("record_id") or stable_record_id(
            record.get("crawl_id", ""),
            source_file,
            record.get("url", ""),
            record.get("warc_date", ""),
            record.get("paragraph", ""),
        )
        yield {
            "schema_version": 1,
            "sample_id": sample_id,
            "sample_origin": "committed_output",
            "crawl_id": record.get("crawl_id", ""),
            "source_file": source_file,
            "url": record.get("url", ""),
            "warc_date": record.get("warc_date", ""),
            "language": record.get("language", "unknown"),
            "paragraph": record.get("paragraph", ""),
            "matched_keywords": record.get("matched_keywords", []),
            "semantic_score": record.get("semantic_score"),
            "concept_match": record.get("concept_match", ""),
            "narrative_score": record.get("narrative_score", MIN_NARRATIVE_INDICATORS),
            "predicted_accept": True,
            "rejection_reason": None,
            "label": None,
            "notes": "",
        }


def build_annotation_sample(
    size: int = 400,
    output_dir: str | Path = OUTPUT_DIR,
    candidate_path: str | Path = EVALUATION_DIR / "candidate_samples.jsonl",
    annotation_path: str | Path = EVALUATION_DIR / "annotations.jsonl",
) -> dict:
    """Create a balanced, language-stratified sample of real project text."""
    if size <= 0:
        raise ValueError("sample size must be positive")
    annotation_path = Path(annotation_path)
    candidate_path = Path(candidate_path)
    existing = {row["sample_id"]: row for row in _read_jsonl(annotation_path)}

    positive_target = size // 2
    output_rows = _stratified_pick(_output_annotation_rows(output_dir), positive_target)
    candidate_rows = _read_jsonl(candidate_path)
    rejected = [
        {
            **row,
            "sample_origin": "live_candidate",
            "label": None,
            "notes": "",
        }
        for row in candidate_rows
        if not row.get("predicted_accept", False)
    ]
    rejected_rows = _stratified_pick(rejected, size - len(output_rows))

    rows = output_rows + rejected_rows
    if len(rows) < size:
        selected_ids = {row["sample_id"] for row in rows}
        extras = [
            {
                **row,
                "sample_origin": "live_candidate",
                "label": None,
                "notes": "",
            }
            for row in candidate_rows
            if row.get("sample_id") not in selected_ids
        ]
        rows.extend(_stratified_pick(extras, size - len(rows)))
    if len(rows) < size:
        selected_ids = {row["sample_id"] for row in rows}
        extra_output = (
            row
            for row in _output_annotation_rows(output_dir)
            if row["sample_id"] not in selected_ids
        )
        rows.extend(_stratified_pick(extra_output, size - len(rows)))

    for row in rows:
        old = existing.get(row["sample_id"])
        if old:
            row["label"] = old.get("label")
            row["notes"] = old.get("notes", "")
    selected_ids = {row["sample_id"] for row in rows}
    for old in existing.values():
        if old.get("label") not in {"positive", "negative"}:
            continue
        if old["sample_id"] not in selected_ids:
            rows.append(old)
            selected_ids.add(old["sample_id"])
    while len(rows) > size:
        removable = next(
            (
                index
                for index in range(len(rows) - 1, -1, -1)
                if rows[index].get("label") not in {"positive", "negative"}
            ),
            None,
        )
        if removable is None:
            break
        rows.pop(removable)
    rows.sort(key=lambda row: (str(row.get("language", "")), _rank(row)))
    _atomic_jsonl(annotation_path, rows)
    return {
        "path": str(annotation_path),
        "samples": len(rows),
        "predicted_positive": sum(bool(row.get("predicted_accept")) for row in rows),
        "predicted_negative": sum(not bool(row.get("predicted_accept")) for row in rows),
        "labeled": sum(row.get("label") in {"positive", "negative"} for row in rows),
    }


def annotate(
    annotation_path: str | Path = EVALUATION_DIR / "annotations.jsonl",
) -> None:
    path = Path(annotation_path)
    rows = _read_jsonl(path)
    if not rows:
        raise FileNotFoundError(f"No annotation sample at {path}; run evaluation sample first")

    for index, row in enumerate(rows, start=1):
        if row.get("label") in {"positive", "negative"}:
            continue
        print("\n" + "=" * 78)
        print(f"Sample {index}/{len(rows)} | language={row.get('language', 'unknown')}")
        print(
            f"Model={'ACCEPT' if row.get('predicted_accept') else 'REJECT'} | "
            f"semantic={row.get('semantic_score')} | narrative={row.get('narrative_score')}"
        )
        print("-" * 78)
        print(row.get("paragraph", ""))
        answer = input("\n[p]ositive [n]egative [s]kip [q]uit: ").strip().lower()
        if answer == "q":
            break
        if answer == "p":
            row["label"] = "positive"
        elif answer == "n":
            row["label"] = "negative"
        else:
            continue
        _atomic_jsonl(path, rows)


def _classification(rows: list[dict], semantic: float | None = None, narrative: int | None = None) -> dict:
    confusion = {"tp": 0, "fp": 0, "tn": 0, "fn": 0}
    for row in rows:
        actual = row["label"] == "positive"
        if semantic is None or narrative is None:
            predicted = bool(row.get("predicted_accept"))
        else:
            score = row.get("semantic_score")
            narrative_score = row.get("narrative_score")
            if score is None or narrative_score is None:
                continue
            predicted = float(score) >= semantic and int(narrative_score) >= narrative
        if predicted and actual:
            confusion["tp"] += 1
        elif predicted:
            confusion["fp"] += 1
        elif actual:
            confusion["fn"] += 1
        else:
            confusion["tn"] += 1

    tp, fp, fn = confusion["tp"], confusion["fp"], confusion["fn"]
    precision = tp / (tp + fp) if tp + fp else 0.0
    recall = tp / (tp + fn) if tp + fn else 0.0
    f1 = 2 * precision * recall / (precision + recall) if precision + recall else 0.0
    return {
        **confusion,
        "precision": round(precision, 4),
        "recall": round(recall, 4),
        "f1": round(f1, 4),
    }


def evaluation_report(
    annotation_path: str | Path = EVALUATION_DIR / "annotations.jsonl",
    report_path: str | Path = EVALUATION_DIR / "report.json",
) -> dict:
    rows = [
        row
        for row in _read_jsonl(Path(annotation_path))
        if row.get("label") in {"positive", "negative"}
    ]
    if not rows:
        raise ValueError("No human-labeled rows are available")

    by_language = {}
    for language in sorted({str(row.get("language", "unknown")) for row in rows}):
        language_rows = [row for row in rows if str(row.get("language", "unknown")) == language]
        by_language[language] = {"samples": len(language_rows), **_classification(language_rows)}

    best = None
    for semantic_step in range(30, 71):
        semantic = semantic_step / 100
        for narrative in range(0, 17):
            result = _classification(rows, semantic, narrative)
            candidate = {
                "semantic_threshold": semantic,
                "narrative_threshold": narrative,
                **result,
            }
            if best is None or (candidate["f1"], candidate["precision"], candidate["recall"]) > (
                best["f1"],
                best["precision"],
                best["recall"],
            ):
                best = candidate

    payload = {
        "schema_version": 1,
        "generated_at": _utc_now(),
        "human_labeled_samples": len(rows),
        "overall": _classification(rows),
        "by_language": by_language,
        "recommended_thresholds": best,
        "false_positives": [
            row["sample_id"]
            for row in rows
            if row.get("predicted_accept") and row["label"] == "negative"
        ],
        "false_negatives": [
            row["sample_id"]
            for row in rows
            if not row.get("predicted_accept") and row["label"] == "positive"
        ],
    }
    report_path = Path(report_path)
    report_path.parent.mkdir(parents=True, exist_ok=True)
    temporary = report_path.with_suffix(report_path.suffix + ".tmp")
    temporary.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    os.replace(temporary, report_path)
    return payload
