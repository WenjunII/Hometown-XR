"""Isolated, deterministic audits of completed Common Crawl sources."""

from __future__ import annotations

import gzip
import json
import multiprocessing
import os
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path

from config import (
    AUDIT_DEFAULT_PER_CRAWL,
    AUDIT_DIR,
    AUDIT_SAMPLE_RATE,
    EVALUATION_DIR,
    METRICS_DIR,
    OUTPUT_DIR,
)
from crawl_catalog import get_crawl_info
from evaluation import DecisionSampler
from metrics import MetricsRecorder
from output import OutputWriter
from pipeline import ExtractionPipeline, InferenceService
from progress import ProgressTracker
from record_identity import content_fingerprint
from signatures import build_run_manifest
from text_normalization import normalize_extracted_text


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _gpu_name() -> str:
    try:
        import torch

        if torch.cuda.is_available():
            return torch.cuda.get_device_name(0)
    except (ImportError, RuntimeError):
        pass
    return "CPU"


def _atomic_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    os.replace(temporary, path)


def build_audit_plan(
    current_signature: str,
    per_crawl: int = AUDIT_DEFAULT_PER_CRAWL,
    crawl_ids: list[str] | None = None,
    include_current: bool = False,
    tracker: ProgressTracker | None = None,
) -> dict:
    """Build a read-only, deterministic sample of completed sources."""
    tracker = tracker or ProgressTracker()
    sources = tracker.sample_completed_for_audit(
        current_signature,
        per_crawl,
        crawl_ids=crawl_ids,
        include_current=include_current,
    )
    by_crawl = Counter(str(row["crawl_id"]) for row in sources)
    by_signature = Counter(str(row["signature_state"]) for row in sources)
    return {
        "schema_version": 1,
        "created_at": _utc_now(),
        "filter_signature": current_signature,
        "selection": {
            "per_crawl": per_crawl,
            "requested_crawls": sorted(set(crawl_ids or [])),
            "include_current": include_current,
            "deterministic": True,
        },
        "total_sources": len(sources),
        "sources_by_crawl": dict(sorted(by_crawl.items())),
        "sources_by_signature_state": dict(sorted(by_signature.items())),
        "preserves_historical_state": True,
        "sources": sources,
    }


def _read_source_records(writer: OutputWriter, source_file: str) -> list[dict]:
    records = []
    for path in writer.find_source_outputs(source_file):
        with gzip.open(path, "rt", encoding="utf-8") as handle:
            records.extend(json.loads(line) for line in handle if line.strip())
    return records


def _comparison_counter(records: list[dict]) -> Counter:
    return Counter(
        content_fingerprint(
            str(record.get("url", "")),
            normalize_extracted_text(str(record.get("paragraph", ""))),
        )
        for record in records
    )


def compare_audit_outputs(
    plan: dict,
    states: dict[str, dict],
    historical_output: str | Path = OUTPUT_DIR,
    audit_output: str | Path | None = None,
) -> dict:
    """Compare isolated results with historical shards using normalized identity."""
    if audit_output is None:
        raise ValueError("audit_output is required")
    historical_writer = OutputWriter(historical_output)
    audit_writer = OutputWriter(audit_output)
    sources = []
    totals = Counter()
    per_crawl: dict[str, Counter] = defaultdict(Counter)

    for selected in plan.get("sources", []):
        source_file = str(selected["file_path"])
        crawl_id = str(selected["crawl_id"])
        state = states.get(source_file, {"status": "missing"})
        row = {
            **selected,
            "audit_status": state.get("status", "missing"),
            "audit_error": state.get("error"),
        }
        totals["selected_sources"] += 1
        per_crawl[crawl_id]["selected_sources"] += 1
        if state.get("status") != "completed":
            sources.append(row)
            continue

        historical = _read_source_records(historical_writer, source_file)
        current = _read_source_records(audit_writer, source_file)
        historical_keys = _comparison_counter(historical)
        current_keys = _comparison_counter(current)
        overlap = sum((historical_keys & current_keys).values())
        removed = sum((historical_keys - current_keys).values())
        added = sum((current_keys - historical_keys).values())
        repaired = sum(bool(record.get("raw_paragraph")) for record in current)
        row.update(
            {
                "historical_output_matches": len(historical),
                "audit_matches": len(current),
                "normalized_overlap": overlap,
                "removed_matches": removed,
                "added_matches": added,
                "normalized_text_repairs": repaired,
            }
        )
        totals.update(
            {
                "completed_sources": 1,
                "historical_matches": len(historical),
                "audit_matches": len(current),
                "normalized_overlap": overlap,
                "removed_matches": removed,
                "added_matches": added,
                "normalized_text_repairs": repaired,
                "sources_with_match_delta": int(added > 0 or removed > 0),
            }
        )
        per_crawl[crawl_id].update(
            {
                "completed_sources": 1,
                "historical_matches": len(historical),
                "audit_matches": len(current),
                "removed_matches": removed,
                "added_matches": added,
            }
        )
        sources.append(row)

    completed = totals["completed_sources"]
    equivalent = (
        completed == totals["selected_sources"]
        and totals["added_matches"] == 0
        and totals["removed_matches"] == 0
    )
    return {
        "summary": {
            **dict(totals),
            "equivalent_normalized_match_sets": equivalent,
            "historical_state_changed": False,
        },
        "by_crawl": {
            crawl_id: dict(values) for crawl_id, values in sorted(per_crawl.items())
        },
        "sources": sources,
    }


def run_audit(
    plan: dict,
    settings,
    audit_dir: str | Path = AUDIT_DIR,
    sample_rate: float = AUDIT_SAMPLE_RATE,
    context=None,
    shutdown_event=None,
) -> dict:
    """Run selected sources against isolated state and publish only evaluation samples."""
    if not 0 <= sample_rate <= 1:
        raise ValueError("sample_rate must be between 0 and 1")
    if not plan.get("sources"):
        raise ValueError("audit plan contains no sources")
    if plan.get("filter_signature") != settings.filter_signature:
        raise ValueError("audit plan and runtime filter signatures do not match")

    root = Path(audit_dir) / settings.run_id
    if root.exists():
        raise FileExistsError(f"audit directory already exists: {root}")
    root.mkdir(parents=True)
    _atomic_json(root / "plan.json", plan)

    tracker = ProgressTracker(root / "progress.db")
    sources_by_crawl: dict[str, list[str]] = defaultdict(list)
    for source in plan["sources"]:
        sources_by_crawl[str(source["crawl_id"])].append(str(source["file_path"]))
    for crawl_id, source_files in sources_by_crawl.items():
        tracker.initialize_paths(source_files, crawl_id)

    crawl_ids = sorted(sources_by_crawl)
    provenance = build_run_manifest(
        settings,
        crawl_ids,
        "audit-stratified",
        len(plan["sources"]),
        int(plan["selection"]["per_crawl"]),
    )
    provenance["mode"] = "isolated_audit"
    provenance["historical_state_changed"] = False
    metrics = MetricsRecorder(
        profile=settings.profile_name,
        workers=settings.workers,
        inference_batch_size=settings.inference_batch_size,
        metrics_dir=METRICS_DIR,
        gpu_name=_gpu_name(),
        provenance=provenance,
    )
    metrics.add_target_files(len(plan["sources"]))
    writer = OutputWriter(
        root / "output",
        run_id=settings.run_id,
        filter_signature=settings.filter_signature,
    )
    sampler = DecisionSampler(
        EVALUATION_DIR / "candidate_samples.jsonl",
        sample_rate=sample_rate,
    )
    context = context or multiprocessing.get_context("spawn")
    shutdown_event = shutdown_event or context.Event()
    service = None
    pipeline_entered = False
    try:
        service = InferenceService(
            settings,
            metrics,
            writer=writer,
            sampler=sampler,
        )
        with ExtractionPipeline(
            settings,
            context,
            metrics,
            shutdown_event=shutdown_event,
            service=service,
        ) as pipeline:
            pipeline_entered = True
            for crawl_id in crawl_ids:
                if shutdown_event.is_set():
                    break
                pipeline.process_crawl(
                    tracker,
                    get_crawl_info(crawl_id),
                    len(sources_by_crawl[crawl_id]),
                )
    finally:
        if service is not None and not pipeline_entered:
            service.close()
        metrics_snapshot = metrics.close()

    states = tracker.get_file_states(
        str(source["file_path"]) for source in plan["sources"]
    )
    comparison = compare_audit_outputs(
        plan,
        states,
        historical_output=OUTPUT_DIR,
        audit_output=root / "output",
    )
    report = {
        "schema_version": 1,
        "audit_id": settings.run_id,
        "created_at": _utc_now(),
        "audit_root": str(root),
        "filter_signature": settings.filter_signature,
        "sample_rate": sample_rate,
        "progress": tracker.get_summary(),
        "failures": tracker.get_failure_summary(examples_per_category=2),
        "metrics": metrics_snapshot,
        **comparison,
    }
    _atomic_json(root / "report.json", report)
    return report
