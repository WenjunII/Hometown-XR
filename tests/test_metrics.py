import gzip
import json

from metrics import (
    MetricsRecorder,
    compact_run_history,
    compare_profiles,
    concise_metrics,
    summarize_run_history,
)


def test_run_provenance_compacts_into_deterministic_shared_history(tmp_path):
    metrics_dir = tmp_path / "metrics"
    target = tmp_path / "run-history.jsonl.gz"
    recorder = MetricsRecorder(
        "3080",
        7,
        800,
        metrics_dir,
        "RTX 3080",
        provenance={"run_id": "run-one", "filter_signature": "abc"},
    )
    recorder.close()

    result = compact_run_history(metrics_dir, target)
    first_bytes = target.read_bytes()
    with gzip.open(target, "rt", encoding="utf-8") as handle:
        rows = [json.loads(line) for line in handle if line.strip()]
    assert result["runs"] == 1
    assert rows[0]["session_id"] == "run-one"
    assert rows[0]["provenance"]["filter_signature"] == "abc"

    compact_run_history(metrics_dir, target)
    assert target.read_bytes() == first_bytes


def test_metrics_expose_funnel_failures_resources_and_concise_history(tmp_path):
    metrics_dir = tmp_path / "metrics"
    recorder = MetricsRecorder(
        "3080",
        7,
        800,
        metrics_dir,
        "RTX 3080",
        provenance={"run_id": "run-two", "filter_signature": "def"},
    )
    recorder.add_target_files(2)
    recorder.record_inference(10, 3, 1.0, runtime_stats={"peak_vram_mb": 2048})
    recorder.record_source(
        "completed",
        20,
        10,
        2,
        1_000_000,
        1.0,
        eligible_paragraphs=100,
        keyword_rejected=90,
        peak_worker_rss_bytes=512 * 1024**2,
    )
    recorder.record_source(
        "failed",
        0,
        0,
        0,
        0,
        0.1,
        error="HTTP 503 service unavailable",
    )
    payload = recorder.close()

    assert payload["acceptance_funnel"] == {
        "eligible_paragraphs": 100,
        "keyword_rejected": 90,
        "keyword_candidates": 10,
        "filter_accepted": 3,
        "committed": 2,
        "filter_rejected": 7,
    }
    assert payload["failure_categories"] == {"http_503": 1}
    assert payload["resources"]["peak_worker_rss_mb"] == 512.0
    assert payload["resources"]["peak_vram_mb"] == 2048.0
    assert concise_metrics(payload)["filter_signature"] == "def"

    history = summarize_run_history(
        metrics_dir=metrics_dir,
        history_path=tmp_path / "missing.jsonl.gz",
    )
    profiles = compare_profiles(
        metrics_dir=metrics_dir,
        history_path=tmp_path / "missing.jsonl.gz",
    )
    assert history["shown"] == 1
    assert profiles["profiles"]["3080"]["files_completed"] == 1
