from audit import build_audit_plan, compare_audit_outputs
from matcher import Match
from output import OutputWriter
from progress import ProgressTracker


def _write(writer, source, text, raw_text=""):
    transaction = writer.begin_source(source)
    transaction.write_matches(
        [
            Match(
                url="https://example.test/story",
                warc_date="2026-01-01",
                text=text,
                raw_text=raw_text,
                matched_keywords=["home"],
                semantic_score=0.8,
                concept_match="home memory",
                crawl_id="crawl",
            )
        ],
        [("en", 0.99)],
    )
    transaction.commit()


def test_audit_plan_and_comparison_preserve_historical_output(tmp_path):
    tracker = ProgressTracker(tmp_path / "historical.db")
    source = "crawl-data/source.wet.gz"
    tracker.initialize_paths([source], "crawl")
    tracker.mark_completed(source, 10, 1, filter_signature="old")
    plan = build_audit_plan("new", 1, tracker=tracker)
    assert plan["total_sources"] == 1
    assert plan["preserves_historical_state"]

    historical = OutputWriter(tmp_path / "historical-output")
    audit = OutputWriter(tmp_path / "audit-output")
    damaged = "I remember my family\u00e2\u20ac\u2122s home &amp; garden."
    repaired = "I remember my family\u2019s home & garden."
    _write(historical, source, damaged)
    _write(audit, source, repaired, raw_text=damaged)

    result = compare_audit_outputs(
        plan,
        {source: {"status": "completed", "error": None}},
        historical_output=historical.output_dir,
        audit_output=audit.output_dir,
    )
    assert result["summary"]["equivalent_normalized_match_sets"]
    assert result["summary"]["normalized_text_repairs"] == 1
    assert result["summary"]["historical_state_changed"] is False
