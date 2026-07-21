from datetime import date

from dependency_audit import evaluate_audit_report

POLICY = {
    "review_by": "2026-08-31",
    "temporarily_allowed_packages": ["torch", "transformers"],
}


def test_known_model_stack_findings_are_allowed_before_review_date():
    report = {
        "dependencies": [
            {"name": "torch", "version": "2.1.0", "vulns": [{"id": "TEST-1"}]},
            {
                "name": "transformers",
                "version": "4.40.2",
                "vulns": [{"id": "TEST-2"}],
            },
        ]
    }

    result = evaluate_audit_report(report, POLICY, today=date(2026, 7, 21))

    assert result["valid"]
    assert not result["unexpected_packages"]


def test_new_vulnerable_package_or_expired_policy_fails():
    report = {
        "dependencies": [
            {"name": "requests", "version": "1.0", "vulns": [{"id": "TEST-3"}]}
        ]
    }

    result = evaluate_audit_report(report, POLICY, today=date(2026, 9, 1))

    assert not result["valid"]
    assert len(result["errors"]) == 2
