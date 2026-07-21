"""Run pip-audit while enforcing the repository's dated vulnerability policy."""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import tempfile
from datetime import date, datetime, timezone
from pathlib import Path

from config import PROJECT_ROOT


def evaluate_audit_report(report: dict, policy: dict, today: date | None = None) -> dict:
    current_date = today or datetime.now(timezone.utc).date()
    review_by = date.fromisoformat(str(policy["review_by"]))
    allowed = {str(name).lower() for name in policy.get("temporarily_allowed_packages", [])}
    findings = []
    unexpected = []
    for dependency in report.get("dependencies", []):
        vulnerabilities = dependency.get("vulns") or []
        if not vulnerabilities:
            continue
        row = {
            "package": str(dependency.get("name", "unknown")),
            "version": dependency.get("version"),
            "vulnerability_ids": sorted(
                {str(item.get("id", "unknown")) for item in vulnerabilities}
            ),
        }
        findings.append(row)
        if row["package"].lower() not in allowed:
            unexpected.append(row)

    errors = []
    if current_date > review_by:
        errors.append(f"vulnerability exception expired on {review_by.isoformat()}")
    if unexpected:
        errors.append(
            "unexpected vulnerable packages: "
            + ", ".join(sorted({row["package"] for row in unexpected}))
        )
    return {
        "valid": not errors,
        "review_by": review_by.isoformat(),
        "days_until_review": (review_by - current_date).days,
        "vulnerable_packages": findings,
        "unexpected_packages": unexpected,
        "errors": errors,
    }


def run_pip_audit(requirements: Path) -> dict:
    with tempfile.TemporaryDirectory(prefix="hometown-xr-audit-") as temporary:
        output = Path(temporary) / "audit.json"
        process = subprocess.run(
            [
                sys.executable,
                "-m",
                "pip_audit",
                "-r",
                str(requirements),
                "--no-deps",
                "--format",
                "json",
                "--output",
                str(output),
            ],
            check=False,
        )
        if not output.exists():
            raise RuntimeError(f"pip-audit failed with exit code {process.returncode}")
        return json.loads(output.read_text(encoding="utf-8"))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--requirements", default="requirements.txt")
    parser.add_argument(
        "--policy",
        default=str(PROJECT_ROOT / ".github" / "dependency-policy.json"),
    )
    args = parser.parse_args(argv)
    policy = json.loads(Path(args.policy).read_text(encoding="utf-8"))
    report = run_pip_audit(Path(args.requirements))
    result = evaluate_audit_report(report, policy)
    print(json.dumps(result, indent=2))
    return 0 if result["valid"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
