"""Command-line entry point for the Hometown XR Common Crawl extractor."""

from __future__ import annotations

import argparse
import json
import logging
import multiprocessing
import shutil
import signal
import sys

from config import (
    DATA_DIR,
    DB_PATH,
    DEFAULT_CRAWL_ID,
    HARDWARE_PROFILES,
    LANG_DETECTION_THRESHOLD,
    LEASE_TIMEOUT_SECONDS,
    OUTPUT_DIR,
    PARQUET_DIR,
    SEMANTIC_THRESHOLD,
    HardwareProfile,
    get_hardware_profile,
)
from crawl_catalog import (
    LEGACY_CRAWLS,
    get_all_crawl_ids,
    get_crawl_info,
    get_modern_crawls,
)
from downloader import fetch_file_paths
from metrics import MetricsRecorder, print_latest
from output import OutputWriter
from pipeline import ExtractionPipeline
from progress import ProgressTracker
from run_lock import CrawlerRunLock
from runtime import RuntimeSettings

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-5s | %(name)-20s | %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("main")

_shutdown_event = None


def _signal_handler(signum, frame) -> None:
    del signum, frame
    if _shutdown_event and _shutdown_event.is_set():
        logger.warning("Second shutdown request received; exiting immediately.")
        raise SystemExit(1)
    if _shutdown_event:
        _shutdown_event.set()
    logger.info("Shutdown requested. Returning active sources to the checkpoint safely...")


signal.signal(signal.SIGINT, _signal_handler)
signal.signal(signal.SIGTERM, _signal_handler)


def _gpu_name() -> str:
    try:
        import torch

        if torch.cuda.is_available():
            return torch.cuda.get_device_name(0)
    except (ImportError, RuntimeError):
        pass
    return "CPU"


def process_crawl(
    crawl_id: str,
    limit: int | None,
    settings: RuntimeSettings,
    pipeline: ExtractionPipeline,
) -> tuple[int, int]:
    crawl_info = get_crawl_info(crawl_id)
    format_name = "ARC (HTML)" if crawl_info.era == "legacy" else "WET (text)"
    logger.info("--- Crawl: %s [%s] ---", crawl_id, format_name)

    tracker = ProgressTracker()
    tracker.recover_stale_leases(LEASE_TIMEOUT_SECONDS)
    logger.info("Fetching file list...")
    file_paths = fetch_file_paths(crawl_info)
    if not file_paths:
        logger.warning("No files found for %s. Skipping.", crawl_id)
        return 0, 0

    tracker.initialize_paths(file_paths, crawl_id)
    summary = tracker.get_summary(crawl_id)
    logger.info(
        "Progress: %s/%s completed, %s pending, %s retryable, %s matches",
        summary["completed"],
        summary["total_files"],
        summary["pending"],
        summary["retryable"],
        summary["total_matches"],
    )
    ready = int(summary["ready"])
    target = min(limit, ready) if limit is not None else ready
    if target <= 0:
        logger.info("No ready files to process for %s.", crawl_id)
        return 0, 0

    pipeline.metrics.add_target_files(target)
    logger.info(
        "Running %s CPU parsers into one GPU inference service (%s sources)...",
        settings.workers,
        target,
    )
    return pipeline.process_crawl(tracker, crawl_info, target)


def run(crawl_ids: list[str], limit: int | None, settings: RuntimeSettings) -> None:
    global _shutdown_event
    context = multiprocessing.get_context("spawn")
    _shutdown_event = context.Event()
    metrics = MetricsRecorder(
        profile=settings.profile_name,
        workers=settings.workers,
        inference_batch_size=settings.inference_batch_size,
        gpu_name=_gpu_name(),
    )

    try:
        with CrawlerRunLock(settings.profile_name):
            OutputWriter().cleanup_stale_staging()
            logger.info("=" * 70)
            logger.info("Hometown XR Common Crawl Extractor")
            logger.info("Crawls: %s", len(crawl_ids))
            logger.info("Profile: %s", settings.profile_name)
            logger.info("CPU parser workers: %s", settings.workers)
            logger.info("Candidate batch: %s", settings.candidate_batch_size)
            logger.info("Shared inference batch: %s", settings.inference_batch_size)
            logger.info("Semantic threshold: %s", settings.semantic_threshold)
            logger.info("=" * 70)

            total_files = 0
            total_matches = 0
            attempted = 0
            with ExtractionPipeline(
                settings,
                context,
                metrics,
                shutdown_event=_shutdown_event,
            ) as pipeline:
                for crawl_id in crawl_ids:
                    if _shutdown_event.is_set():
                        break
                    attempted += 1
                    files, matches = process_crawl(crawl_id, limit, settings, pipeline)
                    total_files += files
                    total_matches += matches

            logger.info("=" * 70)
            logger.info("Crawls attempted: %s", attempted)
            logger.info("Files completed: %s", total_files)
            logger.info("Matches committed: %s", total_matches)
            logger.info("=" * 70)
    finally:
        metrics.close()
        _shutdown_event = None


def show_status() -> None:
    summary = ProgressTracker().get_summary()
    print("\n" + "=" * 62)
    print("  Hometown XR Extractor - Overall Status")
    print("=" * 62)
    print(f"  Total files:       {summary['total_files']}")
    print(f"  Completed:         {summary['completed']}")
    print(f"  Pending:           {summary['pending']}")
    print(f"  Processing:        {summary['processing']}")
    print(f"  Failed:            {summary['failed']}")
    print(f"  Retryable now:     {summary['retryable']}")
    print(f"  Attempts exhausted:{summary['exhausted']:>8}")
    print(f"  Progress:          {summary['progress_pct']:.2f}%")
    print("  ----------------------")
    print(f"  Records processed: {summary['total_records']:,}")
    print(f"  Matches found:     {summary['total_matches']:,}")
    print("=" * 62)

    rows = ProgressTracker().get_per_crawl_summary()
    if rows:
        print("\n  Per-Crawl Breakdown:")
        print(f"  {'Crawl ID':<25} {'Done':>8} {'Total':>8} {'Failed':>8} {'Matches':>10}")
        for row in rows:
            print(
                f"  {row['crawl_id']:<25} {row['completed']:>8} "
                f"{row['total']:>8} {row['failed']:>8} {row['matches']:>10}"
            )
    print()


def list_crawls() -> None:
    print("\n" + "=" * 60)
    print("  Available Common Crawl Datasets")
    print("=" * 60)
    print("\n  LEGACY CRAWLS (2008-2012) - ARC format")
    for crawl in LEGACY_CRAWLS:
        print(f"  {crawl.crawl_id:<25} {crawl.notes}")

    print("\n  MODERN CRAWLS (2013-present) - WET format")
    current_year = None
    modern_crawls = get_modern_crawls()
    for crawl_id in reversed(modern_crawls):
        year = crawl_id.split("-")[2]
        if year != current_year:
            current_year = year
            print(f"\n  {year}:")
        print(f"    {crawl_id}")
    print(f"\n  Total: {len(LEGACY_CRAWLS) + len(modern_crawls)} crawls\n")


def retry_failed(crawl_id: str | None) -> None:
    with CrawlerRunLock("maintenance"):
        count = ProgressTracker().retry_failed(crawl_id)
    print(f"Reset {count} failed files for immediate retry in {crawl_id or 'all crawls'}.")


def recover_leases(minutes: int) -> None:
    with CrawlerRunLock("maintenance"):
        count = ProgressTracker().recover_stale_leases(minutes * 60)
    print(f"Recovered {count} stale processing leases.")


def reset_data() -> None:
    with CrawlerRunLock("maintenance"):
        if DB_PATH.exists():
            DB_PATH.unlink()
        for directory in (OUTPUT_DIR, DATA_DIR / "exports", PARQUET_DIR):
            if directory.exists():
                shutil.rmtree(directory)
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    print("All extracted output, derivatives, and progress have been reset.")


def doctor(profile_name: str) -> int:
    profile = get_hardware_profile(profile_name)
    print("Hometown XR environment check")
    print(f"  Python: {sys.version.split()[0]}")
    print(f"  Profile: {profile.name}")
    print(f"  Workers: {profile.workers}")
    print(f"  Candidate batch: {profile.candidate_batch_size}")
    print(f"  Inference batch: {profile.inference_batch_size}")
    print(f"  Encoding batch: {profile.encoding_batch_size}")
    print(f"  Database: {'present' if DB_PATH.exists() else 'not created'}")
    print(f"  Output directory: {OUTPUT_DIR}")
    try:
        import torch

        print(f"  PyTorch: {torch.__version__}")
        print(f"  CUDA available: {torch.cuda.is_available()}")
        if torch.cuda.is_available():
            print(f"  GPU: {torch.cuda.get_device_name(0)}")
    except ImportError:
        print("  PyTorch: missing")
        return 1
    return 0


def verify_output() -> int:
    with CrawlerRunLock("verify-output"):
        writer = OutputWriter()
        manifests = sorted(writer.manifests_dir.glob("*.json"))
        failures = 0
        covered_shards = set()
        for manifest_path in manifests:
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            covered_shards.update(shard["path"] for shard in manifest.get("shards", []))
            errors = writer.verify_source(manifest["source_file"])
            if errors:
                failures += 1
                print(f"{manifest['source_file']}: {'; '.join(errors)}")
        all_shards = {
            path.relative_to(writer.output_dir).as_posix()
            for path in writer.output_dir.glob("*/*.jsonl.gz")
        }
        uncovered = sorted(all_shards - covered_shards)
        for relative in uncovered[:20]:
            print(f"Missing manifest coverage: {relative}")
        if len(uncovered) > 20:
            print(f"... and {len(uncovered) - 20} more uncovered shards")
        failures += len(uncovered)
        print(
            f"Verified {len(manifests)} source manifests and {len(all_shards)} shards; "
            f"{failures} integrity errors."
        )
        return 1 if failures else 0


def _runtime_settings(args) -> RuntimeSettings:
    profile: HardwareProfile = get_hardware_profile(args.profile)
    settings = RuntimeSettings(
        profile_name=profile.name,
        workers=args.workers or profile.workers,
        candidate_batch_size=args.candidate_batch_size or profile.candidate_batch_size,
        inference_batch_size=args.inference_batch_size or profile.inference_batch_size,
        encoding_batch_size=args.encoding_batch_size or profile.encoding_batch_size,
        semantic_threshold=args.threshold,
        language_threshold=args.language_threshold,
    )
    if settings.workers <= 0:
        raise ValueError("workers must be positive")
    if min(
        settings.candidate_batch_size,
        settings.inference_batch_size,
        settings.encoding_batch_size,
    ) <= 0:
        raise ValueError("batch sizes must be positive")
    if not 0 <= settings.semantic_threshold <= 1:
        raise ValueError("semantic threshold must be between 0 and 1")
    if not 0 <= settings.language_threshold <= 1:
        raise ValueError("language threshold must be between 0 and 1")
    return settings


def _evaluation_command(args) -> None:
    from evaluation import annotate, build_annotation_sample, evaluation_report

    if args.evaluation_command == "sample":
        print(json.dumps(build_annotation_sample(size=args.size), indent=2))
    elif args.evaluation_command == "annotate":
        annotate()
    elif args.evaluation_command == "report":
        print(json.dumps(evaluation_report(), indent=2))


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Extract personal home and belonging narratives from Common Crawl"
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    run_parser = subparsers.add_parser("run", help="start or resume processing")
    run_parser.add_argument("--crawl")
    run_parser.add_argument("--all", action="store_true")
    run_parser.add_argument("--limit", type=int)
    run_parser.add_argument("--threshold", type=float, default=SEMANTIC_THRESHOLD)
    run_parser.add_argument("--language-threshold", type=float, default=LANG_DETECTION_THRESHOLD)
    run_parser.add_argument("--profile", choices=["auto", *HARDWARE_PROFILES], default="auto")
    run_parser.add_argument("--workers", type=int)
    run_parser.add_argument(
        "--candidate-batch-size",
        "--stream-batch-size",
        dest="candidate_batch_size",
        type=int,
    )
    run_parser.add_argument("--inference-batch-size", type=int)
    run_parser.add_argument("--encoding-batch-size", type=int)

    subparsers.add_parser("status", help="show processing progress")
    subparsers.add_parser("metrics", help="show the latest operational metrics")
    subparsers.add_parser("list", help="list available crawls")
    subparsers.add_parser("reset", help="wipe output and progress")
    subparsers.add_parser("verify-output", help="verify source shard checksums")

    retry_parser = subparsers.add_parser("retry", help="retry failed files now")
    retry_parser.add_argument("--crawl")
    retry_parser.add_argument("--all", action="store_true")

    recover_parser = subparsers.add_parser(
        "recover", help="release processing leases older than a threshold"
    )
    recover_parser.add_argument("--minutes", type=int, default=LEASE_TIMEOUT_SECONDS // 60)

    doctor_parser = subparsers.add_parser("doctor", help="check the local runtime")
    doctor_parser.add_argument("--profile", choices=["auto", *HARDWARE_PROFILES], default="auto")

    benchmark_parser = subparsers.add_parser("benchmark", help="benchmark and tune this PC")
    benchmark_parser.add_argument("--profile", choices=["auto", *HARDWARE_PROFILES], default="auto")
    benchmark_parser.add_argument("--quick", action="store_true")
    benchmark_parser.add_argument("--no-write", action="store_true")

    parquet_parser = subparsers.add_parser("parquet", help="export partitioned Parquet")
    parquet_parser.add_argument("--dedupe", choices=["none", "exact", "near"], default="exact")
    parquet_parser.add_argument("--near-distance", type=int, default=3)

    evaluation_parser = subparsers.add_parser("evaluation", help="sample and evaluate filters")
    evaluation_subparsers = evaluation_parser.add_subparsers(
        dest="evaluation_command", required=True
    )
    sample_parser = evaluation_subparsers.add_parser("sample")
    sample_parser.add_argument("--size", type=int, default=400)
    evaluation_subparsers.add_parser("annotate")
    evaluation_subparsers.add_parser("report")

    args = parser.parse_args()
    if args.command == "run":
        if args.limit is not None and args.limit <= 0:
            parser.error("--limit must be positive")
        settings = _runtime_settings(args)
        crawl_ids = get_all_crawl_ids() if args.all else [args.crawl or DEFAULT_CRAWL_ID]
        run(crawl_ids, args.limit, settings)
    elif args.command == "status":
        show_status()
    elif args.command == "metrics":
        print_latest()
    elif args.command == "list":
        list_crawls()
    elif args.command == "retry":
        retry_failed(None if args.all else (args.crawl or DEFAULT_CRAWL_ID))
    elif args.command == "recover":
        if args.minutes < 0:
            parser.error("--minutes cannot be negative")
        recover_leases(args.minutes)
    elif args.command == "reset":
        reset_data()
    elif args.command == "doctor":
        raise SystemExit(doctor(args.profile))
    elif args.command == "verify-output":
        raise SystemExit(verify_output())
    elif args.command == "benchmark":
        from benchmark import run_benchmark

        with CrawlerRunLock("benchmark"):
            print(
                json.dumps(
                    run_benchmark(args.profile, quick=args.quick, write=not args.no_write),
                    indent=2,
                )
            )
    elif args.command == "parquet":
        from parquet_export import export_parquet

        with CrawlerRunLock("parquet"):
            print(
                json.dumps(
                    export_parquet(dedupe=args.dedupe, near_distance=args.near_distance),
                    indent=2,
                )
            )
    elif args.command == "evaluation":
        _evaluation_command(args)


if __name__ == "__main__":
    main()
