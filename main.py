"""
Common Crawl Home/Belonging Extractor - CLI Entry Point

A resumable application that streams Common Crawl WET/ARC files, detects
language, and extracts paragraphs semantically related to concepts of "home",
"hometown", "belonging", "roots", "childhood", etc. - across all languages.

Supports all Common Crawl datasets from 2008 to present:
  - Modern crawls (2013+): WET format (pre-extracted text)
  - Legacy crawls (2008-2012): ARC format (HTML -> text extraction)

Usage:
    python main.py run --crawl CC-MAIN-2026-12           # Process one crawl
    python main.py run --crawl CC-MAIN-2026-12 --limit 5 # Test with 5 files
    python main.py run --all                              # Process ALL crawls
    python main.py status                                 # Show progress
    python main.py list                                   # List all crawls
"""

import argparse
import logging
import signal
import sys
import time

from config import DEFAULT_CRAWL_ID, SEMANTIC_THRESHOLD
from crawl_catalog import get_crawl_info, get_all_crawl_ids, is_legacy_crawl, get_modern_crawls, LEGACY_CRAWLS
from downloader import fetch_file_paths, stream_file
from processor import extract_paragraphs_from_wet, extract_paragraphs_from_arc
from matcher import HybridMatcher
from language_detector import LanguageDetector
from progress import ProgressTracker
from output import OutputWriter

# -- Logging Setup ------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-5s | %(name)-20s | %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("main")

# -- Graceful Shutdown --------------------------------------------------------
_shutdown_requested = False


def _signal_handler(signum, frame):
    global _shutdown_requested
    if _shutdown_requested:
        logger.warning("Force quit! Exiting immediately.")
        sys.exit(1)
    _shutdown_requested = True
    logger.info("Shutdown requested. Finishing current file, then stopping...")


signal.signal(signal.SIGINT, _signal_handler)


# -- Process a Single Crawl ---------------------------------------------------

def process_crawl(
    crawl_id: str,
    limit: int | None,
    threshold: float,
    matcher: HybridMatcher,
    lang_detector: LanguageDetector,
) -> tuple[int, int]:
    """
    Process a single crawl. Returns (files_processed, matches_found).
    """
    global _shutdown_requested

    crawl_info = get_crawl_info(crawl_id)
    is_legacy = crawl_info.era == "legacy"
    format_label = "ARC (HTML)" if is_legacy else "WET (text)"

    logger.info(f"--- Crawl: {crawl_id} [{format_label}] ---")

    tracker = ProgressTracker()
    writer = OutputWriter()

    # Fetch file list
    logger.info("Fetching file list...")
    file_paths = fetch_file_paths(crawl_info)

    if not file_paths:
        logger.warning(f"No files found for {crawl_id}. Skipping.")
        return 0, 0

    # Initialize progress tracking (scoped to this crawl)
    tracker.initialize_paths(file_paths, crawl_id)
    summary = tracker.get_summary(crawl_id)
    logger.info(
        f"Progress: {summary['completed']}/{summary['total_files']} completed, "
        f"{summary['total_matches']} matches so far"
    )

    # Processing loop
    files_processed = 0
    matches_found = 0

    while True:
        if _shutdown_requested:
            logger.info("Shutdown requested. Stopping.")
            break

        if limit and files_processed >= limit:
            logger.info(f"Reached file limit ({limit}). Stopping.")
            break

        file_path = tracker.get_next_pending(crawl_id)
        if file_path is None:
            logger.info(f"All files for {crawl_id} processed!")
            break

        tracker.mark_processing(file_path)
        file_start_time = time.time()
        short_name = file_path.split("/")[-1]

        try:
            logger.info(f"Processing: {short_name}")

            # Stream and parse the file
            stream = stream_file(file_path, crawl_info)

            if is_legacy:
                records_processed, paragraphs = extract_paragraphs_from_arc(stream)
            else:
                records_processed, paragraphs = extract_paragraphs_from_wet(stream)

            logger.info(
                f"   Parsed {records_processed} records -> "
                f"{len(paragraphs)} paragraphs"
            )

            if not paragraphs:
                tracker.mark_completed(file_path, records_processed, 0)
                files_processed += 1
                continue

            # Run the two-stage matcher
            matches = matcher.process_paragraphs(paragraphs)

            if matches:
                languages = [lang_detector.detect(m.text) for m in matches]
                lang_counts = writer.write_matches(matches, languages, file_path)

                lang_summary = ", ".join(
                    f"{lang}: {count}" for lang, count in
                    sorted(lang_counts.items(), key=lambda x: -x[1])[:5]
                )
                logger.info(f"   {len(matches)} matches [{lang_summary}]")
            else:
                logger.info(f"   No matches found")

            elapsed = time.time() - file_start_time
            tracker.mark_completed(file_path, records_processed, len(matches))
            files_processed += 1
            matches_found += len(matches)

            logger.info(f"   Done in {elapsed:.1f}s")

        except Exception as e:
            logger.error(f"   Failed: {e}")
            tracker.mark_failed(file_path, str(e))
            files_processed += 1

    return files_processed, matches_found


# -- Main Commands ------------------------------------------------------------

def run(crawl_ids: list[str], limit: int | None, threshold: float):
    """Main processing loop for one or more crawls."""
    global _shutdown_requested

    logger.info("=" * 70)
    logger.info("  Common Crawl Home/Belonging Extractor")
    logger.info(f"  Crawls to process: {len(crawl_ids)}")
    logger.info(f"  Semantic threshold: {threshold}")
    if limit:
        logger.info(f"  File limit per crawl: {limit}")
    logger.info("=" * 70)

    # Load ML models once (shared across all crawls)
    logger.info("Loading ML models (first run downloads ~600 MB)...")
    matcher = HybridMatcher(threshold=threshold)
    lang_detector = LanguageDetector()
    logger.info("All models loaded. Starting processing.\n")

    total_files = 0
    total_matches = 0

    for i, crawl_id in enumerate(crawl_ids):
        if _shutdown_requested:
            break

        logger.info(f"\n[{i+1}/{len(crawl_ids)}] Starting crawl: {crawl_id}")
        files, matches = process_crawl(
            crawl_id, limit, threshold, matcher, lang_detector
        )
        total_files += files
        total_matches += matches

    # Final summary
    logger.info("")
    logger.info("=" * 70)
    logger.info(f"  Session Summary")
    logger.info(f"  Crawls attempted:       {min(i+1, len(crawl_ids)) if crawl_ids else 0}")
    logger.info(f"  Files processed:        {total_files}")
    logger.info(f"  Matches found:          {total_matches}")
    logger.info("=" * 70)


def show_status():
    """Show current processing status across all crawls."""
    tracker = ProgressTracker()
    summary = tracker.get_summary()

    print("\n" + "=" * 60)
    print("  CC Home/Belonging Extractor - Overall Status")
    print("=" * 60)
    print(f"  Total files:       {summary['total_files']}")
    print(f"  Completed:         {summary['completed']}")
    print(f"  Pending:           {summary['pending']}")
    print(f"  Failed:            {summary['failed']}")
    print(f"  Progress:          {summary['progress_pct']:.2f}%")
    print(f"  ----------------------")
    print(f"  Records processed: {summary['total_records']:,}")
    print(f"  Matches found:     {summary['total_matches']:,}")
    print("=" * 60)

    # Per-crawl breakdown
    crawl_summaries = tracker.get_per_crawl_summary()
    if crawl_summaries:
        print("\n  Per-Crawl Breakdown:")
        print(f"  {'Crawl ID':<25} {'Done':>8} {'Total':>8} {'Matches':>10} {'Status':>10}")
        print(f"  {'-'*25} {'-'*8} {'-'*8} {'-'*10} {'-'*10}")
        for cs in crawl_summaries:
            status = "DONE" if cs['completed'] == cs['total'] else "IN PROGRESS"
            print(
                f"  {cs['crawl_id']:<25} "
                f"{cs['completed']:>8} "
                f"{cs['total']:>8} "
                f"{cs['matches']:>10} "
                f"{status:>10}"
            )
    print()


def list_crawls():
    """List all available crawls."""
    print("\n" + "=" * 60)
    print("  Available Common Crawl Datasets")
    print("=" * 60)

    print("\n  LEGACY CRAWLS (2008-2012) - ARC format (HTML)")
    print(f"  {'-'*55}")
    for crawl in LEGACY_CRAWLS:
        print(f"  {crawl.crawl_id:<25} {crawl.notes}")

    modern_crawls = get_modern_crawls()
    print(f"\n  MODERN CRAWLS (2013-present) - WET format (text)")
    print(f"  (auto-discovered from Common Crawl index API)")
    print(f"  {'-'*55}")
    # Group by year
    current_year = None
    for crawl_id in reversed(modern_crawls):
        year = crawl_id.split("-")[2]
        if year != current_year:
            current_year = year
            print(f"\n  {year}:")
        print(f"    {crawl_id}")

    total = len(LEGACY_CRAWLS) + len(modern_crawls)
    print(f"\n  Total: {total} crawls available")
    print(f"  New crawls are auto-discovered when published by Common Crawl.")
    print(f"  Use: python main.py run --crawl <ID>")
    print(f"  Or:  python main.py run --all\n")


# -- CLI Argument Parsing -----------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Extract home/belonging paragraphs from Common Crawl datasets (2008-present)"
    )
    subparsers = parser.add_subparsers(dest="command", help="Command to run")

    # Run command
    run_parser = subparsers.add_parser("run", help="Start or resume processing")
    run_parser.add_argument(
        "--crawl",
        default=None,
        help=f"Crawl ID to process (default: {DEFAULT_CRAWL_ID})",
    )
    run_parser.add_argument(
        "--all",
        action="store_true",
        help="Process ALL crawls from 2008 to present",
    )
    run_parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Maximum number of files to process per crawl (for testing)",
    )
    run_parser.add_argument(
        "--threshold",
        type=float,
        default=SEMANTIC_THRESHOLD,
        help=f"Semantic similarity threshold (default: {SEMANTIC_THRESHOLD})",
    )

    # Status command
    subparsers.add_parser("status", help="Show processing progress")

    # List command
    subparsers.add_parser("list", help="List all available crawls")

    args = parser.parse_args()

    if args.command == "run":
        if args.all:
            crawl_ids = get_all_crawl_ids()
        elif args.crawl:
            crawl_ids = [args.crawl]
        else:
            crawl_ids = [DEFAULT_CRAWL_ID]
        run(crawl_ids, args.limit, args.threshold)
    elif args.command == "status":
        show_status()
    elif args.command == "list":
        list_crawls()
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
