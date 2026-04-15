"""
Catalog of all Common Crawl datasets from 2008 to present.

Two eras of data:
  - Legacy (2008-2012): ARC format, raw HTML, no WET files
  - Modern (2013-present): WARC/WAT/WET format, pre-extracted text

The legacy data is stored under different S3 paths and requires
HTML-to-text extraction since there are no WET files.
"""

from dataclasses import dataclass


@dataclass
class CrawlInfo:
    """Metadata about a single crawl."""
    crawl_id: str
    era: str          # "legacy" or "modern"
    format: str       # "arc" or "wet"
    base_url: str     # Base URL for accessing files
    paths_file: str   # Path to the file listing (or empty for legacy)
    notes: str = ""


# ── Modern Crawls (2013–2026): WARC/WET format ──────────────────────────────
# These have WET files with pre-extracted text.
# Listed in reverse chronological order.
# Source: https://commoncrawl.org/overview (pages 1-3)
MODERN_CRAWLS = [
    "CC-MAIN-2026-12", "CC-MAIN-2026-08", "CC-MAIN-2026-04",
    "CC-MAIN-2025-51", "CC-MAIN-2025-47", "CC-MAIN-2025-43",
    "CC-MAIN-2025-38", "CC-MAIN-2025-33", "CC-MAIN-2025-30",
    "CC-MAIN-2025-26", "CC-MAIN-2025-21", "CC-MAIN-2025-18",
    "CC-MAIN-2025-13", "CC-MAIN-2025-08", "CC-MAIN-2025-05",
    "CC-MAIN-2024-51", "CC-MAIN-2024-46", "CC-MAIN-2024-42",
    "CC-MAIN-2024-38", "CC-MAIN-2024-33", "CC-MAIN-2024-30",
    "CC-MAIN-2024-26", "CC-MAIN-2024-22", "CC-MAIN-2024-18",
    "CC-MAIN-2024-10",
    "CC-MAIN-2023-50", "CC-MAIN-2023-40", "CC-MAIN-2023-23",
    "CC-MAIN-2023-14", "CC-MAIN-2023-06",
    "CC-MAIN-2022-49", "CC-MAIN-2022-40", "CC-MAIN-2022-33",
    "CC-MAIN-2022-27", "CC-MAIN-2022-21", "CC-MAIN-2022-05",
    "CC-MAIN-2021-49", "CC-MAIN-2021-43", "CC-MAIN-2021-39",
    "CC-MAIN-2021-31", "CC-MAIN-2021-25", "CC-MAIN-2021-21",
    "CC-MAIN-2021-17", "CC-MAIN-2021-10", "CC-MAIN-2021-04",
    "CC-MAIN-2020-50", "CC-MAIN-2020-45", "CC-MAIN-2020-40",
    "CC-MAIN-2020-34", "CC-MAIN-2020-29", "CC-MAIN-2020-24",
    "CC-MAIN-2020-16", "CC-MAIN-2020-10", "CC-MAIN-2020-05",
    "CC-MAIN-2019-51", "CC-MAIN-2019-47", "CC-MAIN-2019-43",
    "CC-MAIN-2019-39", "CC-MAIN-2019-35", "CC-MAIN-2019-30",
    "CC-MAIN-2019-26", "CC-MAIN-2019-22", "CC-MAIN-2019-18",
    "CC-MAIN-2019-13", "CC-MAIN-2019-09", "CC-MAIN-2019-04",
    "CC-MAIN-2018-51", "CC-MAIN-2018-47", "CC-MAIN-2018-43",
    "CC-MAIN-2018-39", "CC-MAIN-2018-34", "CC-MAIN-2018-30",
    "CC-MAIN-2018-26", "CC-MAIN-2018-22", "CC-MAIN-2018-17",
    "CC-MAIN-2018-13", "CC-MAIN-2018-09", "CC-MAIN-2018-05",
    "CC-MAIN-2017-51", "CC-MAIN-2017-47", "CC-MAIN-2017-43",
    "CC-MAIN-2017-39", "CC-MAIN-2017-34", "CC-MAIN-2017-30",
    "CC-MAIN-2017-26", "CC-MAIN-2017-22", "CC-MAIN-2017-17",
    "CC-MAIN-2017-13", "CC-MAIN-2017-09", "CC-MAIN-2017-04",
    "CC-MAIN-2016-50", "CC-MAIN-2016-44", "CC-MAIN-2016-40",
    "CC-MAIN-2016-36", "CC-MAIN-2016-30", "CC-MAIN-2016-26",
    "CC-MAIN-2016-22", "CC-MAIN-2016-18", "CC-MAIN-2016-07",
    "CC-MAIN-2015-48", "CC-MAIN-2015-40", "CC-MAIN-2015-35",
    "CC-MAIN-2015-32", "CC-MAIN-2015-27", "CC-MAIN-2015-22",
    "CC-MAIN-2015-18", "CC-MAIN-2015-14", "CC-MAIN-2015-11",
    "CC-MAIN-2015-06",
    "CC-MAIN-2014-52", "CC-MAIN-2014-49", "CC-MAIN-2014-42",
    "CC-MAIN-2014-41", "CC-MAIN-2014-35", "CC-MAIN-2014-23",
    "CC-MAIN-2014-15", "CC-MAIN-2014-10",
    "CC-MAIN-2013-48", "CC-MAIN-2013-20",
]

# ── Legacy Crawls (2008–2012): ARC format ────────────────────────────────────
# These contain raw HTML — text must be extracted from HTML content.
# Stored under different S3 paths than modern crawls.
LEGACY_CRAWLS = [
    CrawlInfo(
        crawl_id="CC-CRAWL-001",
        era="legacy",
        format="arc",
        base_url="https://data.commoncrawl.org/",
        paths_file="crawl-001/",
        notes="2008-2010 crawl, Nutch-based ARC format",
    ),
    CrawlInfo(
        crawl_id="CC-CRAWL-002",
        era="legacy",
        format="arc",
        base_url="https://data.commoncrawl.org/",
        paths_file="crawl-002/",
        notes="2009-2010 crawl, Nutch-based ARC format",
    ),
    CrawlInfo(
        crawl_id="CC-2012",
        era="legacy",
        format="arc",
        base_url="https://data.commoncrawl.org/",
        paths_file="parse-output/",
        notes="2012 crawl, commoncrawl-crawler ARC format",
    ),
]


def get_crawl_info(crawl_id: str) -> CrawlInfo:
    """Get metadata for a crawl by its ID."""
    # Check legacy crawls
    for crawl in LEGACY_CRAWLS:
        if crawl.crawl_id == crawl_id:
            return crawl

    # Check modern crawls
    if crawl_id in MODERN_CRAWLS:
        return CrawlInfo(
            crawl_id=crawl_id,
            era="modern",
            format="wet",
            base_url="https://data.commoncrawl.org/",
            paths_file=f"crawl-data/{crawl_id}/wet.paths.gz",
        )

    raise ValueError(
        f"Unknown crawl ID: {crawl_id}. "
        f"Use 'python main.py list' to see all available crawls."
    )


def get_all_crawl_ids() -> list[str]:
    """Get all crawl IDs in chronological order (oldest first)."""
    legacy_ids = [c.crawl_id for c in LEGACY_CRAWLS]
    modern_ids = list(reversed(MODERN_CRAWLS))  # Reverse to get oldest first
    return legacy_ids + modern_ids


def is_legacy_crawl(crawl_id: str) -> bool:
    """Check if a crawl uses the legacy ARC format."""
    return any(c.crawl_id == crawl_id for c in LEGACY_CRAWLS)
