"""
WET/ARC file list fetching and HTTP streaming.

Handles:
- Modern crawls (2013+): Downloads wet.paths.gz for file listing
- Legacy crawls (2008-2012): Lists ARC files from S3 directory
- Streaming individual files via HTTP
"""

import gzip
import logging
import re
import xml.etree.ElementTree as ET

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from config import CC_BASE_URL, HTTP_TIMEOUT, HTTP_RETRIES, HTTP_BACKOFF_FACTOR
from crawl_catalog import CrawlInfo, is_legacy_crawl

logger = logging.getLogger(__name__)


def _make_session() -> requests.Session:
    """Create a requests Session with retry logic."""
    session = requests.Session()
    retry = Retry(
        total=HTTP_RETRIES,
        backoff_factor=HTTP_BACKOFF_FACTOR,
        status_forcelist=[500, 502, 503, 504],
        allowed_methods=["GET", "HEAD"],
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


_session = _make_session()


def fetch_file_paths(crawl_info: CrawlInfo) -> list[str]:
    """
    Get the list of data file paths for a crawl.

    For modern crawls: downloads wet.paths.gz
    For legacy crawls: lists ARC files from S3
    """
    if crawl_info.era == "modern":
        return _fetch_wet_paths(crawl_info)
    else:
        return _fetch_arc_paths(crawl_info)


def _fetch_wet_paths(crawl_info: CrawlInfo) -> list[str]:
    """Download and parse the wet.paths.gz file for a modern crawl."""
    url = f"{crawl_info.base_url}{crawl_info.paths_file}"
    logger.info(f"Fetching WET file list from {url}")

    response = _session.get(url, timeout=HTTP_TIMEOUT)
    response.raise_for_status()

    decompressed = gzip.decompress(response.content)
    paths = decompressed.decode("utf-8").strip().split("\n")
    paths = [p.strip() for p in paths if p.strip()]

    logger.info(f"Found {len(paths)} WET files for {crawl_info.crawl_id}")
    return paths


def _fetch_arc_paths(crawl_info: CrawlInfo) -> list[str]:
    """
    List ARC files from a legacy crawl's S3 directory.

    Legacy crawls store data under paths like:
      s3://commoncrawl/crawl-001/...
      s3://commoncrawl/crawl-002/...
      s3://commoncrawl/parse-output/...

    We use the S3 XML listing API to enumerate files.
    """
    base_prefix = crawl_info.paths_file
    logger.info(
        f"Listing ARC files for legacy crawl {crawl_info.crawl_id} "
        f"(prefix: {base_prefix})"
    )

    arc_paths = []
    s3_base = "https://commoncrawl.s3.amazonaws.com/"
    marker = ""

    while True:
        params = {
            "prefix": base_prefix,
            "max-keys": "1000",
        }
        if marker:
            params["marker"] = marker

        try:
            response = _session.get(s3_base, params=params, timeout=HTTP_TIMEOUT)
            response.raise_for_status()
        except Exception as e:
            logger.warning(f"Failed to list S3 directory: {e}")
            break

        # Parse XML listing
        try:
            root = ET.fromstring(response.text)
            # Handle S3's XML namespace
            ns = ""
            if root.tag.startswith("{"):
                ns = root.tag.split("}")[0] + "}"

            keys = root.findall(f".//{ns}Key")
            if not keys:
                break

            for key_elem in keys:
                key = key_elem.text
                if key and (key.endswith(".arc.gz") or key.endswith(".arc")):
                    arc_paths.append(key)

            # Check if there are more results
            is_truncated = root.find(f"{ns}IsTruncated")
            if is_truncated is not None and is_truncated.text == "true":
                marker = keys[-1].text
            else:
                break

        except ET.ParseError as e:
            logger.error(f"Failed to parse S3 listing XML: {e}")
            break

    logger.info(f"Found {len(arc_paths)} ARC files for {crawl_info.crawl_id}")
    return arc_paths


def stream_file(file_path: str, crawl_info: CrawlInfo):
    """
    Open an HTTP stream to a WET or ARC file.

    Args:
        file_path: Relative path to the file
        crawl_info: Crawl metadata

    Returns:
        A file-like stream object
    """
    # Modern crawls use data.commoncrawl.org
    # Legacy crawls also accessible via data.commoncrawl.org
    url = f"{CC_BASE_URL}{file_path}"
    logger.debug(f"Streaming file: {url}")

    response = _session.get(url, timeout=HTTP_TIMEOUT, stream=True)
    response.raise_for_status()

    response.raw.decode_content = True
    return response.raw
