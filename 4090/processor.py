"""
WET and ARC record parsing and paragraph extraction.

- WET files: Pre-extracted plain text (modern crawls, 2013+)
- ARC files: Raw HTML that needs text extraction (legacy crawls, 2008-2012)

Both formats are handled by warcio's ArchiveIterator.
"""

import html
import logging
import re
from dataclasses import dataclass

from warcio.archiveiterator import ArchiveIterator

from config import MIN_PARAGRAPH_LENGTH, MAX_PARAGRAPH_LENGTH

logger = logging.getLogger(__name__)


@dataclass
class Paragraph:
    """A single paragraph extracted from a WET or ARC record."""
    url: str
    warc_date: str
    text: str
    crawl_id: str = ""


# ── HTML to Text Extraction ──────────────────────────────────────────────────
# Lightweight HTML-to-text for ARC files (no external dependency needed).
# We strip tags, decode entities, and normalize whitespace.

# Tags whose content should be completely removed (not just the tags)
_REMOVE_CONTENT_TAGS = re.compile(
    r'<(script|style|noscript|iframe|svg|head)[^>]*>.*?</\1>',
    re.DOTALL | re.IGNORECASE,
)

# All remaining HTML tags
_STRIP_TAGS = re.compile(r'<[^>]+>')

# Multiple whitespace / blank lines
_MULTI_SPACE = re.compile(r'[ \t]+')
_MULTI_NEWLINE = re.compile(r'\n{3,}')


def _html_to_text(html_content: str) -> str:
    """
    Extract readable text from HTML content.

    This is a lightweight extractor — not as sophisticated as trafilatura
    or BeautifulSoup, but fast and dependency-free. For Common Crawl scale,
    speed matters more than perfect extraction.
    """
    text = html_content

    # Remove script, style, and other non-content tags entirely
    text = _REMOVE_CONTENT_TAGS.sub(' ', text)

    # Convert <br>, <p>, <div>, <li>, <h*> etc. to newlines for paragraph splitting
    text = re.sub(r'<br\s*/?>|</p>|</div>|</li>|</h[1-6]>|</tr>|</blockquote>',
                  '\n', text, flags=re.IGNORECASE)

    # Strip all remaining tags
    text = _STRIP_TAGS.sub(' ', text)

    # Decode HTML entities
    text = html.unescape(text)

    # Normalize whitespace
    text = _MULTI_SPACE.sub(' ', text)
    text = _MULTI_NEWLINE.sub('\n\n', text)

    return text.strip()


# ── Paragraph Extraction ─────────────────────────────────────────────────────

def extract_paragraphs_from_wet(
    stream, crawl_id: str = "", keyword_matcher=None, shutdown_event=None
) -> tuple[int, list[Paragraph]]:
    """
    Parse a WET file stream (modern crawls, 2013+).
    WET records contain pre-extracted plain text.
    
    Returns a generator yielding (Paragraph, matched_keywords).
    """
    records_processed = 0

    try:
        for record in ArchiveIterator(stream):
            if shutdown_event and shutdown_event.is_set():
                break

            if record.rec_type != "conversion":
                continue

            records_processed += 1
            url = record.rec_headers.get_header("WARC-Target-URI") or ""
            warc_date = record.rec_headers.get_header("WARC-Date") or ""

            try:
                content = record.content_stream().read().decode("utf-8", errors="ignore")
            except Exception as e:
                logger.warning(f"Failed to read content from {url}: {e}")
                continue

            if not content.strip():
                continue

            # Split into paragraphs and yield those that pass keyword filter
            for para, keywords in _extract_paras(content, url, warc_date, crawl_id, keyword_matcher, shutdown_event):
                yield para, keywords, records_processed

    except Exception as e:
        logger.error(f"Error processing WET stream: {e}")


def extract_paragraphs_from_arc(
    stream, crawl_id: str = "", keyword_matcher=None, shutdown_event=None
) -> tuple[int, list[Paragraph]]:
    """
    Parse an ARC file stream (legacy crawls, 2008-2012).
    ARC records contain raw HTML — text is extracted on the fly.
    
    Returns a generator yielding (Paragraph, matched_keywords, records_processed).
    """
    records_processed = 0

    try:
        for record in ArchiveIterator(stream, arc2warc=True):
            if shutdown_event and shutdown_event.is_set():
                break

            # ARC records converted to WARC appear as 'response' type
            if record.rec_type not in ("response", "resource"):
                continue

            # Only process HTML content
            content_type = ""
            if record.http_headers:
                content_type = record.http_headers.get_header("Content-Type") or ""
            if content_type and "html" not in content_type.lower():
                continue

            records_processed += 1
            url = record.rec_headers.get_header("WARC-Target-URI") or ""
            warc_date = record.rec_headers.get_header("WARC-Date") or ""

            try:
                raw_content = record.content_stream().read()
                # Try UTF-8 first, fall back to latin-1
                try:
                    html_content = raw_content.decode("utf-8", errors="ignore")
                except Exception:
                    html_content = raw_content.decode("latin-1", errors="ignore")
            except Exception as e:
                logger.warning(f"Failed to read ARC content from {url}: {e}")
                continue

            if not html_content.strip():
                continue

            # Extract text from HTML
            text_content = _html_to_text(html_content)

            if not text_content.strip():
                continue

            # Split into paragraphs and yield those that pass keyword filter
            for para, keywords in _extract_paras(text_content, url, warc_date, crawl_id, keyword_matcher, shutdown_event):
                yield para, keywords, records_processed

    except Exception as e:
        logger.error(f"Error processing ARC stream: {e}")


def _extract_paras(
    content: str, url: str, warc_date: str,
    crawl_id: str = "", keyword_matcher=None, shutdown_event=None,
):
    """
    Split text content into paragraphs and apply length filters.
    If keyword_matcher is provided, only yields paragraphs that contain keywords.
    """
    raw_paragraphs = content.split("\n\n")

    for raw_para in raw_paragraphs:
        if shutdown_event and shutdown_event.is_set():
            break

        text = " ".join(raw_para.split())

        if len(text) < MIN_PARAGRAPH_LENGTH:
            continue
        if len(text) > MAX_PARAGRAPH_LENGTH:
            continue

        # Stage 1 keyword filter (optional but recommended for streaming efficiency)
        kw_matches = []
        if keyword_matcher:
            kw_matches = keyword_matcher.find_matches(text)
            if not kw_matches:
                continue

        yield Paragraph(
            url=url,
            warc_date=warc_date,
            text=text,
            crawl_id=crawl_id,
        ), kw_matches
