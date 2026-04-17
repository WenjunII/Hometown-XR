"""
JSONL output writer.

Writes matched paragraphs to gzip-compressed JSONL files,
organized by detected language in subdirectories.
"""

import gzip
import json
import logging
from pathlib import Path

from config import OUTPUT_DIR
from matcher import Match

logger = logging.getLogger(__name__)


class OutputWriter:
    """
    Writes Match objects to JSONL files organized by language.

    Output structure:
        data/output/<language_code>/<wet_filename>.jsonl.gz

    Files are appended to, not overwritten, for resume safety.
    """

    def __init__(self):
        self.output_dir = OUTPUT_DIR
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def _get_output_path(self, language: str, wet_path: str) -> Path:
        """Compute the output file path for a given language and WET source."""
        # Create language subdirectory
        lang_dir = self.output_dir / language
        lang_dir.mkdir(parents=True, exist_ok=True)

        # Derive filename from WET path
        # e.g. "crawl-data/.../CC-MAIN-...00000.warc.wet.gz" → safe filename
        wet_filename = wet_path.replace("/", "_").replace("\\", "_")
        if wet_filename.endswith(".gz"):
            wet_filename = wet_filename[:-3]
        wet_filename = wet_filename + ".jsonl.gz"

        return lang_dir / wet_filename

    def write_matches(
        self,
        matches: list[Match],
        languages: list[tuple[str, float]],
        wet_path: str,
    ) -> dict[str, int]:
        """
        Write matched paragraphs to JSONL files, grouped by language.

        Args:
            matches: List of Match objects from the matcher
            languages: List of (language_code, confidence) tuples,
                       one per match, from the language detector
            wet_path: Source WET file path (for output filename)

        Returns:
            Dict mapping language code → number of matches written
        """
        if not matches:
            return {}

        # Group matches by language
        by_language: dict[str, list[dict]] = {}
        for match, (lang, lang_conf) in zip(matches, languages):
            record = {
                "crawl_id": match.crawl_id,
                "url": match.url,
                "warc_date": match.warc_date,
                "language": lang,
                "language_confidence": round(lang_conf, 4),
                "paragraph": match.text,
                "matched_keywords": match.matched_keywords,
                "semantic_score": round(match.semantic_score, 4),
                "concept_match": match.concept_match,
            }

            if lang not in by_language:
                by_language[lang] = []
            by_language[lang].append(record)

        # Write each language group to its own file
        counts = {}
        for lang, records in by_language.items():
            output_path = self._get_output_path(lang, wet_path)

            # Append mode for resume safety
            with gzip.open(output_path, "at", encoding="utf-8") as f:
                for record in records:
                    f.write(json.dumps(record, ensure_ascii=False) + "\n")

            counts[lang] = len(records)
            logger.debug(f"Wrote {len(records)} matches to {output_path}")

        return counts
