"""
Three-stage matching engine:
  Stage 1: keyword pre-filter → fast elimination of irrelevant paragraphs
  Stage 2: semantic similarity scoring → cosine similarity against concept anchors
  Stage 3: narrative voice filter → keeps only first-person personal narratives

Stage 1 (KeywordMatcher): Fast substring search using the multilingual keyword
dictionary. Eliminates ~99% of irrelevant paragraphs.

Stage 2 (SemanticMatcher): Embeds candidate paragraphs with a multilingual
sentence-transformer and scores them against pre-computed concept anchors
via cosine similarity.

Stage 3 (NarrativeFilter): Checks for first-person pronouns and narrative
indicators across 18+ languages. Eliminates dictionary definitions,
genealogy databases, commercial pages, and other non-personal text.
"""

import logging
import re
from dataclasses import dataclass

import numpy as np
from sentence_transformers import SentenceTransformer, util

from config import (
    SEMANTIC_MODEL_NAME,
    SEMANTIC_THRESHOLD,
    ENCODING_BATCH_SIZE,
    DEVICE,
    NARRATIVE_FILTER_ENABLED,
    MIN_NARRATIVE_INDICATORS,
)
from concepts import CONCEPT_ANCHORS
from keywords import get_all_keywords_flat
from processor import Paragraph

logger = logging.getLogger(__name__)

# Short keywords (≤4 chars) need word-boundary matching to avoid
# substring false positives (e.g. "hem" matching inside "them").
_SHORT_KW_THRESHOLD = 4


@dataclass
class Match:
    """A paragraph that passed all matching stages."""
    url: str
    warc_date: str
    text: str
    matched_keywords: list[str]
    semantic_score: float
    concept_match: str
    crawl_id: str = ""


class KeywordMatcher:
    """
    Fast keyword pre-filter.

    Scans paragraph text for any keyword from the flat multilingual dictionary.
    Case-insensitive matching. Short keywords (≤4 chars) use word-boundary
    matching to avoid substring false positives.
    """

    def __init__(self):
        all_kw = get_all_keywords_flat()

        # Split into short (regex) and long (substring) keywords
        self.long_keywords = []
        self.short_patterns = []

        for kw in all_kw:
            if len(kw) <= _SHORT_KW_THRESHOLD:
                # Use word boundary regex for short keywords
                # \b works for ASCII word boundaries; for CJK characters,
                # the keyword itself acts as a natural boundary
                try:
                    pattern = re.compile(r'\b' + re.escape(kw) + r'\b', re.IGNORECASE)
                    self.short_patterns.append((kw, pattern))
                except re.error:
                    # Fallback to substring for regex-unfriendly patterns
                    self.long_keywords.append(kw)
            else:
                self.long_keywords.append(kw)

        logger.info(
            f"KeywordMatcher loaded {len(all_kw)} unique keywords "
            f"({len(self.long_keywords)} substring, {len(self.short_patterns)} word-boundary)"
        )

    def find_matches(self, text: str) -> list[str]:
        """
        Find all keywords present in the text.

        Returns:
            List of matched keywords (empty if none found)
        """
        text_lower = text.lower()
        found = []

        # Fast substring search for longer keywords
        for kw in self.long_keywords:
            if kw in text_lower:
                found.append(kw)

        # Regex word-boundary search for short keywords
        for kw, pattern in self.short_patterns:
            if pattern.search(text):
                found.append(kw)

        return found


class SemanticMatcher:
    """
    Semantic similarity scorer using a multilingual sentence-transformer.

    Encodes candidate paragraphs and compares them to pre-computed
    concept anchor embeddings via cosine similarity.
    """

    def __init__(self):
        logger.info(f"Loading semantic model: {SEMANTIC_MODEL_NAME} on {DEVICE}")
        self.model = SentenceTransformer(SEMANTIC_MODEL_NAME, device=DEVICE)
        logger.info("Encoding concept anchors...")
        self.anchor_embeddings = self.model.encode(
            CONCEPT_ANCHORS,
            convert_to_tensor=True,
            show_progress_bar=False,
            device=DEVICE,
        )
        logger.info(f"Encoded {len(CONCEPT_ANCHORS)} concept anchors")

    def score_paragraphs(
        self, paragraphs: list[str]
    ) -> list[tuple[float, str]]:
        """
        Score a batch of paragraphs against concept anchors.

        Args:
            paragraphs: List of paragraph texts to score

        Returns:
            List of (max_score, best_matching_concept) tuples
        """
        if not paragraphs:
            return []

        # Encode all paragraphs in a batch
        para_embeddings = self.model.encode(
            paragraphs,
            batch_size=ENCODING_BATCH_SIZE,
            convert_to_tensor=True,
            show_progress_bar=False,
            device=DEVICE,
        )

        # Compute cosine similarity against all concept anchors
        # Shape: (num_paragraphs, num_anchors)
        cos_scores = util.cos_sim(para_embeddings, self.anchor_embeddings)

        results = []
        for i in range(len(paragraphs)):
            scores = cos_scores[i].cpu().numpy()
            max_idx = int(np.argmax(scores))
            max_score = float(scores[max_idx])
            best_concept = CONCEPT_ANCHORS[max_idx]
            results.append((max_score, best_concept))

        return results


class NarrativeFilter:
    """
    Stage 3: Narrative voice filter.

    Checks for first-person pronouns and narrative indicators across
    multiple languages. Filters out non-personal text like dictionary
    definitions, database records, marketing copy, and structured data.
    """

    # First-person pronouns and possessives across languages
    # These use word-boundary matching to avoid false positives
    _FIRST_PERSON_PATTERNS = [
        # English
        r"\bI\b", r"\bmy\b", r"\bme\b", r"\bmyself\b", r"\bmine\b",
        r"\bwe\b", r"\bour\b", r"\bourselves\b",
        # Spanish
        r"\byo\b", r"\bmi\b", r"\bmis\b", r"\bnosotros\b", r"\bnuestro\b",
        # French
        r"\bje\b", r"\bmon\b", r"\bma\b", r"\bmes\b", r"\bnous\b", r"\bnotre\b",
        # German
        r"\bich\b", r"\bmein\b", r"\bmeine\b", r"\bwir\b", r"\bunser\b",
        # Portuguese
        r"\beu\b", r"\bmeu\b", r"\bminha\b", r"\bnós\b", r"\bnosso\b",
        # Italian
        r"\bio\b", r"\bmio\b", r"\bmia\b", r"\bnoi\b", r"\bnostro\b",
        # Russian (no word boundaries for Cyrillic — use regex-free check below)
        # Turkish
        r"\bben\b", r"\bbenim\b", r"\bbiz\b", r"\bbizim\b",
        # Dutch
        r"\bik\b", r"\bmijn\b", r"\bwij\b", r"\bonze\b",
        # Polish
        r"\bja\b", r"\bmój\b", r"\bmoja\b", r"\bmy\b", r"\bnasz\b",
        # Swedish
        r"\bjag\b", r"\bmin\b", r"\bmitt\b", r"\bvi\b", r"\bvår\b",
        # Vietnamese
        r"\btôi\b", r"\bcủa tôi\b", r"\bchúng tôi\b",
        # Indonesian/Malay
        r"\bsaya\b", r"\bkami\b", r"\bkita\b",
    ]

    # Non-Latin script first-person markers (substring matching)
    _FIRST_PERSON_SUBSTRINGS = [
        # Chinese
        "我", "我的", "我们",
        # Japanese
        "私", "僕", "俺", "わたし", "ぼく",
        # Korean
        "나는", "내", "우리", "저는", "저의",
        # Arabic
        "أنا", "لي", "نحن",
        # Hindi
        "मैं", "मेरा", "मेरी", "हम", "हमारा",
        # Thai
        "ฉัน", "ผม", "ดิฉัน", "เรา",
        # Russian / Ukrainian
        "я ", " мой", " моя", " мое", " мои", " наш",
        "я ", " мій", " моя", " моє", " мої", " наш",
    ]

    # Narrative indicator phrases — strong signals of personal storytelling
    _NARRATIVE_PHRASES = [
        # English
        "I remember", "I recall", "I grew up", "when I was",
        "my mother", "my father", "my parents", "my family",
        "my grandmother", "my grandfather", "my grandparents",
        "I was born", "I moved", "I left", "I came",
        "I miss", "I feel", "I realized", "I discovered",
        "back home", "my childhood", "my hometown",
        "I always", "I used to", "I never forgot",
        # Chinese
        "我记得", "我从小", "小时候", "我的家",
        "我长大", "我出生", "我思念", "我怀念",
        # Spanish
        "recuerdo", "crecí en", "cuando era", "mi madre",
        "mi padre", "mi familia", "nací en",
        # French
        "je me souviens", "j'ai grandi", "quand j'étais",
        "ma mère", "mon père", "ma famille",
        # German
        "ich erinnere", "ich bin aufgewachsen", "als ich",
        "meine Mutter", "mein Vater", "meine Familie",
        # Japanese
        "覚えている", "育った", "生まれた", "子供の頃",
        # Korean
        "기억해", "자랐", "태어났", "어렸을 때",
        # Arabic
        "أتذكر", "نشأت", "ولدت في", "عندما كنت",
    ]

    def __init__(self):
        # Pre-compile regex patterns for speed
        self._pronoun_patterns = []
        for pat in self._FIRST_PERSON_PATTERNS:
            try:
                self._pronoun_patterns.append(
                    re.compile(pat, re.IGNORECASE)
                )
            except re.error:
                pass

        # Pre-lowercase narrative phrases for fast lookup
        self._narrative_phrases_lower = [p.lower() for p in self._NARRATIVE_PHRASES]

        logger.info(
            f"NarrativeFilter loaded: {len(self._pronoun_patterns)} pronoun patterns, "
            f"{len(self._FIRST_PERSON_SUBSTRINGS)} substring markers, "
            f"{len(self._narrative_phrases_lower)} narrative phrases"
        )

    def count_indicators(self, text: str) -> int:
        """
        Count narrative voice indicators in a paragraph.

        Returns the total number of first-person pronouns, possessives,
        and narrative phrases found.
        """
        count = 0

        # Check regex-based pronoun patterns (Latin scripts)
        for pattern in self._pronoun_patterns:
            if pattern.search(text):
                count += 1

        # Check substring markers (CJK, Arabic, Cyrillic, Thai)
        text_for_sub = text  # keep original case for non-Latin
        for marker in self._FIRST_PERSON_SUBSTRINGS:
            if marker in text_for_sub:
                count += 1

        # Check narrative phrases (strong signal, worth more)
        text_lower = text.lower()
        for phrase in self._narrative_phrases_lower:
            if phrase in text_lower:
                count += 2  # narrative phrases are weighted higher

        return count

    def passes(self, text: str, min_indicators: int) -> bool:
        """Check if a paragraph has enough narrative voice indicators."""
        return self.count_indicators(text) >= min_indicators


class HybridMatcher:
    """
    Orchestrates the three-stage matching pipeline.

    1. Keyword pre-filter (fast)
    2. Semantic similarity scoring (accurate)
    3. Narrative voice filter (personal stories only)
    """

    def __init__(self, threshold: float = SEMANTIC_THRESHOLD):
        self.threshold = threshold
        self.keyword_matcher = KeywordMatcher()
        self.semantic_matcher = SemanticMatcher()

        if NARRATIVE_FILTER_ENABLED:
            self.narrative_filter = NarrativeFilter()
        else:
            self.narrative_filter = None

        logger.info(
            f"HybridMatcher ready (threshold={self.threshold}, "
            f"narrative_filter={'ON' if self.narrative_filter else 'OFF'})"
        )

    def process_paragraphs(self, paragraphs: list[Paragraph]) -> list[Match]:
        """
        Run all matching stages on a list of paragraphs.

        Args:
            paragraphs: List of Paragraph objects from the processor

        Returns:
            List of Match objects that passed all stages
        """
        # Stage 1: Keyword pre-filter
        candidates = []
        candidate_keywords = []

        for para in paragraphs:
            kw_matches = self.keyword_matcher.find_matches(para.text)
            if kw_matches:
                candidates.append(para)
                candidate_keywords.append(kw_matches)

        if not candidates:
            return []

        logger.debug(
            f"Stage 1: {len(candidates)}/{len(paragraphs)} paragraphs "
            f"passed keyword filter ({len(candidates)/max(len(paragraphs),1)*100:.1f}%)"
        )

        # Stage 2: Semantic similarity scoring
        candidate_texts = [c.text for c in candidates]
        scores = self.semantic_matcher.score_paragraphs(candidate_texts)

        # Filter by threshold
        semantic_matches = []
        for i, (score, concept) in enumerate(scores):
            if score >= self.threshold:
                semantic_matches.append((i, score, concept))

        logger.debug(
            f"Stage 2: {len(semantic_matches)}/{len(candidates)} candidates "
            f"passed semantic threshold ({self.threshold})"
        )

        if not semantic_matches:
            return []

        # Stage 3: Narrative voice filter
        if self.narrative_filter:
            matches = []
            narrative_passed = 0
            for i, score, concept in semantic_matches:
                if self.narrative_filter.passes(candidates[i].text, MIN_NARRATIVE_INDICATORS):
                    narrative_passed += 1
                    matches.append(Match(
                        url=candidates[i].url,
                        warc_date=candidates[i].warc_date,
                        text=candidates[i].text,
                        matched_keywords=candidate_keywords[i],
                        semantic_score=score,
                        concept_match=concept,
                        crawl_id=candidates[i].crawl_id,
                    ))

            logger.debug(
                f"Stage 3: {narrative_passed}/{len(semantic_matches)} candidates "
                f"passed narrative voice filter (min_indicators={MIN_NARRATIVE_INDICATORS})"
            )
        else:
            # No narrative filter — pass everything from Stage 2
            matches = []
            for i, score, concept in semantic_matches:
                matches.append(Match(
                    url=candidates[i].url,
                    warc_date=candidates[i].warc_date,
                    text=candidates[i].text,
                    matched_keywords=candidate_keywords[i],
                    semantic_score=score,
                    concept_match=concept,
                    crawl_id=candidates[i].crawl_id,
                ))

        return matches
