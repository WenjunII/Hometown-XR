"""
Two-stage matching engine: keyword pre-filter → semantic similarity scoring.

Stage 1 (KeywordMatcher): Fast substring search using the multilingual keyword
dictionary. Eliminates ~99% of irrelevant paragraphs.

Stage 2 (SemanticMatcher): Embeds candidate paragraphs with a multilingual
sentence-transformer and scores them against pre-computed concept anchors
via cosine similarity.
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
    """A paragraph that passed both matching stages."""
    url: str
    warc_date: str
    text: str
    matched_keywords: list[str]
    semantic_score: float
    concept_match: str


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
        logger.info(f"Loading semantic model: {SEMANTIC_MODEL_NAME}")
        self.model = SentenceTransformer(SEMANTIC_MODEL_NAME)
        logger.info("Encoding concept anchors...")
        self.anchor_embeddings = self.model.encode(
            CONCEPT_ANCHORS,
            convert_to_tensor=True,
            show_progress_bar=False,
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


class HybridMatcher:
    """
    Orchestrates the two-stage matching pipeline.

    1. Keyword pre-filter (fast)
    2. Semantic similarity scoring (accurate)
    """

    def __init__(self, threshold: float = SEMANTIC_THRESHOLD):
        self.threshold = threshold
        self.keyword_matcher = KeywordMatcher()
        self.semantic_matcher = SemanticMatcher()
        logger.info(
            f"HybridMatcher ready (threshold={self.threshold})"
        )

    def process_paragraphs(self, paragraphs: list[Paragraph]) -> list[Match]:
        """
        Run both matching stages on a list of paragraphs.

        Args:
            paragraphs: List of Paragraph objects from the processor

        Returns:
            List of Match objects that passed both stages
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
        matches = []
        for i, (score, concept) in enumerate(scores):
            if score >= self.threshold:
                matches.append(Match(
                    url=candidates[i].url,
                    warc_date=candidates[i].warc_date,
                    text=candidates[i].text,
                    matched_keywords=candidate_keywords[i],
                    semantic_score=score,
                    concept_match=concept,
                ))

        logger.debug(
            f"Stage 2: {len(matches)}/{len(candidates)} candidates "
            f"passed semantic threshold ({self.threshold})"
        )

        return matches
