"""
FastText-based language detection wrapper.

Uses Facebook's lid.176.bin model to detect the language of text.
The model is downloaded on first use (~126 MB).
"""

import logging
import urllib.request

import fasttext

from config import FASTTEXT_MODEL_URL, FASTTEXT_MODEL_PATH, LANG_DETECT_CHARS

logger = logging.getLogger(__name__)

# Suppress fasttext warnings about loading with warning
fasttext.FastText.eprint = lambda x: None


class LanguageDetector:
    """Detects the language of a text using FastText."""

    def __init__(self):
        self._ensure_model()
        logger.info(f"Loading FastText language model from {FASTTEXT_MODEL_PATH}")
        self.model = fasttext.load_model(str(FASTTEXT_MODEL_PATH))
        logger.info("FastText language model loaded")

    def _ensure_model(self):
        """Download the FastText model if not already present."""
        if FASTTEXT_MODEL_PATH.exists():
            return

        logger.info(
            f"Downloading FastText model (~126 MB) to {FASTTEXT_MODEL_PATH}..."
        )
        FASTTEXT_MODEL_PATH.parent.mkdir(parents=True, exist_ok=True)

        urllib.request.urlretrieve(FASTTEXT_MODEL_URL, str(FASTTEXT_MODEL_PATH))
        logger.info("FastText model downloaded successfully")

    def detect(self, text: str) -> tuple[str, float]:
        """
        Detect the language of a text.

        Args:
            text: Input text (uses first LANG_DETECT_CHARS characters)

        Returns:
            Tuple of (language_code, confidence)
            language_code is e.g. "en", "zh", "de", etc.
        """
        # Clean text for prediction (fasttext expects single line)
        clean = text[:LANG_DETECT_CHARS].replace("\n", " ").strip()

        if not clean:
            return ("unknown", 0.0)

        predictions = self.model.predict(clean, k=1)
        # predictions = ([['__label__en']], [array([0.98])])
        label = predictions[0][0].replace("__label__", "")
        confidence = float(predictions[1][0])

        return (label, confidence)
