# Common Crawl Home/Belonging Extractor

A local, resumable Python application that extracts multilingual paragraphs about **home**, **hometown**, **belonging**, **roots**, **childhood**, **diaspora**, and **nostalgia** from Common Crawl web archive datasets spanning 2008–2026.

## How It Works

```
WET/ARC File → Split into Paragraphs → Keyword Pre-Filter → Semantic Scoring → Language Detection → JSONL Output
                                         (441 keywords,        (multilingual       (176 languages)    (by language)
                                          18 languages)         MiniLM model)
```

**Two-stage matching** keeps processing fast and accurate:

1. **Keyword Pre-Filter** — Scans each paragraph for any of 441 multilingual keywords covering home, belonging, roots, childhood, nostalgia, diaspora, and exile. Eliminates ~99% of irrelevant content instantly.
2. **Semantic Similarity Scoring** — Encodes remaining candidates with a multilingual sentence-transformer and compares them against 20 concept anchor sentences via cosine similarity. Filters out false positives like "home page" or "home button."

No LLM is used. The two ML models are small and run locally on CPU:

| Model | Size | Purpose |
|-------|------|---------|
| `paraphrase-multilingual-MiniLM-L12-v2` | ~500 MB | Sentence embeddings for semantic matching (50+ languages) |
| `lid.176.bin` (FastText) | ~126 MB | Language detection (176 languages) |

Both models are downloaded automatically on first run.

---

## 💾 Pre-extracted Data Available!

Because the full Common Crawl is massive, we have already run this extractor and synced the **ready-to-use output** directly to this repository! 

If you just want to read the extracted multilingual paragraphs, **you do not need to run any code or download models.** 
- Browse the processed JSONL datasets in `data/output/`
- Read the beautifully exported Markdown files in `data/exports/`

---

## Dataset Coverage

Supports **all 122+ Common Crawl datasets** from 2008 to present:

| Era | Years | Format | Files Available |
|-----|-------|--------|-----------------|
| Legacy | 2008–2012 | ARC (raw HTML → text extraction) | 3 crawls |
| Modern | 2013–present | WET (pre-extracted text) | 119+ crawls |

✨ **Auto-Discovery:** For modern crawls, the application automatically queries the Common Crawl Index API on startup. When Common Crawl publishes a new dataset, the application will pick it up automatically—no code updates required.

---

## Setup

### Requirements

- Python 3.10+
- ~700 MB disk space for ML models (downloaded once)
- Internet connection (for streaming Common Crawl files)

### Installation

```bash
cd cc-home-extractor
pip install -r requirements.txt
```

Dependencies:
- `warcio` — WARC/ARC file parsing
- `requests` — HTTP streaming
- `sentence-transformers` — Multilingual semantic similarity
- `fasttext-wheel` — Language detection
- `torch` — ML backend
- `tqdm` — Progress bars

---

## Usage

### List All Available Crawls

```bash
python main.py list
```

Shows all 122 crawls organized by year, with format labels (ARC vs WET).

### Process a Single Crawl

```bash
# Process the latest crawl
python main.py run --crawl CC-MAIN-2026-12

# Process a specific older crawl
python main.py run --crawl CC-MAIN-2019-04

# Process a legacy crawl (2008-2012)
python main.py run --crawl CC-CRAWL-001
```

### Process ALL Crawls

```bash
python main.py run --all
```

Processes all 122 crawls sequentially from oldest to newest. Fully resumable — stop and restart at any time.

### Test with a Small Sample

```bash
# Process only 5 files from a crawl (for testing/tuning)
python main.py run --crawl CC-MAIN-2026-12 --limit 5
```

### Check Progress

```bash
python main.py status
```

Shows overall and per-crawl progress including files completed, matches found, and percentage done.

### Adjust Semantic Threshold

```bash
# More strict (fewer but higher-quality matches)
python main.py run --crawl CC-MAIN-2026-12 --threshold 0.45

# More permissive (more matches, some may be loosely related)
python main.py run --crawl CC-MAIN-2026-12 --threshold 0.30
```

Default threshold is `0.35`.

### Stop and Resume

Press `Ctrl+C` at any time. The application will:
1. Finish processing the current file
2. Save all progress to the SQLite database
3. Exit cleanly

To resume, just run the same command again. It picks up exactly where it left off.

If the application crashes or is killed abruptly, any file that was mid-processing will be reset to "pending" on the next startup and re-processed from scratch. **Nothing is ever skipped or duplicated.**

---

## Output

### Structure

```
data/output/
├── en/           ← English matches
│   ├── crawl-data_CC-MAIN-2026-12_...00000.warc.wet.jsonl.gz
│   └── crawl-data_CC-MAIN-2026-12_...00001.warc.wet.jsonl.gz
├── de/           ← German matches
├── ja/           ← Japanese matches
├── zh/           ← Chinese matches
├── fr/           ← French matches
├── es/           ← Spanish matches
├── ar/           ← Arabic matches
└── ...           ← One folder per detected language
```

### Record Format

Each line in a `.jsonl.gz` file is a JSON object:

```json
{
  "url": "https://de.example.com/mein-leben",
  "warc_date": "2026-03-15T10:23:45Z",
  "language": "de",
  "language_confidence": 0.97,
  "paragraph": "Meine Heimat ist ein kleines Dorf in Bayern. Dort bin ich aufgewachsen und dort habe ich meine Kindheit verbracht. Die Erinnerungen an die Wiesen und Wälder sind mir bis heute geblieben.",
  "matched_keywords": ["Heimat", "Kindheit"],
  "semantic_score": 0.78,
  "concept_match": "Childhood memories of the place I call home"
}
```

| Field | Description |
|-------|-------------|
| `url` | Source web page URL |
| `warc_date` | When Common Crawl captured the page |
| `language` | Detected language (ISO 639-1 code) |
| `language_confidence` | FastText detection confidence (0–1) |
| `paragraph` | The extracted paragraph text |
| `matched_keywords` | Which keywords triggered the pre-filter |
| `semantic_score` | Cosine similarity to best concept anchor (0–1) |
| `concept_match` | The concept anchor sentence it matched |

### Reading Output (Markdown Export)

While files are stored as compressed JSONL to save space, you can easily export them to beautiful, readable Markdown files:

```bash
python export_md.py
```

This will convert all `.jsonl.gz` chunks into consolidated Markdown files in `data/exports/` (e.g., `matches_en.md`, `matches_de.md`), sorted by semantic score.

Alternatively, you can read the JSONL files directly in Python:

```python
import gzip
import json

with gzip.open("data/output/en/some_file.jsonl.gz", "rt", encoding="utf-8") as f:
    for line in f:
        record = json.loads(line)
        print(f"[{record['language']}] {record['semantic_score']:.2f} — {record['paragraph'][:100]}...")
```

### Review Helper

A built-in review script shows top matches in your terminal:

```bash
python review.py
```

---

## Project Structure

```
cc-home-extractor/
├── main.py               ← CLI entry point (run / status / list)
├── config.py              ← Tunable settings (thresholds, paths, model names)
├── crawl_catalog.py       ← Catalog of all 122 Common Crawl datasets
├── keywords.py            ← 441 multilingual keywords across 18 languages
├── concepts.py            ← 20 semantic anchor sentences
├── downloader.py          ← HTTP streaming for WET/ARC files
├── processor.py           ← WET text parser + ARC HTML-to-text extractor
├── matcher.py             ← Two-stage hybrid matcher (keyword + semantic)
├── language_detector.py   ← FastText wrapper (176 languages)
├── progress.py            ← SQLite-based resumable progress tracker
├── output.py              ← JSONL writer (gzip, organized by language)
├── export_md.py           ← Markdown exporter for extracted records
├── review.py              ← Helper script to inspect output quality
├── requirements.txt       ← Python dependencies
├── .gitignore             ← Excludes data/models/ from version control
└── data/                  ← Application data
    ├── progress.db        ← SQLite state database (synced to Git)
    ├── models/            ← Downloaded ML models (excluded from Git due to size)
    ├── exports/           ← Markdown exports (synced to Git)
    └── output/            ← Extracted results (synced to Git)
```

---

## Configuration

All settings are in `config.py`:

| Setting | Default | Description |
|---------|---------|-------------|
| `SEMANTIC_THRESHOLD` | `0.35` | Min cosine similarity to accept a match |
| `MIN_PARAGRAPH_LENGTH` | `100` | Skip paragraphs shorter than this (chars) |
| `MAX_PARAGRAPH_LENGTH` | `5000` | Skip paragraphs longer than this (chars) |
| `ENCODING_BATCH_SIZE` | `32` | Batch size for sentence-transformer |
| `DEFAULT_CRAWL_ID` | `CC-MAIN-2026-12` | Default crawl when `--crawl` is omitted |
| `LANG_DETECTION_THRESHOLD` | `0.5` | Min confidence for language detection |

---

## Customization

### Adding Keywords

Edit `keywords.py` to add keywords for new languages or refine existing ones:

```python
KEYWORDS_BY_LANGUAGE = {
    "your_lang_code": [
        "keyword1", "keyword2", "keyword3",
        # ...
    ],
    # ...
}
```

### Adding Concept Anchors

Edit `concepts.py` to add new anchor sentences that describe what you're looking for:

```python
CONCEPT_ANCHORS = [
    "Your new concept description here",
    # ...
]
```

The sentence-transformer will automatically use these as comparison targets. Because the model is multilingual, English anchor sentences match content in any supported language.

### Current Concept Anchors

The application is pre-configured with 20 anchor sentences organized into 7 thematic categories. These map conceptually to content in over 50 languages:

**Hometown & Place of Origin**
1. "My hometown is the place where I was born and raised. It shaped who I am and gave me my earliest memories."
2. "I grew up in a small village surrounded by nature. The streets and houses of my hometown are etched in my memory."
3. "Returning to the town where I spent my childhood fills me with a deep sense of connection and nostalgia."

**Childhood & Growing Up**
4. "Childhood memories of playing in the fields near our family home stay with me wherever I go."
5. "Growing up in my parents' house, I learned the values and traditions that would define my life."
6. "The experiences of my early years and upbringing in my native community formed my identity."

**Belonging & Community**
7. "The feeling of belonging to a community and knowing that you have a place where you are accepted."
8. "Home is not just a building — it is the sense of belonging, comfort, and safety that comes from being among your own people."
9. "Finding where you truly belong, the place and community that feels like home to your soul."

**Roots & Heritage**
10. "My roots run deep in this land. My ancestors lived here for generations, and their stories are part of who I am."
11. "Understanding your cultural heritage and ancestral origins gives you a foundation for your identity."
12. "The traditions passed down from our grandparents connect us to our roots and give meaning to where we come from."

**Nostalgia & Homecoming**
13. "After years of living abroad, I feel a deep longing for my homeland and the simple life I once knew."
14. "Homesickness is a powerful emotion — the ache of missing the familiar places, sounds, and smells of home."
15. "Coming back to the place where I grew up after many years brought tears to my eyes and warmth to my heart."

**Diaspora & Displacement**
16. "As an immigrant, I carry my homeland within me. My cultural identity bridges two worlds."
17. "The diaspora experience means being caught between two cultures, longing for a home that may no longer exist as you remember it."
18. "Being uprooted from your native land and having to rebuild a sense of home in a foreign country."

**Concept of Home**
19. "Home is more than a physical place. It is where the heart is, where you feel safe, loved, and truly yourself."
20. "The meaning of home changes as we grow older, but the longing for a place to call our own never fades."

### Tuning the Threshold

- **`0.30`** — More permissive. Catches loosely related content. Good for exploration.
- **`0.35`** — Default. Balanced precision/recall.
- **`0.40–0.45`** — More strict. Higher relevance but may miss some valid content.
- **`0.50+`** — Very strict. Only strong matches pass.

Recommended workflow: run with `--limit 10`, inspect output with `python review.py`, adjust threshold, repeat.

---

## Performance

| Metric | Estimate |
|--------|----------|
| Time per WET file (CPU) | ~40–60 seconds |
| Time per ARC file (CPU) | ~60–120 seconds (HTML parsing overhead) |
| Files per modern crawl | ~72,000–100,000 |
| Match rate | ~5–10 matches per file |
| Disk space per 1,000 files | ~5–15 MB (compressed JSONL) |

> **Tip:** You don't need to process entire crawls. Even 100–500 files per crawl yield thousands of matches across dozens of languages. Start small, review quality, then scale up.

---

## FAQ

**Q: Does this need a GPU?**
No. The sentence-transformer model (~500 MB) runs well on CPU. A CUDA GPU will be used automatically if available and will speed up semantic scoring ~5–10x.

**Q: Does this call any external API?**
No. Everything runs locally. The only network traffic is downloading WET/ARC files from Common Crawl's public servers.

**Q: How much disk space do I need?**
~700 MB for models (one-time download). Output is small — a few MB per 1,000 files processed. No raw data is stored locally.

**Q: Can I run this on multiple machines?**
Yes, but each machine tracks its own progress independently. To avoid duplicate work, assign different crawl IDs to different machines.

**Q: What if a WET/ARC file fails to download?**
It's marked as "failed" in the database and skipped. Failed files can be retried by resetting their status in the SQLite database. If an entire crawl's index is unavailable or returns a 404, the application logs a warning and gracefully skips to the next crawl without crashing.

**Q: Do I need AWS credentials?**
No, you do not need AWS credentials for the 119+ modern WET format datasets. However, because Common Crawl disabled anonymous listing on their legacy S3 buckets, the 3 legacy ARC datasets (2008-2012) require authenticated requests to build their file lists. If you do not have AWS credentials configured, the application will simply log an error (`NoCredentialsError`) and gracefully skip these oldest 3 datasets, allowing you to seamlessly process the modern datasets without needing an AWS account.

---

## License

This project processes publicly available Common Crawl data. Common Crawl data is available under the [Common Crawl Terms of Use](https://commoncrawl.org/terms-of-use).
