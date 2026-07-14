# Hometown XR Common Crawl Extractor

A resumable multilingual pipeline for finding first-person stories about home,
hometown, childhood, roots, migration, and belonging in Common Crawl data.

Canonical repository: [wenjunii/hometown-xr](https://github.com/wenjunii/hometown-xr)

## Models

The extractor uses two local machine-learning models:

- `sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2` scores text
  against the concepts in `concepts.py`.
- FastText `lid.176.bin` identifies language and records prediction confidence.

Neither model is a generative LLM. The crawler does not call OpenAI, Gemini,
Anthropic, or another hosted text-generation API.

## Workstation Safety

The RTX 3080 and RTX 4090 PCs share one Git checkpoint. Run the crawler on
only one PC at a time.

Before switching machines:

1. Press `Ctrl+C` once and wait for the final summary.
2. Confirm `data/.crawler.lock` is gone.
3. Commit and push on the old PC.
4. Pull Git and Git LFS on the new PC.
5. Run `main.py doctor` and `main.py status` before resuming.

See [HANDOFF.md](HANDOFF.md) for the exact commands.

## Architecture

One run has seven lightweight CPU parser processes and one inference owner:

```text
WET/ARC sources
    -> bounded CPU download/parse/keyword workers
    -> bounded candidate queue
    -> one shared sentence-transformer on the GPU
    -> FastText language detection
    -> source-scoped staged output
    -> atomic shard + manifest commit
    -> SQLite checkpoint completion
```

The semantic model is loaded once, not once per worker. The process pool and
GPU service are reused across crawls. Queue backpressure keeps RAM bounded when
the CPU workers are faster than inference.

Each source has a SQLite lease. Interrupted sources return to `pending`, hard
crashes are recovered after the lease timeout, and failed sources retry with
exponential backoff. Output becomes visible only after an entire source is
successfully parsed and filtered.

## Quick Start

Requirements:

- Windows 10 or 11
- Python 3.10
- Git and Git LFS
- NVIDIA driver compatible with CUDA 12.1
- RTX 3080-class or RTX 4090 GPU

First-time setup on this RTX 3080 PC:

```powershell
git lfs install
git lfs pull
Set-ExecutionPolicy -Scope Process Bypass
.\scripts\setup.ps1 -Profile 3080
```

Use `-Profile 4090` on the other PC. Setup installs the exact versions in
`requirements-lock.txt`, including the CUDA 12.1 PyTorch wheel.

Verify the environment and checkpoint:

```powershell
.\.venv\Scripts\python.exe main.py doctor --profile 3080
.\.venv\Scripts\python.exe main.py status
```

Start or resume one crawl:

```powershell
.\scripts\run.ps1 -Profile 3080 run --crawl CC-MAIN-2026-12
```

Resume every available crawl:

```powershell
.\scripts\run.ps1 -Profile 3080 run --all
```

The old `4090\main.py` command remains a compatibility launcher. It invokes
the same root implementation and shared `data/` checkpoint.

## Hardware Profiles

| Profile | CPU workers | Candidate batch | Inference batch | Encoding batch |
| --- | ---: | ---: | ---: | ---: |
| `3080` | 7 | 100 | 800 | 128 |
| `4090` | 7 | 150 | 1600 | 256 |

`candidate_batch_size` controls worker-to-parent messages.
`inference_batch_size` combines candidates from multiple sources before one
semantic call. `encoding_batch_size` is the sentence-transformer CUDA batch.

Benchmark this PC and write an ignored, machine-local override:

```powershell
.\scripts\benchmark.ps1 -Profile 3080
```

Use `-Quick` for a shorter pass. Results are written to
`data/hardware-profile.local.json`; that file is intentionally not synchronized
because the 3080 and 4090 should keep their own measured settings.

One-run overrides are also available:

```powershell
python main.py run --profile 3080 --workers 7 `
  --candidate-batch-size 100 --inference-batch-size 800 --encoding-batch-size 128
```

## Filtering

Each paragraph passes through:

1. A multilingual keyword prefilter from `keywords.py`.
2. Sentence-transformer similarity against home and belonging concepts.
3. A multilingual first-person narrative filter.

CJK, Japanese, Korean, and Thai keywords use substring matching where word
boundaries are unreliable. FastText predictions below the confidence threshold
are stored under `unknown/` with the original confidence.

Every live candidate can contribute to a deterministic local evaluation sample.
The sample stores semantic score, narrative score, acceptance, and rejection
reason so threshold changes can be evaluated against human labels.

## Commands

| Command | Purpose |
| --- | --- |
| `python main.py run --crawl ID` | Start or resume one crawl |
| `python main.py run --all` | Process every known crawl |
| `python main.py run --limit 5` | Process at most five ready sources |
| `python main.py status` | Show checkpoint progress |
| `python main.py metrics` | Show latest rates, GPU time, and ETA |
| `python main.py doctor --profile 3080` | Check Python, PyTorch, CUDA, and profile |
| `python main.py benchmark --profile 3080` | Benchmark and tune this PC |
| `python main.py retry --all` | Retry all failed sources immediately |
| `python main.py recover --minutes 10` | Release expired source leases |
| `python main.py verify-output` | Verify committed shard checksums |
| `python main.py parquet --dedupe exact` | Build partitioned Parquet output |
| `python main.py evaluation sample` | Build a real-text annotation sample |
| `python main.py evaluation annotate` | Label samples interactively |
| `python main.py evaluation report` | Compute precision, recall, F1, and tuning |
| `python main.py reset` | Delete output, derivatives, and progress |

Use `recover --minutes 0` only after confirming no crawler is running.

## JSONL Output

Output is gzip-compressed JSON Lines grouped by detected language:

```text
data/
  progress.db
  models/
    lid.176.bin
  output/
    _manifests/
      <source-hash>.json
    en/
      <source-hash>_<source-name>.jsonl.gz
    zh/
    unknown/
```

Schema version 2 records include deterministic provenance and content IDs:

```json
{
  "schema_version": 2,
  "record_id": "<sha256>",
  "content_fingerprint": "<sha256>",
  "crawl_id": "CC-MAIN-2026-12",
  "source_file": "crawl-data/.../example.warc.wet.gz",
  "url": "https://example.org/story",
  "warc_date": "2026-03-01T12:00:00Z",
  "language": "en",
  "language_confidence": 0.9821,
  "paragraph": "I remember the home where I grew up...",
  "matched_keywords": ["home", "grew up"],
  "semantic_score": 0.7312,
  "concept_match": "memories of childhood home",
  "narrative_score": 12
}
```

Each source with output has a manifest recording shard paths, row counts, byte
sizes, and SHA-256 checksums. Zero-match completion is already represented in
SQLite, so it does not create hundreds of thousands of empty manifest files.

Migrate existing output to schema version 2 and rebuild manifests with:

```powershell
python refilter_output.py
python main.py verify-output
```

The migration stages a complete replacement, atomically swaps it into place,
and updates SQLite counts in the same journaled operation.

## Parquet And Deduplication

Build a local analytical dataset:

```powershell
python main.py parquet --dedupe exact
python main.py parquet --dedupe near --near-distance 3
```

Parquet is partitioned by `crawl_id` and `language`, compressed with Zstandard,
and installed by atomic directory swap. Exact deduplication uses normalized
content fingerprints. Near deduplication uses 64-bit SimHash with an
SQLite-backed band index, so memory does not grow with the corpus. Duplicate
decisions and dataset checksums are included in the export manifest.

`data/parquet/` is ignored because it is reproducible and can be large.

## Evaluation

Build an unlabeled sample from real committed records and sampled live rejects:

```powershell
python main.py evaluation sample --size 400
python main.py evaluation annotate
python main.py evaluation report
```

Sampling is deterministic and language-stratified. Existing labels are kept
when a sample is rebuilt. Reports use only human-labeled rows and include an
overall confusion matrix, per-language metrics, false-positive/false-negative
IDs, and a semantic/narrative threshold grid search.

The original synthetic regression corpus remains available:

```powershell
python scripts\evaluate_filters.py
```

## Review And Export

```powershell
python review.py --limit 20
python export_md.py
```

Both commands stream data or use temporary SQLite storage instead of loading
the complete corpus into memory.

## Development

```powershell
.\.venv\Scripts\python.exe -m pip install -r requirements-test.txt
.\scripts\test.ps1
```

The suite covers leases, retries, interruption, source transactions, stable
IDs, checksum rollback, multilingual filtering, real WET parsing, spawned
Windows-compatible orchestration, deduplication, and Parquet export. GitHub
Actions runs lint, tests, and compilation on both Windows and Ubuntu without
downloading GPU models or Git LFS data.

## Project Structure

```text
main.py                 CLI and run orchestration
pipeline.py             bounded CPU queue and shared GPU inference owner
progress.py             SQLite leases, retries, and checkpoint migration
output.py               source transactions, stable IDs, and manifests
processor.py            WET/ARC parsing and counters
matcher.py              keyword, semantic, and narrative filters
evaluation.py           real-text sampling, annotation, and reports
metrics.py              run rates, GPU timing, and ETA
benchmark.py            local hardware benchmark and autotuning
dedupe.py               disk-backed exact and SimHash duplicate index
parquet_export.py       staged partitioned analytical export
refilter_output.py      transactional schema/filter migration
4090/                   compatibility launchers only
scripts/                setup, run, test, benchmark, and handoff commands
tests/                  unit, regression, and integration tests
```

## License

MIT. See [LICENSE](LICENSE). Common Crawl data remains subject to the
[Common Crawl Terms of Use](https://commoncrawl.org/terms-of-use).
