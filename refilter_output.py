import gzip
import json
import os
import logging
from pathlib import Path
from tqdm import tqdm

# Import the updated matcher and config
from matcher import HybridMatcher
from config import OUTPUT_DIR, SEMANTIC_THRESHOLD, MIN_NARRATIVE_INDICATORS

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)-7s | %(message)s')
logger = logging.getLogger(__name__)

def refilter():
    logger.info("Initializing HybridMatcher with new thresholds...")
    # Make sure we use the right thresholds even if config hasn't reloaded
    logger.info(f"Thresholds: Semantic >= {SEMANTIC_THRESHOLD}, Narrative >= {MIN_NARRATIVE_INDICATORS}")
    matcher = HybridMatcher(threshold=SEMANTIC_THRESHOLD)
    
    # Iterate through all language folders in output
    for lang_dir in OUTPUT_DIR.iterdir():
        if not lang_dir.is_dir():
            continue
            
        logger.info(f"Processing language: {lang_dir.name}")
        
        # Iterate through all .jsonl.gz files
        for file_path in tqdm(list(lang_dir.glob("*.jsonl.gz"))):
            records = []
            
            # Read existing records
            try:
                with gzip.open(file_path, "rt", encoding="utf-8") as f:
                    for line in f:
                        records.append(json.loads(line))
            except Exception as e:
                logger.error(f"Failed to read {file_path}: {e}")
                continue
            
            if not records:
                continue
                
            # Re-filter records
            passing_records = []
            for record in records:
                text = record["paragraph"]
                
                # Check semantic threshold (score from record)
                if record["semantic_score"] < SEMANTIC_THRESHOLD:
                    continue
                
                # Check narrative indicators (re-calculate with NEW logic)
                indicators = matcher.narrative_filter.count_indicators(text)
                if indicators >= MIN_NARRATIVE_INDICATORS:
                    passing_records.append(record)
            
            # Overwrite file with passing records (or delete if none left)
            if passing_records:
                try:
                    with gzip.open(file_path, "wt", encoding="utf-8") as f:
                        for record in passing_records:
                            f.write(json.dumps(record, ensure_ascii=False) + "\n")
                except Exception as e:
                    logger.error(f"Failed to write {file_path}: {e}")
            else:
                # If no records passed, delete the file
                try:
                    os.remove(file_path)
                except Exception as e:
                    logger.error(f"Failed to delete empty file {file_path}: {e}")
                    
        # Remove empty lang directory
        if not any(lang_dir.iterdir()):
            try:
                lang_dir.rmdir()
            except Exception:
                pass

    logger.info("Refiltering complete.")

if __name__ == "__main__":
    refilter()
