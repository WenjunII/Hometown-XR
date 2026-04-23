# Workstation Handoff Guide

This project is configured to share progress and results between your 3080 and 4090 workstations via GitHub.

## How to Switch Workstations

### Step 1: Save and Push from Machine A (e.g., 3080)
When you are done running on your first machine:
1. Stop the crawl (press `Ctrl+C`).
2. Wait for workers to finish current batches.
3. Commit and push the progress database and results:
   ```powershell
   git add .
   git commit -m "Sync: Progress from 3080"
   git push
   ```

### Step 2: Pull and Resume on Machine B (e.g., 4090)
On the second machine:
1. Update your local repository:
   ```powershell
   git pull
   ```
2. Resume the crawl using the 4090-optimized script:
   ```powershell
   python 4090/main.py run --all
   ```

## Why this works
- **Shared Data**: Both the root scripts and the `4090/` scripts now point to the same `data/progress.db` file.
- **Git Tracking**: We have configured Git to track the `progress.db` file (even though it is large), allowing the exact state of the crawl to move between machines.
