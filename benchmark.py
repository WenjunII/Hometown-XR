"""Hardware benchmark and local profile autotuning."""

from __future__ import annotations

import json
import multiprocessing
import os
import platform
import time
from concurrent.futures import ProcessPoolExecutor
from datetime import datetime, timezone

from config import HARDWARE_OVERRIDE_PATH, get_hardware_profile

_CPU_TEXT = (
    "I remember the old family home where we grew up, the rooms, the garden, "
    "and the feeling of returning to my hometown after many years away."
)


def _cpu_keyword_task(iterations: int) -> int:
    from matcher import KeywordMatcher

    matcher = KeywordMatcher()
    matches = 0
    for _ in range(iterations):
        matches += bool(matcher.find_matches(_CPU_TEXT))
    return matches


def _cpu_benchmark(quick: bool) -> list[dict]:
    cpu_count = os.cpu_count() or 1
    worker_counts = [count for count in (1, 2, 4, 7) if count <= cpu_count]
    iterations_per_worker = 2_000 if quick else 10_000
    context = multiprocessing.get_context("spawn")
    results = []
    for workers in worker_counts:
        started = time.perf_counter()
        with ProcessPoolExecutor(max_workers=workers, mp_context=context) as executor:
            total_matches = sum(
                executor.map(_cpu_keyword_task, [iterations_per_worker] * workers)
            )
        elapsed = time.perf_counter() - started
        results.append(
            {
                "workers": workers,
                "paragraphs": iterations_per_worker * workers,
                "matches": total_matches,
                "seconds": round(elapsed, 4),
                "paragraphs_per_second": round(iterations_per_worker * workers / elapsed, 2),
            }
        )
    return results


def _gpu_benchmark(quick: bool) -> tuple[str, list[dict]]:
    import torch

    if not torch.cuda.is_available():
        return "CPU", []
    from matcher import SemanticMatcher

    gpu_name = torch.cuda.get_device_name(0)
    batch_sizes = [64, 128] if quick else [64, 128, 256, 512]
    matcher = SemanticMatcher(encoding_batch_size=max(batch_sizes))
    matcher.score_paragraphs([_CPU_TEXT] * 8)
    results = []
    for batch_size in batch_sizes:
        matcher.encoding_batch_size = batch_size
        torch.cuda.empty_cache()
        torch.cuda.reset_peak_memory_stats()
        started = time.perf_counter()
        try:
            matcher.score_paragraphs([_CPU_TEXT] * batch_size)
            torch.cuda.synchronize()
        except RuntimeError as exc:
            if "out of memory" not in str(exc).lower():
                raise
            results.append({"batch_size": batch_size, "status": "out_of_memory"})
            torch.cuda.empty_cache()
            break
        elapsed = time.perf_counter() - started
        results.append(
            {
                "batch_size": batch_size,
                "status": "ok",
                "seconds": round(elapsed, 4),
                "paragraphs_per_second": round(batch_size / elapsed, 2),
                "peak_vram_mb": round(torch.cuda.max_memory_allocated() / 1024**2, 1),
            }
        )
    return gpu_name, results


def run_benchmark(profile_name: str = "auto", quick: bool = False, write: bool = True) -> dict:
    profile = get_hardware_profile(profile_name)
    cpu_results = _cpu_benchmark(quick)
    gpu_name, gpu_results = _gpu_benchmark(quick)
    best_cpu = max(cpu_results, key=lambda item: item["paragraphs_per_second"])
    successful_gpu = [item for item in gpu_results if item.get("status") == "ok"]
    best_gpu = (
        max(successful_gpu, key=lambda item: item["paragraphs_per_second"])
        if successful_gpu
        else None
    )
    recommendation = {
        "profile": profile.name,
        "workers": int(best_cpu["workers"]),
        "candidate_batch_size": profile.candidate_batch_size,
        "inference_batch_size": (
            max(profile.inference_batch_size, int(best_gpu["batch_size"]) * 4)
            if best_gpu
            else profile.inference_batch_size
        ),
        "encoding_batch_size": (
            int(best_gpu["batch_size"]) if best_gpu else profile.encoding_batch_size
        ),
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "gpu": gpu_name,
        "host": platform.node(),
    }
    payload = {
        "schema_version": 1,
        "quick": quick,
        "cpu": cpu_results,
        "gpu": gpu_results,
        "recommendation": recommendation,
    }
    if write:
        HARDWARE_OVERRIDE_PATH.parent.mkdir(parents=True, exist_ok=True)
        temporary = HARDWARE_OVERRIDE_PATH.with_suffix(".json.tmp")
        temporary.write_text(json.dumps(recommendation, indent=2) + "\n", encoding="utf-8")
        os.replace(temporary, HARDWARE_OVERRIDE_PATH)
    return payload
