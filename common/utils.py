import json
import os
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml


# --------------------------------------------
# Config
# --------------------------------------------
def load_config(config_path: str) -> Dict[str, Any]:
    with open(config_path, "r") as f:
        return yaml.safe_load(f)


def validate_config(cfg: Dict[str, Any]) -> None:
    lsf = cfg.get("lsf", {})
    model = cfg.get("model", {})
    execution = cfg.get("execution", {})

    # Required fields
    assert "num_workers" in lsf, "Missing lsf.num_workers"

    # Validate tensor parallel vs GPUs
    gpus_per_worker = lsf.get("gpus_per_worker", 0)
    total_gpus = lsf["num_workers"] * gpus_per_worker

    tp = model.get("tensor_parallel_size", 1)

    if tp != "auto" and total_gpus > 0:
        assert total_gpus >= tp, (
            f"Not enough GPUs: total={total_gpus}, tensor_parallel_size={tp}"
        )


# --------------------------------------------
# Paths
# --------------------------------------------
def resolve_output_path(path: str) -> str:
    job_id = os.environ.get("LSB_JOBID")
    if job_id:
        path = path.replace("%J", job_id).replace("{job_id}", job_id)
    return path


def ensure_dir(path: str) -> None:
    Path(path).parent.mkdir(parents=True, exist_ok=True)


# --------------------------------------------
# Data
# --------------------------------------------
def load_jsonl_prompts(
    path: str,
    max_prompts: Optional[int] = None
) -> List[str]:
    prompts = []

    with open(path, "r") as f:
        for line in f:
            if max_prompts and len(prompts) >= max_prompts:
                break

            line = line.strip()
            if not line:  # Skip empty lines
                continue

            data = json.loads(line)
            prompt = data.get("text") or data.get("prompt")

            if prompt:
                prompts.append(prompt)

    return prompts


# --------------------------------------------
# Logging
# --------------------------------------------
def configure_logging(level: str = "INFO") -> None:
    import logging

    logging.basicConfig(
        level=getattr(logging, level),
        format="%(asctime)s - %(levelname)s - %(message)s"
    )

