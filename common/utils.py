#!/usr/bin/env python3

import json
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml


def load_yaml_config(config_path: str) -> Dict[str, Any]:
    with open(config_path, "r") as f:
        return yaml.safe_load(f)


def load_jsonl_prompts(input_path: str, max_prompts: Optional[int] = None) -> List[str]:
    prompts: List[str] = []
    with open(input_path, "r") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            if max_prompts and len(prompts) >= max_prompts:
                break
            data = json.loads(line)
            prompt = data.get("text") or data.get("prompt")
            if prompt:
                prompts.append(prompt)
    return prompts


def save_jsonl_records(records: List[Dict[str, Any]], output_path: str) -> None:
    output_file = Path(output_path)
    output_file.parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, "w") as f:
        for record in records:
            f.write(json.dumps(record) + "\n")

# Made with Bob
