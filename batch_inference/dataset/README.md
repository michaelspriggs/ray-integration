# Dataset Format

This directory contains sample datasets for batch inference.

## Input Format

Input files should be in JSONL (JSON Lines) format, with one JSON object per line.

### Required Fields

Each line must contain either:
- `text`: The prompt text
- `prompt`: The prompt text (alternative field name)

### Example

```jsonl
{"text": "Explain quantum computing"}
{"prompt": "What is machine learning?"}
{"text": "Describe neural networks", "metadata": {"id": 1}}
```

### Additional Fields

You can include additional fields for metadata, which will be preserved in the output:

```jsonl
{"text": "Your prompt here", "id": "prompt_001", "category": "science"}
```

## Creating Your Own Dataset

### From a text file (one prompt per line)

```python
import json

with open('prompts.txt', 'r') as infile, open('prompts.jsonl', 'w') as outfile:
    for line in infile:
        prompt = line.strip()
        if prompt:
            json.dump({"text": prompt}, outfile)
            outfile.write('\n')
```

### From a CSV file

```python
import csv
import json

with open('prompts.csv', 'r') as infile, open('prompts.jsonl', 'w') as outfile:
    reader = csv.DictReader(infile)
    for row in reader:
        # Assuming CSV has a 'prompt' column
        json.dump({"text": row['prompt']}, outfile)
        outfile.write('\n')
```

### From a list in Python

```python
import json

prompts = [
    "First prompt",
    "Second prompt",
    "Third prompt",
]

with open('prompts.jsonl', 'w') as f:
    for prompt in prompts:
        json.dump({"text": prompt}, f)
        f.write('\n')
```

## Sample Dataset

The `sample_prompts.jsonl` file contains 20 example prompts about distributed computing, Ray, and machine learning topics. Use this for testing the batch inference pipeline.

## Output Format

The output will also be in JSONL format with the following structure:

```jsonl
{
  "prompt": "Original prompt text",
  "generated_text": ["Generated completion"],
  "finish_reason": ["stop"],
  "num_tokens": [42]
}
```

If `n > 1` in the generation config, the arrays will contain multiple completions per prompt.