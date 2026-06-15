"""
Simple CPU-based batch inference test using a small encoder model.
Tests Ray batch processing with transformers on CPU.
"""
import ray
import time
import os
from transformers import AutoTokenizer, AutoModel
import torch
import json
from datetime import datetime

# Connect to Ray cluster
# Use head_node_ip if available (for multi-node), otherwise fall back to head_node
head_node = str(os.environ.get("head_node_ip", os.environ["head_node"]))
port = str(os.environ["port"])
print(f"Connecting to Ray cluster at {head_node}:{port}")
ray.init(address=f"{head_node}:{port}")

# Small encoder model for CPU testing
# Note: Replace with IBM Slate model when available (e.g., ibm-granite/granite-embedding-30m-english)
MODEL_NAME = "sentence-transformers/all-MiniLM-L6-v2"  # 22M parameters, fast on CPU

@ray.remote
class EncoderActor:
    """Actor that loads model once and processes batches."""
    
    def __init__(self, model_name: str):
        print(f"Loading model {model_name}...")
        self.tokenizer = AutoTokenizer.from_pretrained(model_name)
        self.model = AutoModel.from_pretrained(model_name)
        self.model.eval()
        print(f"Model loaded on {ray.get_runtime_context().get_node_id()}")
    
    def encode_batch(self, texts: list) -> dict:
        """Encode a batch of texts and return embeddings."""
        start_time = time.time()
        
        # Tokenize
        inputs = self.tokenizer(
            texts, 
            padding=True, 
            truncation=True, 
            return_tensors="pt",
            max_length=128
        )
        
        # Get embeddings
        with torch.no_grad():
            outputs = self.model(**inputs)
            # Mean pooling
            embeddings = outputs.last_hidden_state.mean(dim=1)
        
        inference_time = time.time() - start_time
        
        return {
            "num_texts": len(texts),
            "embedding_dim": embeddings.shape[1],
            "inference_time": inference_time,
            "throughput": len(texts) / inference_time
        }

def main():
    print("\n=== CPU Batch Inference Test ===")
    print(f"Model: {MODEL_NAME}")
    print(f"Time: {datetime.now().isoformat()}\n")
    
    # Sample texts for encoding
    sample_texts = [
        "Ray is a unified framework for scaling AI and Python applications.",
        "Batch inference allows processing multiple inputs efficiently.",
        "CPU inference is useful for smaller models and testing.",
        "Transformers library provides easy access to pre-trained models.",
        "Distributed computing enables scaling workloads across clusters.",
    ]
    
    # Create 2 encoder actors (one per CPU core available)
    num_actors = 2
    print(f"Creating {num_actors} encoder actors...")
    actors = [EncoderActor.remote(MODEL_NAME) for _ in range(num_actors)]
    
    # Wait for actors to initialize
    ray.get([actor.encode_batch.remote(["test"]) for actor in actors])
    print("Actors initialized!\n")
    
    # Run batch inference
    print("Running batch inference...")
    num_batches = 10
    batch_size = 5
    
    start_time = time.time()
    results = []
    
    for i in range(num_batches):
        # Distribute batches across actors
        actor = actors[i % num_actors]
        result = actor.encode_batch.remote(sample_texts[:batch_size])
        results.append(result)
    
    # Wait for all results
    results = ray.get(results)
    total_time = time.time() - start_time
    
    # Calculate statistics
    total_texts = sum(r["num_texts"] for r in results)
    avg_throughput = sum(r["throughput"] for r in results) / len(results)
    
    print("\n=== Results ===")
    print(f"Total batches processed: {num_batches}")
    print(f"Total texts encoded: {total_texts}")
    print(f"Total time: {total_time:.2f}s")
    print(f"Overall throughput: {total_texts / total_time:.2f} texts/sec")
    print(f"Average batch throughput: {avg_throughput:.2f} texts/sec")
    print(f"Embedding dimension: {results[0]['embedding_dim']}")
    
    # Save results
    output_dir = "batch_inference_results"
    os.makedirs(output_dir, exist_ok=True)
    
    output_file = os.path.join(
        output_dir, 
        f"cpu_test_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    )
    
    output_data = {
        "model": MODEL_NAME,
        "timestamp": datetime.now().isoformat(),
        "num_actors": num_actors,
        "num_batches": num_batches,
        "batch_size": batch_size,
        "total_texts": total_texts,
        "total_time": total_time,
        "overall_throughput": total_texts / total_time,
        "avg_batch_throughput": avg_throughput,
        "embedding_dim": results[0]["embedding_dim"],
        "batch_results": results
    }
    
    with open(output_file, 'w') as f:
        json.dump(output_data, f, indent=2)
    
    print(f"\nResults saved to: {output_file}")
    print("\n=== Test Complete ===")
    
    ray.shutdown()

if __name__ == "__main__":
    main()

# Made with Bob
