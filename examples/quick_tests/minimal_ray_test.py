#!/usr/bin/env python3

import ray

ray.init(address="auto")
print(ray.cluster_resources())
ray.shutdown()
