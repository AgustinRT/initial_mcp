import pytest
import os
from pathlib import Path

# Ensure the test directory exists for the module structure
test_dir = Path("test/mcps1")
os.makedirs(test_dir, exist_ok=True) 

print(f"Created necessary directory structure in {test_dir}")