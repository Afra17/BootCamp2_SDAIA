from pathlib import Path  
from Data_profiler.config import make_paths
root = Path(__file__).resolve().parents[1]
print(root)
paths = make_paths(root)    