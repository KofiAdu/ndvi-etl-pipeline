import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from main import run_pipeline

if __name__ == "__main__":
    run_pipeline()
