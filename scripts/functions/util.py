import os
import sys
import glob
from pathlib import Path


def find_input_file(data_dir: str, filename: str, extension: str) -> str:
    pattern = os.path.join(data_dir, f"*{filename}*.{extension}")
    matches = glob.glob(pattern)
    if len(matches) != 1:
        print(f"[Erro] Esperava um único arquivo em '{data_dir}' que case com '*failure_sensors*.txt', achei: {matches}")
        sys.exit(1)
    return matches[0]