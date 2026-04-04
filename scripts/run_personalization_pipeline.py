#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from training.personalization_pipeline import run_pipeline


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the offline personalization training pipeline.")
    parser.add_argument("--project-root", default=str(PROJECT_ROOT))
    parser.add_argument("--config-path", default=None, help="Optional JSON-as-YAML config override.")
    args = parser.parse_args()

    result = run_pipeline(project_root=Path(args.project_root), config_path=args.config_path)
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()

