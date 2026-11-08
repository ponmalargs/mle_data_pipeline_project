import os
from pathlib import Path
from datetime import date
import yaml


def _find_config() -> Path:
    # 1. explicit path from an environment variable
    if os.getenv("PIPELINE_CONFIG"):
        return Path(os.environ["PIPELINE_CONFIG"]).resolve()
    # 2. config.yaml in the current folder or any parent folder
    for folder in [Path.cwd(), *Path.cwd().parents]:
        if (folder / "config.yaml").exists():
            return folder / "config.yaml"
    # 3. the project root, relative to this file (src/taxi_pipeline/config.py)
    return Path(__file__).resolve().parents[2] / "config.yaml"


CONFIG_FILE = _find_config()
PROJECT_ROOT = CONFIG_FILE.parent

def load_config() -> dict:
    with open(CONFIG_FILE, encoding="utf-8") as f:
        cfg = yaml.safe_load(f)

    # Turn every relative path into an absolute one, based on the project root
    cfg["paths"] = {k: (PROJECT_ROOT / v).resolve() for k, v in cfg["paths"].items()}
    return cfg


CFG = load_config()


def file_name(year: int, month: int) -> str:
    return CFG["source"]["file_pattern"].format(year=year, month=month)


def file_url(year: int, month: int) -> str:
    return f'{CFG["source"]["base_url"]}/{file_name(year, month)}'


def run_period() -> tuple[int, list[int]]:
    """Year/months from config, or lag_months before today if they are empty."""
    run = CFG["run"]
    if run.get("year") and run.get("months"):
        return run["year"], list(run["months"])
    today = date.today()
    total = today.year * 12 + (today.month - 1) - run["lag_months"]
    return total // 12, [total % 12 + 1]