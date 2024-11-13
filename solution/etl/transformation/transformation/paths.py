from pathlib import Path

ROOT_PATH = Path(__file__).absolute().parent

DATA_PATH = ROOT_PATH.parent.parent.parent.parent / "data"

RAW_DATA_PATH = DATA_PATH / "raw"

EVENTS_DATASET_PATH = RAW_DATA_PATH / "events.jsonl"

CURATED_DATASET_PATH = DATA_PATH / "curated"
