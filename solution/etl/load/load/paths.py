from pathlib import Path

ROOT_PATH = Path(__file__).absolute().parent

DATA_PATH = ROOT_PATH.parent.parent.parent.parent / "data"

CURATED_DATASET_PATH = DATA_PATH / "curated"

REGISTRATION_DATASET = CURATED_DATASET_PATH / "registration"

MATCH_DATASET = CURATED_DATASET_PATH / "match"

SESSION_DATASET = CURATED_DATASET_PATH / "session_ping"

TIMEZONE_DEFINITION_DATASET = CURATED_DATASET_PATH / "timezones.jsonl"
