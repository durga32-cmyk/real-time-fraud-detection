"""Central settings. Everything can be overridden with environment variables."""
import glob
import os
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT / "data"
MODEL_DIR = ROOT / "models"
PIPELINE_PATH = str(MODEL_DIR / "gbt_pipeline")
THRESHOLD_PATH = MODEL_DIR / "threshold.json"
METRICS_PATH = MODEL_DIR / "metrics.json"
CHECKPOINT_DIR = str(ROOT / "checkpoints" / "stream")

# --- Kafka ---
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "transactions")
NUM_PARTITIONS = 3

# --- PostgreSQL ---
PG = dict(
    host=os.getenv("PG_HOST", "localhost"),
    port=int(os.getenv("PG_PORT", "5432")),
    dbname=os.getenv("PG_DB", "fraud"),
    user=os.getenv("PG_USER", "fraud"),
    password=os.getenv("PG_PASSWORD", "fraud"),
)

# --- Time-based split on PaySim's `step` column (1 step = 1 hour, 1..743) ---
TRAIN_MAX_STEP = 500          # train:      step <= 500
VAL_MAX_STEP = 620            # validation: 501..620  (threshold is chosen here)
                              # test:       step >= 621 (final numbers + stream replay)

# --- Business assumptions for choosing the alert threshold ---
REVIEW_COST = float(os.getenv("REVIEW_COST", "10"))   # cost of an analyst reviewing one alert
BASELINE_AMOUNT = 200_000                             # static rule: TRANSFER with amount > this

# Keep this fraction of non-fraud rows when training (fraud rows are always kept).
NEG_KEEP_FRACTION = float(os.getenv("NEG_KEEP_FRACTION", "0.1"))

SPARK_KAFKA_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3"


def find_csv() -> str:
    files = sorted(glob.glob(str(DATA_DIR / "*.csv")))
    if not files:
        raise FileNotFoundError(
            f"No CSV found in {DATA_DIR}. Download the PaySim dataset and place it there."
        )
    return files[0]
