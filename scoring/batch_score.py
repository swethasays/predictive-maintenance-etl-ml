import pandas as pd
import joblib, json
from datetime import datetime
import os, glob

# repo root visible both locally and in containers
PROJECT_DIR = os.environ.get("PROJECT_DIR", "/opt/airflow/project")

MODEL_PATH   = f"{PROJECT_DIR}/ml/xgb_model.pkl"
FEATS_PATH   = f"{PROJECT_DIR}/ml/feature_list.json"
SILVER_DIR   = f"{PROJECT_DIR}/data/ai4i_stream_silver.csv"   # Spark wrote a FOLDER here
OUT_PATH     = f"{PROJECT_DIR}/data/fact_events_pred.csv"

# pick the CSV part file inside the Spark output folder
part_files = sorted(glob.glob(os.path.join(SILVER_DIR, "part-*.csv")))
if not part_files:
    raise FileNotFoundError(f"No CSV part files found in {SILVER_DIR}")
silver_csv = part_files[0]

# load model & features
model    = joblib.load(MODEL_PATH)
features = json.load(open(FEATS_PATH))

# load silver data
df = pd.read_csv(silver_csv)

# normalize column names (must match training)
df.columns = (
    df.columns.str.strip().str.lower().str.replace(" ", "_").str.replace(r"[\[\]<>]", "", regex=True)
)

# backfill any missing features the model expects
for col in features:
    if col not in df.columns:
        df[col] = 0

# score
X = df[features]
df["failure_proba"] = model.predict_proba(X)[:, 1]
df["failure_flag"]  = (df["failure_proba"] > 0.5).astype(int)
df["scored_at"]     = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

# save
df.to_csv(OUT_PATH, index=False)
print(f"Predictions saved to {OUT_PATH}")