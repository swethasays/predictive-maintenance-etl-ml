import pandas as pd
import joblib, json
from datetime import datetime

model = joblib.load("/Users/swetha/predictive-maintenance-etl-ml/ml/xgb_model.pkl")
features = json.load(open("/Users/swetha/predictive-maintenance-etl-ml/ml/feature_list.json"))
df = pd.read_csv("/Users/swetha/predictive-maintenance-etl-ml/data/ai4i2020_featured.csv")

df.columns = (df.columns
                .str.strip()
                .str.lower()
                .str.replace(" ", "_")
                .str.replace(r"[\[\]<>]", "", regex=True))

for col in features:
    if col not in df.columns:
        df[col] = 0

X = df[features]

df["failure_proba"] = model.predict_proba(X)[:, 1]
df["failure_flag"] = (df["failure_proba"] > 0.5).astype(int)
df["scored_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

df.to_csv("/Users/swetha/predictive-maintenance-etl-ml/fact_events_pred.csv", index=False)
print("Predictions saved to ../data/fact_events_pred.csv")