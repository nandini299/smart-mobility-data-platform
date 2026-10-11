"""Unsupervised anomaly detection on per-minute sensor aggregates (IsolationForest).

Ground truth (sim_label = 'episode') is used ONLY to evaluate, never to train.
"""
import os

import pandas as pd
from pyathena import connect
from pyathena.pandas.cursor import PandasCursor
from sklearn.ensemble import IsolationForest

BUCKET = os.environ["S3_BUCKET"]
REGION = os.getenv("AWS_REGION", "ap-south-1")
FEATURES = ["avg_v", "max_v", "std_v"]

SQL = """
select sensor_id, sensor_type,
       date_trunc('minute', event_ts)                          as minute,
       avg(value)                                              as avg_v,
       max(value)                                              as max_v,
       coalesce(stddev_samp(value), 0)                         as std_v,
       max(case when sim_label = 'episode' then 1 else 0 end)  as is_episode
from mobility_gold.stg_sensor_readings
group by 1, 2, 3
"""


def main():
    cur = connect(s3_staging_dir=f"s3://{BUCKET}/athena-results/",
                  region_name=REGION, cursor_class=PandasCursor).cursor()
    df = cur.execute(SQL).as_pandas()
    print(f"{len(df):,} sensor-minutes loaded")

    scored = []
    # One model per sensor type: CO ppm and visibility metres have different scales
    for stype, g in df.groupby("sensor_type"):
        model = IsolationForest(contamination=0.03, random_state=42)
        g = g.assign(predicted=(model.fit_predict(g[FEATURES]) == -1).astype(int))
        scored.append(g)
    out = pd.concat(scored)

    tp = int(((out.predicted == 1) & (out.is_episode == 1)).sum())
    fp = int(((out.predicted == 1) & (out.is_episode == 0)).sum())
    fn = int(((out.predicted == 0) & (out.is_episode == 1)).sum())
    precision = tp / (tp + fp) if tp + fp else 0.0
    recall = tp / (tp + fn) if tp + fn else 0.0
    print(f"precision={precision:.2%} recall={recall:.2%} (tp={tp} fp={fp} fn={fn})")

    out[out.predicted == 1].to_csv("ai/anomalies.csv", index=False)


if __name__ == "__main__":
    main()
