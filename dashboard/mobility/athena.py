import pandas as pd
from pyathena import connect

conn = connect(
    region_name="ap-south-1",
    s3_staging_dir="s3://nm-2026/athena-results/"
)


def run_query(sql):
    return pd.read_sql(sql, conn)
