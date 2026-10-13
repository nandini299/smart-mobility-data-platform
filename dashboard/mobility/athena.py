import os

import pandas as pd
from dotenv import load_dotenv
from pyathena import connect

load_dotenv()

conn = connect(
    region_name=os.getenv("AWS_REGION", "ap-south-1"),
    s3_staging_dir=f"s3://{os.environ['S3_BUCKET']}/athena-results/",
    work_group=os.getenv("ATHENA_WORKGROUP", "primary"),
)


def run_query(sql):
    return pd.read_sql(sql, conn)
