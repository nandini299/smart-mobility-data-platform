import os

import pandas as pd
import streamlit as st
from dotenv import load_dotenv
from pyathena import connect

load_dotenv()

st.set_page_config(
    page_title="NYC Taxi Analytics",
    layout="wide"
)

st.title("🚕 NYC Taxi Mobility Dashboard")
st.write("Data Engineering Portfolio Project")

@st.cache_resource
def get_connection():
    return connect(
        region_name=os.getenv("AWS_REGION", "ap-south-1"),
        s3_staging_dir=f"s3://{os.environ['S3_BUCKET']}/athena-results/",
    )


# Every widget change re-runs the script; caching avoids paying for the
# same Athena scan again when a filter combination repeats.
@st.cache_data(ttl=3600, show_spinner="Querying Athena...")
def run_query(sql):
    return pd.read_sql(sql, get_connection())



# -------------------------
# Sidebar Filters
# -------------------------

st.sidebar.header("🔎 Filters")

year = st.sidebar.selectbox(
    "Year",
    [2023]
)

month_options = ["All"] + [
    f"{month:02d}" for month in range(1, 13)
]

month = st.sidebar.selectbox(
    "Month",
    month_options
)



pickup_zone = st.sidebar.multiselect(
    "Pickup Zone",
    list(range(1, 266))
)

dropoff_zone = st.sidebar.multiselect(
    "Dropoff Zone",
    list(range(1, 266))
)

payment_type = st.sidebar.multiselect(
    "Payment Type",
    [0, 1, 2, 3, 4, 5, 6]
)



# -------------------------
# Build Filter
# -------------------------

where_clause = f"WHERE pickup_month LIKE '{year}-%'"

if month != "All":
    where_clause += f" AND pickup_month = '{year}-{month}'"

if pickup_zone:
    zones = ",".join(str(z) for z in pickup_zone)
    where_clause += f" AND pickup_zone_id IN ({zones})"

if dropoff_zone:
    zones = ",".join(str(z) for z in dropoff_zone)
    where_clause += f" AND dropoff_zone_id IN ({zones})"

if payment_type:
    payments = ",".join(str(p) for p in payment_type)
    where_clause += f" AND payment_type_id IN ({payments})"
    
    
query = f"""
SELECT
    COUNT(*) AS total_trips,
    SUM(total_amount) AS total_revenue,
    AVG(total_amount) AS avg_fare,
    AVG(trip_distance) AS avg_distance,
    AVG(trip_minutes) AS avg_duration
FROM mobility_gold.fact_trips
{where_clause}
"""

df = run_query(query)

row = df.iloc[0]

col1, col2, col3, col4, col5 = st.columns(5)

col1.metric("Total Trips", f"{row['total_trips']:,.0f}")
col2.metric("Revenue", f"${row['total_revenue']:,.0f}")
col3.metric("Avg Fare", f"${row['avg_fare']:.2f}")
col4.metric("Avg Distance", f"{row['avg_distance']:.2f}")
col5.metric("Avg Duration", f"{row['avg_duration']:.1f} min")



st.divider()

st.subheader("📈 Monthly Trip & Revenue Analysis")

monthly_query = f"""
SELECT
    pickup_month,
    COUNT(*) AS trips,
    SUM(total_amount) AS revenue,
    AVG(total_amount) AS avg_fare,
    AVG(trip_distance) AS avg_distance,
    AVG(trip_minutes) AS avg_duration
FROM mobility_gold.fact_trips
{where_clause}
GROUP BY pickup_month
ORDER BY pickup_month
"""

monthly_df = run_query(monthly_query)

st.write("Monthly Trips")
st.line_chart(
    monthly_df.set_index("pickup_month")["trips"]
)

st.write("Monthly Revenue")
st.line_chart(
    monthly_df.set_index("pickup_month")["revenue"]
)







