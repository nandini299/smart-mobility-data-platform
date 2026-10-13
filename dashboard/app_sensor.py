import os

import streamlit as st
from dotenv import load_dotenv
from pyathena import connect
from pyathena.pandas.cursor import PandasCursor

# ---------------------------------------------------------
# Page configuration
# ---------------------------------------------------------

load_dotenv()

st.set_page_config(
    page_title="Smart Mobility - Sensors",
    layout="wide",
)


# ---------------------------------------------------------
# Athena connection
# ---------------------------------------------------------

@st.cache_resource
def cursor():
    return connect(
        s3_staging_dir=f"s3://{os.environ['S3_BUCKET']}/athena-results/",
        region_name=os.getenv("AWS_REGION", "ap-south-1"),
        schema_name="mobility_gold",
        cursor_class=PandasCursor,
    ).cursor()


@st.cache_data(ttl=3600)
def q(sql: str):
    return cursor().execute(sql).as_pandas()


# ---------------------------------------------------------
# Header
# ---------------------------------------------------------

st.title("📡 Smart Mobility Sensor Dashboard")
st.caption("Sensor analytics from the Gold layer in Amazon Athena")


# ---------------------------------------------------------
# KPI section
# ---------------------------------------------------------

kpi = q("""
    select
        count(*) as hourly_records,
        count(distinct sensor_id) as sensors,
        count(distinct sensor_type) as sensor_types,
        count(distinct zone_id) as zones
    from fact_sensor_hourly
""")

c1, c2, c3, c4 = st.columns(4)

c1.metric(
    "Hourly Records",
    f"{kpi['hourly_records'].iloc[0]:,}",
)

c2.metric(
    "Sensors",
    f"{kpi['sensors'].iloc[0]:,}",
)

c3.metric(
    "Sensor Types",
    f"{kpi['sensor_types'].iloc[0]:,}",
)

c4.metric(
    "Zones",
    f"{kpi['zones'].iloc[0]:,}",
)


st.divider()


# ---------------------------------------------------------
# Sensor type overview
# ---------------------------------------------------------

left, right = st.columns(2)

with left:
    st.subheader("Sensors by Type")

    sensor_types = q("""
        select
            sensor_type,
            count(distinct sensor_id) as sensor_count
        from fact_sensor_hourly
        group by sensor_type
        order by sensor_count desc
    """)

    st.bar_chart(
        sensor_types,
        x="sensor_type",
        y="sensor_count",
    )


with right:
    st.subheader("Hourly Readings")

    hourly = q("""
        select
            reading_hour,
            sum(reading_count) as readings
        from fact_sensor_hourly
        group by reading_hour
        order by reading_hour
    """)

    st.line_chart(
        hourly,
        x="reading_hour",
        y="readings",
    )


st.divider()


# ---------------------------------------------------------
# Sensor selection
# ---------------------------------------------------------

st.subheader("Sensor Analysis")

sensors = q("""
    select distinct sensor_id
    from fact_sensor_hourly
    order by sensor_id
""")["sensor_id"].tolist()

if not sensors:
    st.warning("No sensor data is available.")
    st.stop()

sensor = st.selectbox(
    "Select Sensor",
    sensors,
)


# ---------------------------------------------------------
# Selected sensor information
# ---------------------------------------------------------

sensor_info = q(f"""
    select distinct
        sensor_id,
        sensor_type,
        zone_id
    from fact_sensor_hourly
    where sensor_id = '{sensor}'
    limit 1
""")

if not sensor_info.empty:

    sensor_type = sensor_info["sensor_type"].iloc[0]
    zone_id = sensor_info["zone_id"].iloc[0]

    c1, c2, c3 = st.columns(3)

    c1.metric(
        "Sensor ID",
        sensor,
    )

    c2.metric(
        "Sensor Type",
        sensor_type,
    )

    c3.metric(
        "Zone",
        str(zone_id),
    )


# ---------------------------------------------------------
# Selected sensor statistics
# ---------------------------------------------------------

stats = q(f"""
    select
        round(avg(avg_value), 2) as overall_avg,
        round(min(min_value), 2) as overall_min,
        round(max(max_value), 2) as overall_max,
        sum(reading_count) as total_readings
    from fact_sensor_hourly
    where sensor_id = '{sensor}'
""")

c1, c2, c3, c4 = st.columns(4)

c1.metric(
    "Average",
    f"{stats['overall_avg'].iloc[0]:.2f}",
)

c2.metric(
    "Minimum",
    f"{stats['overall_min'].iloc[0]:.2f}",
)

c3.metric(
    "Maximum",
    f"{stats['overall_max'].iloc[0]:.2f}",
)

c4.metric(
    "Total Readings",
    f"{stats['total_readings'].iloc[0]:,}",
)


st.divider()


# ---------------------------------------------------------
# Hourly trend
# ---------------------------------------------------------

st.subheader("Hourly Sensor Trend")

sensor_data = q(f"""
    select
        reading_hour,
        reading_count,
        avg_value,
        min_value,
        max_value,
        stddev_value,
        zone_id
    from fact_sensor_hourly
    where sensor_id = '{sensor}'
    order by reading_hour
""")

if sensor_data.empty:
    st.info("No readings available for this sensor.")
else:

    st.line_chart(
        sensor_data,
        x="reading_hour",
        y="avg_value",
    )

    st.caption(
        f"Hourly average for {sensor} ({sensor_type})"
    )


# ---------------------------------------------------------
# Sensor reading statistics
# ---------------------------------------------------------

st.subheader("Hourly Statistics")

st.dataframe(
    sensor_data,
    use_container_width=True,
    hide_index=True,
)


st.divider()


# ---------------------------------------------------------
# SCD Type 2 sensor history
# ---------------------------------------------------------

st.subheader("Sensor Configuration History")
st.caption(
    "Historical sensor configurations are maintained using "
    "Slowly Changing Dimension Type 2."
)

history = q(f"""
    select
        sensor_id,
        zone_id,
        firmware_version,
        valid_from,
        valid_to,
        is_current
    from dim_sensor
    where sensor_id = '{sensor}'
    order by valid_from
""")

if history.empty:
    st.info("No historical configuration found.")
else:
    st.dataframe(
        history,
        use_container_width=True,
        hide_index=True,
    )


# ---------------------------------------------------------
# Current sensor configuration
# ---------------------------------------------------------

st.subheader("Current Configuration")

current = q(f"""
    select
        sensor_id,
        sensor_type,
        zone_id,
        firmware_version,
        valid_from
    from dim_sensor
    where sensor_id = '{sensor}'
      and is_current = true
""")

if not current.empty:

    c1, c2, c3, c4 = st.columns(4)

    c1.metric(
        "Sensor",
        current["sensor_id"].iloc[0],
    )

    c2.metric(
        "Type",
        current["sensor_type"].iloc[0],
    )

    c3.metric(
        "Zone",
        str(current["zone_id"].iloc[0]),
    )

    c4.metric(
        "Firmware",
        current["firmware_version"].iloc[0],
    )


# ---------------------------------------------------------
# Footer
# ---------------------------------------------------------

st.divider()

st.caption(
    "Smart Mobility Data Platform • "
    "Streaming Sensors → Spark → S3 → Glue → Athena → dbt"
)


