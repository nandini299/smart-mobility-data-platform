"""NYC Taxi analytics dashboard on the mobility_gold star schema (Amazon Athena).

Run:
    streamlit run dashboard/mobility/app.py

Needs S3_BUCKET in the environment or .env; AWS_REGION and ATHENA_WORKGROUP
are optional.

Cost notes:
- Every fact query filters on pickup_month, the partition column of
  fact_trips, so Athena only scans the months being viewed.
- Results are cached for an hour, so repeating a filter combination is free.
- Queries use bind parameters (pyformat) instead of string formatting.
- Each query starts with a "-- dashboard: <name>" comment so it can be found
  in the Athena query history.
"""

import os

import altair as alt
import pandas as pd
import streamlit as st
from dotenv import load_dotenv

load_dotenv()

st.set_page_config(page_title="NYC Taxi Analytics", page_icon="🚕", layout="wide")

DAY_NAMES = {1: "Mon", 2: "Tue", 3: "Wed", 4: "Thu", 5: "Fri", 6: "Sat", 7: "Sun"}
DAY_ORDER = list(DAY_NAMES.values())

CARD_PAYMENT_TYPE_ID = 1  # TLC only records tips for card payments


# ---------------------------------------------------------
# Athena access
# ---------------------------------------------------------

@st.cache_resource
def get_connection():
    from pyathena import connect
    from pyathena.pandas.cursor import PandasCursor

    return connect(
        s3_staging_dir=f"s3://{os.environ['S3_BUCKET']}/athena-results/",
        region_name=os.getenv("AWS_REGION", "ap-south-1"),
        work_group=os.getenv("ATHENA_WORKGROUP", "primary"),
        cursor_class=PandasCursor,
    )


@st.cache_data(ttl=3600, show_spinner="Querying Athena...")
def run_query(sql: str, params: dict | None = None) -> pd.DataFrame:
    # One cursor per query: cursors are not safe to share between sessions.
    return get_connection().cursor().execute(sql, params or None).as_pandas()


# ---------------------------------------------------------
# SQL building blocks
# ---------------------------------------------------------

FACT_FROM = """
FROM mobility_gold.fact_trips f
LEFT JOIN mobility_gold.dim_zone pz ON f.pickup_zone_id = pz.zone_id
"""


def in_clause(column: str, prefix: str, values: list) -> tuple[str, dict]:
    """Build `column IN (%(p0)s, %(p1)s, ...)` with one bind parameter per value."""

    names = [f"{prefix}{i}" for i in range(len(values))]
    placeholders = ", ".join(f"%({name})s" for name in names)
    return f"{column} IN ({placeholders})", dict(zip(names, values))


def build_where(month_start, month_end, boroughs, payment_ids) -> tuple[str, dict]:
    clauses = ["f.pickup_month BETWEEN %(month_start)s AND %(month_end)s"]
    params = {"month_start": month_start, "month_end": month_end}

    if boroughs:
        clause, extra = in_clause("pz.borough", "borough", boroughs)
        clauses.append(clause)
        params.update(extra)

    if payment_ids:
        clause, extra = in_clause("f.payment_type_id", "payment", payment_ids)
        clauses.append(clause)
        params.update(extra)

    return "WHERE " + "\n  AND ".join(clauses), params


# ---------------------------------------------------------
# Queries
# ---------------------------------------------------------

def load_months() -> list[str]:
    df = run_query("""
-- dashboard: months
SELECT DISTINCT pickup_month
FROM mobility_gold.fact_trips
ORDER BY pickup_month
""")
    return df["pickup_month"].tolist()


def load_boroughs() -> list[str]:
    df = run_query("""
-- dashboard: boroughs
SELECT DISTINCT borough
FROM mobility_gold.dim_zone
WHERE borough IS NOT NULL
ORDER BY borough
""")
    return df["borough"].tolist()


def load_payment_types() -> pd.DataFrame:
    return run_query("""
-- dashboard: payment_types
SELECT payment_type_id, payment_type_name
FROM mobility_gold.dim_payment_type
ORDER BY payment_type_id
""")


def load_kpis(where, params) -> pd.Series:
    df = run_query(f"""
-- dashboard: kpis
SELECT
    count(*)                    AS trips,
    sum(f.total_amount)         AS revenue,
    avg(f.total_amount)         AS avg_fare,
    avg(f.trip_distance)        AS avg_distance,
    avg(f.trip_minutes)         AS avg_minutes,
    sum(f.trip_distance) / nullif(sum(f.trip_minutes) / 60.0, 0) AS avg_mph,
    sum(CASE WHEN f.payment_type_id = {CARD_PAYMENT_TYPE_ID} THEN f.tip_amount END)
      / nullif(sum(CASE WHEN f.payment_type_id = {CARD_PAYMENT_TYPE_ID}
                        THEN f.fare_amount END), 0) AS card_tip_rate,
    max(f.pickup_ts)            AS latest_pickup
{FACT_FROM}
{where}
""", params)
    return df.iloc[0]


def load_monthly(where, params) -> pd.DataFrame:
    return run_query(f"""
-- dashboard: monthly
SELECT
    f.pickup_month,
    count(*)             AS trips,
    sum(f.total_amount)  AS revenue,
    avg(f.total_amount)  AS avg_fare
{FACT_FROM}
{where}
GROUP BY f.pickup_month
ORDER BY f.pickup_month
""", params)


def load_daily(where, params) -> pd.DataFrame:
    return run_query(f"""
-- dashboard: daily
SELECT
    cast(f.pickup_ts AS date)  AS pickup_day,
    count(*)                   AS trips
{FACT_FROM}
{where}
GROUP BY cast(f.pickup_ts AS date)
ORDER BY pickup_day
""", params)


def load_hour_of_week(where, params) -> pd.DataFrame:
    return run_query(f"""
-- dashboard: hour_of_week
SELECT
    day_of_week(f.pickup_ts)  AS dow,
    hour(f.pickup_ts)         AS hour,
    count(*)                  AS trips,
    sum(f.trip_distance) / nullif(sum(f.trip_minutes) / 60.0, 0) AS avg_mph
{FACT_FROM}
{where}
GROUP BY day_of_week(f.pickup_ts), hour(f.pickup_ts)
""", params)


def load_top_zones(where, params) -> pd.DataFrame:
    return run_query(f"""
-- dashboard: top_zones
SELECT
    coalesce(pz.zone_name, 'Unknown')  AS zone,
    coalesce(pz.borough, 'Unknown')    AS borough,
    count(*)                           AS trips,
    sum(f.total_amount)                AS revenue,
    avg(f.total_amount)                AS avg_fare
{FACT_FROM}
{where}
GROUP BY coalesce(pz.zone_name, 'Unknown'), coalesce(pz.borough, 'Unknown')
ORDER BY trips DESC
LIMIT 20
""", params)


def load_boroughs_share(where, params) -> pd.DataFrame:
    return run_query(f"""
-- dashboard: borough_share
SELECT
    coalesce(pz.borough, 'Unknown')  AS borough,
    count(*)                         AS trips,
    sum(f.total_amount)              AS revenue
{FACT_FROM}
{where}
GROUP BY coalesce(pz.borough, 'Unknown')
ORDER BY trips DESC
""", params)


def load_top_routes(where, params) -> pd.DataFrame:
    return run_query(f"""
-- dashboard: top_routes
SELECT
    coalesce(pz.zone_name, 'Unknown')  AS pickup_zone,
    coalesce(dz.zone_name, 'Unknown')  AS dropoff_zone,
    count(*)                           AS trips,
    avg(f.trip_minutes)                AS avg_minutes,
    avg(f.total_amount)                AS avg_fare
{FACT_FROM}
LEFT JOIN mobility_gold.dim_zone dz ON f.dropoff_zone_id = dz.zone_id
{where}
GROUP BY coalesce(pz.zone_name, 'Unknown'), coalesce(dz.zone_name, 'Unknown')
ORDER BY trips DESC
LIMIT 15
""", params)


def load_payment_mix(where, params) -> pd.DataFrame:
    return run_query(f"""
-- dashboard: payment_mix
SELECT
    coalesce(p.payment_type_name, 'Unknown')  AS payment_type,
    count(*)                                  AS trips,
    sum(f.total_amount)                       AS revenue
{FACT_FROM}
LEFT JOIN mobility_gold.dim_payment_type p ON f.payment_type_id = p.payment_type_id
{where}
GROUP BY coalesce(p.payment_type_name, 'Unknown')
ORDER BY trips DESC
""", params)


def load_tip_by_hour(where, params) -> pd.DataFrame:
    return run_query(f"""
-- dashboard: tip_by_hour
SELECT
    hour(f.pickup_ts)  AS hour,
    sum(f.tip_amount) / nullif(sum(f.fare_amount), 0)  AS tip_rate
{FACT_FROM}
{where}
  AND f.payment_type_id = {CARD_PAYMENT_TYPE_ID}
GROUP BY hour(f.pickup_ts)
ORDER BY hour
""", params)


def load_distance_buckets(where, params) -> pd.DataFrame:
    return run_query(f"""
-- dashboard: distance_buckets
SELECT
    cast(least(floor(f.trip_distance), 20) AS integer)  AS miles,
    count(*)                                             AS trips
{FACT_FROM}
{where}
GROUP BY cast(least(floor(f.trip_distance), 20) AS integer)
ORDER BY miles
""", params)


def load_data_health(month_start, month_end) -> pd.DataFrame:
    # Deliberately ignores the borough/payment filters: this checks the data, not a slice.
    return run_query("""
-- dashboard: data_health
SELECT
    pickup_month,
    count(*)  AS trips,
    avg(CASE WHEN passenger_count IS NULL THEN 1.0 ELSE 0.0 END)  AS null_passenger_share,
    avg(CASE WHEN fare_amount = 0 THEN 1.0 ELSE 0.0 END)          AS zero_fare_share,
    avg(CASE WHEN pickup_zone_id IN (264, 265) THEN 1.0 ELSE 0.0 END) AS unknown_zone_share,
    min(pickup_ts)  AS first_pickup,
    max(pickup_ts)  AS last_pickup
FROM mobility_gold.fact_trips
WHERE pickup_month BETWEEN %(month_start)s AND %(month_end)s
GROUP BY pickup_month
ORDER BY pickup_month
""", {"month_start": month_start, "month_end": month_end})


# ---------------------------------------------------------
# Formatting helpers
# ---------------------------------------------------------

def is_missing(value) -> bool:
    return value is None or pd.isna(value)


def fmt_count(value) -> str:
    if is_missing(value):
        return "–"
    value = float(value)
    if value >= 1_000_000:
        return f"{value / 1_000_000:.2f}M"
    if value >= 1_000:
        return f"{value / 1_000:.1f}K"
    return f"{value:,.0f}"


def fmt_money(value, decimals=0) -> str:
    if is_missing(value):
        return "–"
    value = float(value)
    if decimals == 0 and value >= 1_000_000:
        return f"${value / 1_000_000:.2f}M"
    return f"${value:,.{decimals}f}"


def pct_change(current, previous) -> str | None:
    if previous is None or is_missing(current) or is_missing(previous) or float(previous) == 0:
        return None
    return f"{(float(current) - float(previous)) / float(previous):+.1%}"


def kpi(column, label, key, current, previous, formatter, help_text=None):
    prev_value = None if previous is None else previous[key]
    column.metric(
        label,
        formatter(current[key]),
        delta=pct_change(current[key], prev_value),
        help=help_text,
    )


# ---------------------------------------------------------
# Sidebar filters
# ---------------------------------------------------------

if not os.getenv("S3_BUCKET"):
    st.error("S3_BUCKET is not set. Add it to your environment or .env file.")
    st.stop()

months = load_months()

if not months:
    st.warning("mobility_gold.fact_trips is empty. Run the trips_monthly DAG first.")
    st.stop()

st.sidebar.header("Filters")

month_start, month_end = st.sidebar.select_slider(
    "Pickup months",
    options=months,
    value=(months[0], months[-1]),
)

boroughs = st.sidebar.multiselect("Pickup borough", load_boroughs())

payment_types = load_payment_types()
payment_names = st.sidebar.multiselect(
    "Payment type", payment_types["payment_type_name"].tolist()
)
payment_ids = [
    int(i)
    for i in payment_types.loc[
        payment_types["payment_type_name"].isin(payment_names), "payment_type_id"
    ]
]

if st.sidebar.button("Clear cache", help="Force fresh Athena queries"):
    st.cache_data.clear()
    st.rerun()

st.sidebar.caption(
    "Every query is limited to the selected months (the partition column), "
    "so Athena only scans what you are looking at."
)

where, params = build_where(month_start, month_end, boroughs, payment_ids)

# Comparison period: the same number of months immediately before the selection.
start_idx, end_idx = months.index(month_start), months.index(month_end)
span = end_idx - start_idx + 1
previous_kpis = None
if start_idx - span >= 0:
    prev_where, prev_params = build_where(
        months[start_idx - span], months[start_idx - 1], boroughs, payment_ids
    )
    previous_kpis = load_kpis(prev_where, prev_params)


# ---------------------------------------------------------
# Header and KPIs
# ---------------------------------------------------------

current_kpis = load_kpis(where, params)

st.title("🚕 NYC Taxi Analytics")

period = month_start if month_start == month_end else f"{month_start} → {month_end}"
latest = current_kpis["latest_pickup"]
st.caption(
    f"Yellow-taxi trips · {period}"
    + (f" · latest pickup in selection: {pd.Timestamp(latest):%Y-%m-%d %H:%M}"
       if not is_missing(latest) else "")
    + (" · deltas vs. the previous " + (f"{span} months" if span > 1 else "month")
       if previous_kpis is not None else "")
)

if is_missing(current_kpis["trips"]) or int(current_kpis["trips"]) == 0:
    st.info("No trips match these filters.")
    st.stop()

c1, c2, c3, c4, c5, c6 = st.columns(6)
kpi(c1, "Trips", "trips", current_kpis, previous_kpis, fmt_count)
kpi(c2, "Revenue", "revenue", current_kpis, previous_kpis, fmt_money)
kpi(c3, "Avg fare", "avg_fare", current_kpis, previous_kpis, lambda v: fmt_money(v, 2),
    help_text="Average total_amount per trip, including tolls and surcharges.")
kpi(c4, "Avg distance", "avg_distance", current_kpis, previous_kpis,
    lambda v: "–" if is_missing(v) else f"{float(v):.2f} mi")
kpi(c5, "Avg speed", "avg_mph", current_kpis, previous_kpis,
    lambda v: "–" if is_missing(v) else f"{float(v):.1f} mph",
    help_text="Total distance ÷ total time, so long trips weigh more than short ones.")
kpi(c6, "Card tip rate", "card_tip_rate", current_kpis, previous_kpis,
    lambda v: "–" if is_missing(v) else f"{float(v):.1%}",
    help_text="Tips ÷ fares for card payments only. Cash tips are not recorded by TLC.")

st.divider()


# ---------------------------------------------------------
# Tabs
# ---------------------------------------------------------

trends_tab, when_tab, where_tab, pay_tab, health_tab, about_tab = st.tabs(
    ["📈 Trends", "🕒 When", "📍 Where", "💳 Payments & tips", "🩺 Data health", "ℹ️ About"]
)

with trends_tab:
    monthly = load_monthly(where, params)

    left, right = st.columns(2)
    with left:
        st.subheader("Trips per month")
        st.altair_chart(
            alt.Chart(monthly).mark_bar().encode(
                x=alt.X("pickup_month:O", title=None),
                y=alt.Y("trips:Q", title="Trips"),
                tooltip=["pickup_month", alt.Tooltip("trips:Q", format=",")],
            )
        )
    with right:
        st.subheader("Revenue per month")
        st.altair_chart(
            alt.Chart(monthly).mark_line(point=True).encode(
                x=alt.X("pickup_month:O", title=None),
                y=alt.Y("revenue:Q", title="Revenue ($)"),
                tooltip=["pickup_month", alt.Tooltip("revenue:Q", format="$,.0f"),
                         alt.Tooltip("avg_fare:Q", format="$.2f", title="avg fare")],
            )
        )

    st.subheader("Trips per day")
    daily = load_daily(where, params)
    daily["pickup_day"] = pd.to_datetime(daily["pickup_day"])
    daily["avg_7d"] = daily["trips"].rolling(7, min_periods=1).mean()
    base = alt.Chart(daily).encode(x=alt.X("pickup_day:T", title=None))
    st.altair_chart(
        base.mark_line(opacity=0.35).encode(
            y=alt.Y("trips:Q", title="Trips"),
            tooltip=[alt.Tooltip("pickup_day:T"), alt.Tooltip("trips:Q", format=",")],
        )
        + base.mark_line(strokeWidth=2.5).encode(
            y="avg_7d:Q",
            tooltip=[alt.Tooltip("pickup_day:T"),
                     alt.Tooltip("avg_7d:Q", format=",.0f", title="7-day avg")],
        )
    )
    st.caption("Thin line: daily trips. Thick line: 7-day rolling average, which "
               "hides the weekday/weekend cycle.")

with when_tab:
    how = load_hour_of_week(where, params)
    how["day"] = how["dow"].map(DAY_NAMES)

    busiest = how.loc[how["trips"].idxmax()]
    slowest = how.loc[how["avg_mph"].idxmin()]
    st.markdown(
        f"**Busiest hour:** {busiest['day']} {int(busiest['hour']):02d}:00 "
        f"({int(busiest['trips']):,} trips) · "
        f"**Slowest traffic:** {slowest['day']} {int(slowest['hour']):02d}:00 "
        f"({slowest['avg_mph']:.1f} mph)"
    )

    left, right = st.columns(2)
    with left:
        st.subheader("Demand by hour of week")
        st.altair_chart(
            alt.Chart(how).mark_rect().encode(
                x=alt.X("hour:O", title="Hour of day"),
                y=alt.Y("day:O", sort=DAY_ORDER, title=None),
                color=alt.Color("trips:Q", title="Trips", scale=alt.Scale(scheme="blues")),
                tooltip=["day", "hour", alt.Tooltip("trips:Q", format=",")],
            )
        )
    with right:
        st.subheader("Average speed by hour of week")
        st.altair_chart(
            alt.Chart(how).mark_rect().encode(
                x=alt.X("hour:O", title="Hour of day"),
                y=alt.Y("day:O", sort=DAY_ORDER, title=None),
                color=alt.Color("avg_mph:Q", title="mph",
                                scale=alt.Scale(scheme="redyellowgreen")),
                tooltip=["day", "hour", alt.Tooltip("avg_mph:Q", format=".1f")],
            )
        )
    st.caption("Speed is a proxy for congestion: red cells are the hours when "
               "taxis move slowest.")

with where_tab:
    left, right = st.columns([3, 2])
    with left:
        st.subheader("Top 20 pickup zones")
        zones = load_top_zones(where, params)
        st.dataframe(
            zones,
            hide_index=True,
            column_config={
                "trips": st.column_config.ProgressColumn(
                    "Trips", format="%d", min_value=0,
                    max_value=int(zones["trips"].max()) if len(zones) else 1,
                ),
                "revenue": st.column_config.NumberColumn("Revenue", format="$%.0f"),
                "avg_fare": st.column_config.NumberColumn("Avg fare", format="$%.2f"),
            },
        )
    with right:
        st.subheader("Trips by pickup borough")
        share = load_boroughs_share(where, params)
        st.altair_chart(
            alt.Chart(share).mark_bar().encode(
                x=alt.X("trips:Q", title="Trips"),
                y=alt.Y("borough:N", sort="-x", title=None),
                tooltip=["borough", alt.Tooltip("trips:Q", format=","),
                         alt.Tooltip("revenue:Q", format="$,.0f")],
            )
        )

    st.subheader("Top 15 routes")
    routes = load_top_routes(where, params)
    st.dataframe(
        routes,
        hide_index=True,
        column_config={
            "trips": st.column_config.NumberColumn("Trips", format="%d"),
            "avg_minutes": st.column_config.NumberColumn("Avg minutes", format="%.1f"),
            "avg_fare": st.column_config.NumberColumn("Avg fare", format="$%.2f"),
        },
    )

with pay_tab:
    left, right = st.columns(2)
    with left:
        st.subheader("Payment mix")
        mix = load_payment_mix(where, params)
        st.altair_chart(
            alt.Chart(mix).mark_arc(innerRadius=60).encode(
                theta="trips:Q",
                color=alt.Color("payment_type:N", title="Payment"),
                tooltip=["payment_type", alt.Tooltip("trips:Q", format=","),
                         alt.Tooltip("revenue:Q", format="$,.0f")],
            )
        )
    with right:
        st.subheader("Card tip rate by hour")
        tips = load_tip_by_hour(where, params)
        st.altair_chart(
            alt.Chart(tips).mark_line(point=True).encode(
                x=alt.X("hour:O", title="Hour of day"),
                y=alt.Y("tip_rate:Q", title="Tip ÷ fare", axis=alt.Axis(format="%")),
                tooltip=["hour", alt.Tooltip("tip_rate:Q", format=".1%")],
            )
        )
        st.caption("Card payments only: cash tips are not recorded.")

    st.subheader("Trip distance distribution")
    dist = load_distance_buckets(where, params)
    dist["label"] = dist["miles"].map(lambda m: "20+" if m >= 20 else f"{m}–{m + 1}")
    st.altair_chart(
        alt.Chart(dist).mark_bar().encode(
            x=alt.X("label:O", sort=dist["label"].tolist(), title="Miles"),
            y=alt.Y("trips:Q", title="Trips"),
            tooltip=["label", alt.Tooltip("trips:Q", format=",")],
        )
    )

with health_tab:
    st.subheader("Gold-layer health by month")
    st.caption("Ignores the borough and payment filters. Silver already removes invalid "
               "trips, so these numbers should be small and stable from month to month. "
               "A sudden jump usually means an upstream change.")
    health = load_data_health(month_start, month_end)
    share_columns = ["null_passenger_share", "zero_fare_share", "unknown_zone_share"]
    health[share_columns] = health[share_columns] * 100
    st.dataframe(
        health,
        hide_index=True,
        column_config={
            "trips": st.column_config.NumberColumn("Trips", format="%d"),
            "null_passenger_share": st.column_config.NumberColumn(
                "Null passenger_count", format="%.2f%%"),
            "zero_fare_share": st.column_config.NumberColumn("Zero fare", format="%.2f%%"),
            "unknown_zone_share": st.column_config.NumberColumn(
                "Unknown pickup zone", format="%.2f%%"),
        },
    )
    st.altair_chart(
        alt.Chart(health).mark_bar().encode(
            x=alt.X("pickup_month:O", title=None),
            y=alt.Y("trips:Q", title="Trips"),
            tooltip=["pickup_month", alt.Tooltip("trips:Q", format=",")],
        ).properties(height=220)
    )

with about_tab:
    st.markdown("""
**Pipeline**

1. Airflow downloads each monthly TLC Parquet file into the S3 **Bronze** layer.
2. A Glue (PySpark) job casts, validates and deduplicates trips into **Silver**.
3. dbt builds the **Gold** star schema in Athena: `fact_trips` with `dim_zone`,
   `dim_date` and `dim_payment_type`.
4. This dashboard queries Gold through Athena.

**Definitions**

- *Revenue*: sum of `total_amount` (fare, tolls, surcharges and tips).
- *Avg speed*: total distance ÷ total trip time.
- *Card tip rate*: tips ÷ fares for card payments; cash tips are not recorded.
- Trips are attributed to their **pickup** zone and pickup time.
""")
