import pandas as pd
import streamlit as st
from athena import run_query
from llm import generate_sql
from openai import APIError, OpenAIError
from pyathena.error import DatabaseError
from sql_validator import validate_sql

# --------------------------------------------------
# PAGE CONFIGURATION
# --------------------------------------------------

st.set_page_config(
    page_title="Smart Mobility | Sensor Intelligence",
    page_icon="📡",
    layout="wide",
    initial_sidebar_state="expanded",
)


# --------------------------------------------------
# CUSTOM CSS
# --------------------------------------------------

st.markdown(
    """
    <style>
    @import url(
        'https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&display=swap'
    );

    html, body, [class*="css"] {
        font-family: 'Inter', sans-serif;
    }

    .stApp {
        background-color: #0b1120;
        color: #e2e8f0;
    }

    [data-testid="stSidebar"] {
        background-color: #111827;
        border-right: 1px solid #263244;
    }

    [data-testid="stSidebar"] * {
        color: #e2e8f0;
    }

    .hero {
        padding: 28px 30px;
        border-radius: 18px;
        background: linear-gradient(
            120deg,
            #12243b 0%,
            #102d36 100%
        );
        border: 1px solid #244b59;
        margin-bottom: 24px;
    }

    .hero-label {
        color: #5eead4;
        font-size: 12px;
        font-weight: 700;
        letter-spacing: 2px;
        text-transform: uppercase;
    }

    .hero-title {
        color: #f8fafc;
        font-size: 34px;
        font-weight: 800;
        margin-top: 10px;
        margin-bottom: 8px;
    }

    .hero-description {
        color: #a7b8cc;
        font-size: 14px;
        line-height: 1.7;
    }

    .section-title {
        color: #f1f5f9;
        font-size: 19px;
        font-weight: 700;
        margin-top: 20px;
        margin-bottom: 14px;
    }

    .metric-card {
        background: #111c2e;
        border: 1px solid #263449;
        border-radius: 14px;
        padding: 20px;
        min-height: 120px;
    }

    .metric-label {
        color: #94a3b8;
        font-size: 12px;
        font-weight: 600;
        text-transform: uppercase;
        letter-spacing: 1px;
    }

    .metric-value {
        color: #f8fafc;
        font-size: 29px;
        font-weight: 800;
        margin-top: 12px;
    }

    .metric-caption {
        color: #64748b;
        font-size: 12px;
        margin-top: 6px;
    }

    .panel {
        background: #111c2e;
        border: 1px solid #263449;
        border-radius: 14px;
        padding: 20px;
    }

    .assistant-panel {
        background: #111c2e;
        border: 1px solid #244b59;
        border-radius: 16px;
        padding: 24px;
        margin-top: 12px;
        margin-bottom: 16px;
    }

    .assistant-heading {
        color: #5eead4;
        font-size: 12px;
        font-weight: 700;
        letter-spacing: 1.5px;
    }

    .assistant-title {
        color: #f8fafc;
        font-size: 24px;
        font-weight: 800;
        margin-top: 8px;
    }

    .assistant-description {
        color: #94a3b8;
        font-size: 13px;
        margin-top: 8px;
        margin-bottom: 18px;
    }

    div.stButton > button {
        border-radius: 9px;
        font-weight: 600;
        min-height: 42px;
        border: 1px solid #334155;
    }

    div.stButton > button[kind="primary"] {
        background: #0d9488;
        color: white;
        border: none;
    }

    div.stButton > button[kind="primary"]:hover {
        background: #0f766e;
        color: white;
    }

    [data-testid="stDataFrame"] {
        border: 1px solid #263449;
        border-radius: 10px;
        overflow: hidden;
    }

    div[data-baseweb="input"] input {
        background-color: #0b1120;
    }

    div[data-baseweb="textarea"] textarea {
        background-color: #0b1120;
    }

    hr {
        border-color: #263449;
    }
    </style>
    """,
    unsafe_allow_html=True,
)


# --------------------------------------------------
# SESSION STATE
# --------------------------------------------------

if "last_question" not in st.session_state:
    st.session_state.last_question = ""

if "last_sql" not in st.session_state:
    st.session_state.last_sql = ""

if "last_df" not in st.session_state:
    st.session_state.last_df = pd.DataFrame()

if "last_error" not in st.session_state:
    st.session_state.last_error = ""


# --------------------------------------------------
# SIDEBAR
# --------------------------------------------------

with st.sidebar:
    st.markdown("## 📡 Smart Mobility")

    st.caption("DATA INTELLIGENCE PLATFORM")

    st.divider()

    page = st.radio(
        "NAVIGATION",
        [
            "Overview",
            "Sensor AI Assistant",
            "About this platform",
        ],
        index=0,
    )

    st.divider()

    st.markdown("### Data warehouse")

    st.markdown(
        """
        **Database**

        `mobility_gold`

        **Fact table**

        `fact_sensor_hourly`

        **Dimension table**

        `dim_sensor`
        """
    )

    st.divider()

    st.markdown("### Technology")

    st.caption(
        "Python · Kafka · Spark · AWS S3 · "
        "Glue · Athena · dbt · Streamlit"
    )


# --------------------------------------------------
# OVERVIEW PAGE
# --------------------------------------------------

if page == "Overview":

    st.markdown(
        """
        <div class="hero">
            <div class="hero-label">
                SMART MOBILITY · LIVE ANALYTICS
            </div>
            <div class="hero-title">
                Sensor Operations Overview
            </div>
            <div class="hero-description">
                Monitor your sensor network, explore aggregated
                readings, and track the freshness of data stored
                in your Amazon Athena Gold warehouse.
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    # Refresh dashboard data on demand.
    refresh_col, info_col = st.columns([1, 5])

    with refresh_col:
        refresh_dashboard = st.button(
            "↻ Refresh data",
            type="primary",
            use_container_width=True,
        )

    with info_col:
        st.caption(
            "Metrics are queried from mobility_gold in Amazon Athena."
        )

    if refresh_dashboard:
        st.cache_data.clear()




    # --------------------------------------------------
    # LOAD OVERVIEW DATA
    # --------------------------------------------------

    @st.cache_data(ttl=300, show_spinner=False)
    def load_overview_data():

        kpi_sql = """
            SELECT
                COUNT(DISTINCT sensor_id) AS total_sensors,
                COUNT(DISTINCT zone_id) AS total_zones,
                COUNT(*) AS hourly_records,
                COALESCE(SUM(reading_count), 0) AS total_readings,
                MAX(reading_hour) AS latest_reading_hour
            FROM mobility_gold.fact_sensor_hourly
        """

        type_sql = """
            SELECT
                sensor_type,
                COUNT(DISTINCT sensor_id) AS sensors,
                SUM(reading_count) AS total_readings,
                SUM(avg_value * reading_count)
                    / NULLIF(SUM(reading_count), 0) AS average_reading
            FROM mobility_gold.fact_sensor_hourly
            GROUP BY sensor_type
            ORDER BY sensor_type
        """

        inventory_sql = """
            SELECT
                COUNT(DISTINCT sensor_id) AS total_sensors,
                COUNT(DISTINCT zone_id) AS total_zones,
                COUNT(DISTINCT sensor_type) AS sensor_types
            FROM mobility_gold.dim_sensor
            WHERE is_current = TRUE
        """

        return (
            run_query(kpi_sql),
            run_query(type_sql),
            run_query(inventory_sql),
        )

    try:
        with st.spinner("Loading sensor analytics from Athena..."):
            kpi_df, type_df, inventory_df = load_overview_data()

        if kpi_df.empty or inventory_df.empty:
            st.warning("Athena returned no overview metrics.")
            st.stop()

        kpi = kpi_df.iloc[0]
        inventory = inventory_df.iloc[0]

        total_sensors = int(inventory["total_sensors"] or 0)
        total_zones = int(inventory["total_zones"] or 0)
        sensor_types = int(inventory["sensor_types"] or 0)

        total_readings = int(kpi["total_readings"] or 0)
        hourly_records = int(kpi["hourly_records"] or 0)
        latest_reading = kpi["latest_reading_hour"]

        # --------------------------------------------------
        # KPI CARDS
        # --------------------------------------------------

        st.markdown(
            '<div class="section-title">Network at a glance</div>',
            unsafe_allow_html=True,
        )

        def metric_card(label, value, caption):
            st.markdown(
                f"""
                <div class="metric-card">
                    <div class="metric-label">{label}</div>
                    <div class="metric-value">{value}</div>
                    <div class="metric-caption">{caption}</div>
                </div>
                """,
                unsafe_allow_html=True,
            )

        c1, c2, c3, c4 = st.columns(4)

        with c1:
            metric_card(
                "Current sensors",
                f"{total_sensors:,}",
                "Sensors in the current inventory",
            )

        with c2:
            metric_card(
                "Monitored zones",
                f"{total_zones:,}",
                "Zones represented in the current inventory",
            )

        with c3:
            metric_card(
                "Total readings",
                f"{total_readings:,}",
                "Readings represented in hourly aggregates",
            )

        with c4:
            metric_card(
                "Hourly records",
                f"{hourly_records:,}",
                f"{sensor_types} sensor types",
            )

        # --------------------------------------------------
        # DATA FRESHNESS
        # --------------------------------------------------

        st.markdown(
            '<div class="section-title">Data freshness</div>',
            unsafe_allow_html=True,
        )

        if pd.isna(latest_reading):
            st.warning("No sensor timestamps are available.")
        else:
            latest_ts = pd.Timestamp(latest_reading)

            # Athena timestamps are normally returned without
            # timezone information. The stored value is UTC.
            if latest_ts.tzinfo is None:
                latest_ts = latest_ts.tz_localize("UTC")
            else:
                latest_ts = latest_ts.tz_convert("UTC")

            now_utc = pd.Timestamp.now(tz="UTC")
            age_hours = (
                now_utc - latest_ts
            ).total_seconds() / 3600

            freshness_col, timestamp_col = st.columns(2)

            with freshness_col:
                if age_hours < 0:
                    st.warning(
                        "The latest reading timestamp is in the future. "
                     	   "Check the sensor timestamp or timezone."
                   )
                elif age_hours < 2:
                    st.success(
                        f"Recent data · {age_hours:.1f} hours old"
                    )
                elif age_hours < 6:
                    st.warning(
                        f"Data may be delayed · {age_hours:.1f} hours old"
                    )
                else:
                    st.error(
                        f"Stale data · {age_hours:.1f} hours old"
                    )

            with timestamp_col:
                st.metric(
                    "Latest recorded hour (UTC)",
                    latest_ts.strftime("%d %b %Y, %H:%M"),
                )

        # --------------------------------------------------
        # SENSOR TYPE CHARTS
        # --------------------------------------------------

        st.markdown(
            '<div class="section-title">Sensor analytics</div>',
            unsafe_allow_html=True,
        )

        if not type_df.empty:
            chart_left, chart_right = st.columns(2)

            with chart_left:
                st.markdown("#### Reading volume by sensor type")

                readings_chart = type_df[
                    ["sensor_type", "total_readings"]
                ].copy()

                readings_chart["total_readings"] = pd.to_numeric(
                    readings_chart["total_readings"],
                    errors="coerce",
                )

                st.bar_chart(
                    readings_chart.set_index("sensor_type")
                )

                st.caption(
                    "Total readings represented in hourly aggregates."
                )

            with chart_right:
                st.markdown("#### Average measurement by type")

                average_chart = type_df[
                    ["sensor_type", "average_reading"]
                ].copy()

                average_chart["average_reading"] = pd.to_numeric(
                    average_chart["average_reading"],
                    errors="coerce",
                )

                st.bar_chart(
                    average_chart.set_index("sensor_type")
                )

                st.caption(
                    "Reading-weighted average within each sensor type. "
                    "Units differ by sensor type."
                )

            # --------------------------------------------------
            # SENSOR SUMMARY TABLE
            # --------------------------------------------------

            st.markdown(
                '<div class="section-title">Sensor type breakdown</div>',
                unsafe_allow_html=True,
            )

            display_df = type_df.copy()

            display_df["total_readings"] = pd.to_numeric(
                display_df["total_readings"],
                errors="coerce",
            ).fillna(0).astype("int64")

            display_df["average_reading"] = pd.to_numeric(
                display_df["average_reading"],
                errors="coerce",
            ).round(2)

            display_df = display_df.rename(
                columns={
                    "sensor_type": "Sensor type",
                    "sensors": "Sensors",
                    "total_readings": "Total readings",
                    "average_reading": "Weighted average",
                }
            )

            st.dataframe(
                display_df,
                use_container_width=True,
                hide_index=True,
            )

        st.caption(
            "Overview queries are cached for up to five minutes. "
            "Use Refresh data to request updated results."
        )

    except (DatabaseError, pd.errors.DatabaseError, ValueError) as exc:
        st.error("Unable to load the sensor overview from Athena.")

        with st.expander("Technical details"):
            st.code(str(exc))

    st.stop()
# --------------------------------------------------
# HERO HEADER
# --------------------------------------------------
	
st.markdown(
    """
    <div class="hero">
        <div class="hero-label">SMART MOBILITY DATA PLATFORM</div>
        <div class="hero-title">Sensor Intelligence</div>
        <div class="hero-description">
            Explore sensor readings, investigate trends,
            and query your Gold data warehouse using natural
            language and AI-powered SQL generation.
        </div>
    </div>
    """,
    unsafe_allow_html=True,
)

# --------------------------------------------------
# KPI CARDS
# --------------------------------------------------

st.markdown(
    '<div class="section-title">Your workspace</div>',
    unsafe_allow_html=True,
)

kpi1, kpi2, kpi3, kpi4 = st.columns(4)

with kpi1:
    st.markdown(
        """
        <div class="metric-card">
            <div class="metric-label">Data warehouse</div>
            <div class="metric-value">AWS</div>
            <div class="metric-caption">Athena and S3</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

with kpi2:
    st.markdown(
        """
        <div class="metric-card">
            <div class="metric-label">AI interface</div>
            <div class="metric-value">NL → SQL</div>
            <div class="metric-caption">Natural language queries</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

with kpi3:
    st.markdown(
        """
        <div class="metric-card">
            <div class="metric-label">Analytics layer</div>
            <div class="metric-value">Gold</div>
            <div class="metric-caption">Curated dbt models</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

with kpi4:
    st.markdown(
        """
        <div class="metric-card">
            <div class="metric-label">Query engine</div>
            <div class="metric-value">Athena</div>
            <div class="metric-caption">Serverless SQL analytics</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

# --------------------------------------------------
# ABOUT PAGE
# --------------------------------------------------

if page == "About this platform":
    st.markdown("## Platform architecture")

    st.markdown(
        """
        This application is the analytics and AI layer of the
        Smart Mobility Data Platform.

        **Streaming ingestion**
        - Python sensor simulator generates sensor events.
        - Kafka transports the events.
        - Spark Structured Streaming processes the stream.
        - Amazon S3 stores the sensor data.

        **Data processing**
        - AWS Glue transforms Bronze data into Silver.
        - Athena queries the curated data.
        - dbt builds the Gold fact and dimension tables.

        **AI analytics**
        - Natural-language questions are converted into SQL.
        - SQL validation checks the generated query.
        - Athena executes accepted queries.
        - Results are displayed as tables and charts.
        """
    )

    st.info(
        "The AI assistant generates queries against the "
        "configured Gold schema. Its availability does not "
        "by itself verify that every upstream pipeline is healthy."
    )

    st.stop()




# --------------------------------------------------
# ABOUT PAGE
# --------------------------------------------------

if page == "About this platform":
    st.markdown("## Platform architecture")

    st.markdown(
        """
        This application is the analytics and AI layer of the
        Smart Mobility Data Platform.

        **Streaming ingestion**
        - Python sensor simulator generates sensor events.
        - Kafka transports the events.
        - Spark Structured Streaming processes the stream.
        - Amazon S3 stores the sensor data.

        **Data processing**
        - AWS Glue transforms Bronze data into Silver.
        - Athena queries the curated data.
        - dbt builds the Gold fact and dimension tables.

        **AI analytics**
        - Natural-language questions are converted into SQL.
        - SQL validation checks the generated query.
        - Athena executes accepted queries.
        - Results are displayed as tables and charts.
        """
    )

    st.info(
        "The AI assistant generates queries against the "
        "configured Gold schema. Its availability does not "
        "by itself verify that every upstream pipeline is healthy."
    )

    st.stop()
# --------------------------------------------------
# AI ASSISTANT
# --------------------------------------------------

st.markdown(
    """
    <div class="assistant-description">
        Ask a question in plain English. The assistant
        generates SQL, validates it, and queries Athena.
    </div>
    """,
    unsafe_allow_html=True,
)

# Suggested questions

st.markdown("#### Try a suggested question")

suggestions = [
    "Show all current sensors",
    "Show hourly readings by sensor",
    "Show the top 10 sensors by reading count",
    "Show current sensors by zone",
]

suggestion_columns = st.columns(4)

for index, suggestion in enumerate(suggestions):
    with suggestion_columns[index]:
        if st.button(
            suggestion,
            key=f"suggestion_{index}",
            use_container_width=True,
        ):
            st.session_state.selected_question = suggestion


# Question input

default_question = st.session_state.get(
    "selected_question",
    st.session_state.last_question,
)

with st.form("sensor_question_form"):
    question = st.text_input(
        "Your question",
        value=default_question,
        placeholder=(
            "Example: Show hourly readings for each sensor"
        ),
    )

    submitted = st.form_submit_button(
        "Generate insights →",
        type="primary",
        use_container_width=True,
    )


# --------------------------------------------------
# GENERATE, VALIDATE AND EXECUTE
# --------------------------------------------------

if submitted:
    if not question.strip():
        st.warning("Please enter a question.")

    else:
        st.session_state.last_question = question.strip()
        st.session_state.last_error = ""

        try:
            with st.spinner("Generating SQL with AI..."):
                sql = generate_sql(question.strip())

            st.session_state.last_sql = sql

            valid, message = validate_sql(sql)

            if not valid:
                st.session_state.last_error = (
                    f"SQL validation failed: {message}"
                )
                st.error(st.session_state.last_error)

            else:
                with st.spinner(
                    "Executing query in Amazon Athena..."
                ):
                    df = run_query(sql)

                st.session_state.last_df = df

        except (
           DatabaseError,
           pd.errors.DatabaseError,
           APIError,
           OpenAIError,
           ValueError,
           ) as exc:
            st.session_state.last_error = str(exc)
            st.error(
                "The query could not be completed. "
                "Check the error details below."
            )
            with st.expander("Technical details"):
                st.code(str(exc))


# --------------------------------------------------
# RESULTS
# --------------------------------------------------

if st.session_state.last_error:
    st.warning(st.session_state.last_error)


df = st.session_state.last_df
sql = st.session_state.last_sql


if sql:
    st.divider()

    st.markdown(
        '<div class="section-title">Query workspace</div>',
        unsafe_allow_html=True,
    )

    results_tab, chart_tab, sql_tab = st.tabs(
        [
            "📋 Results",
            "📈 Visualization",
            "⌘ Generated SQL",
        ]
    )

    with results_tab:
        st.markdown("#### Query results")

        if not df.empty:
            metric1, metric2 = st.columns(2)

            with metric1:
                st.metric(
                    "Rows returned",
                    f"{len(df):,}",
                )

            with metric2:
                st.metric(
                    "Columns",
                    f"{len(df.columns):,}",
                )

            st.dataframe(
                df,
                use_container_width=True,
                hide_index=True,
            )

            csv_data = df.to_csv(index=False).encode("utf-8")

            st.download_button(
                "Download results as CSV",
                data=csv_data,
                file_name="sensor_query_results.csv",
                mime="text/csv",
            )

        else:
            st.info(
                "The query returned no rows. "
                "Try a different question."
            )

    with chart_tab:
        st.markdown("#### Explore your results")

        if df.empty:
            st.info("Run a query that returns data to see a chart.")

        elif len(df.columns) >= 2:
            st.caption(
                "Choose the fields you want to visualize."
            )

            columns = list(df.columns)

            x_column = st.selectbox(
                "Horizontal axis",
                columns,
                index=0,
            )

            numeric_columns = [
                column
                for column in columns
                if pd.api.types.is_numeric_dtype(df[column])
            ]

            if numeric_columns:
                y_column = st.selectbox(
                    "Measure",
                    numeric_columns,
                )

                chart_df = df[[x_column, y_column]].copy()
                chart_df[y_column] = pd.to_numeric(
                    chart_df[y_column],
                    errors="coerce",
                )
                chart_df = chart_df.dropna()

                if not chart_df.empty:
                    chart_df = chart_df.set_index(x_column)
                    st.line_chart(chart_df[[y_column]])
                else:
                    st.info(
                        "No numeric values are available "
                        "for this chart."
                    )

            else:
                st.info(
                    "This result has no numeric columns to chart."
                )

        else:
            st.info(
                "Ask a question that returns at least two "
                "columns to visualize the results."
            )

    with sql_tab:
        st.markdown("#### Generated SQL")

        st.caption(
            "Review the SQL generated by the AI assistant."
        )

        st.code(sql, language="sql")

        st.caption(
            "The query passed your application's SQL validator "
            "before execution. This is not a guarantee that "
            "the query is safe in every possible context."
        )


# --------------------------------------------------
# FOOTER
# --------------------------------------------------

st.divider()

st.caption(
    "Smart Mobility Data Platform · "
    "Sensor Intelligence · Powered by Streamlit and Amazon Athena"
)
