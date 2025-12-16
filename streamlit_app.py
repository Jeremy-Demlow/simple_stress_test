# All Credit to Tyler Gaede for the original code and project set up for locust https://snow.gitlab-dedicated.com/snowflakecorp/SE/sales-engineering/hybrid-table-performance-explorer/-/blob/main/app.py?ref_type=heads

import pandas as pd
import plotly.express as px
import streamlit as st
import os
from hybrid_tables.connection import SnowparkConnection
from snowflake.snowpark.version import VERSION

st.set_page_config(page_title="Load Test Results", layout="wide")

# Initialize session state
if 'snowpark_connection' not in st.session_state:
    st.session_state.snowpark_connection = None

def create_connection_config():
    st.sidebar.header("Snowflake Connection Configuration")

    connection_config = {
        'account': st.sidebar.text_input("Snowflake Account"),
        'user': st.sidebar.text_input("Username"),
        'password': st.sidebar.text_input("Password", type="password"),
        'role': st.sidebar.text_input("Role", value="HYBRID_USER_ROLE"),
        'warehouse': st.sidebar.text_input("Warehouse", value="HYBRID_WH"),
        'database': st.sidebar.text_input("Database", value="HYBRID_DB"),
        'schema': st.sidebar.text_input("Schema", value="SILVER")
    }

    return connection_config

def display_connection_info(session, config):
    snowflake_environment = session.sql('SELECT current_user(), current_version()').collect()
    snowpark_version = VERSION

    st.sidebar.success("Connection Established")
    with st.sidebar.expander("Connection Details"):
        st.write(f"User: {snowflake_environment[0][0]}")
        st.write(f"Role: {config['role']}")
        st.write(f"Database: {config['database']}")
        st.write(f"Schema: {config['schema']}")
        st.write(f"Warehouse: {config['warehouse']}")
        st.write(f"Snowflake version: {snowflake_environment[0][1]}")
        st.write(f"Snowpark for Python version: {snowpark_version[0]}.{snowpark_version[1]}.{snowpark_version[2]}")

# Establish Snowflake connection
if st.session_state.snowpark_connection is None:
    try:
        # Use default SIS connection parameters
        connection_config = {
            'user': os.getenv('SNOWFLAKE_USER', ''),
            'password': os.getenv('SNOWFLAKE_PASSWORD', ''),
            'account': os.getenv('SNOWFLAKE_ACCOUNT', ''),
            'database': 'HYBRID_DB',
            'warehouse': 'HYBRID_WH',
            'schema': 'SILVER',
            'role': 'HYBRID_USER_ROLE'
        }

        connection = SnowparkConnection(connection_config=connection_config)
        session = connection.get_session()

        st.session_state.connection_config = connection_config
        st.session_state.snowpark_connection = connection
        display_connection_info(session, connection_config)

    except Exception as e:
        st.sidebar.error(f"Failed to establish connection using default SIS parameters: {str(e)}")
        st.sidebar.warning("Please provide connection details:")

        user_config = create_connection_config()

        if st.sidebar.button("Connect"):
            try:
                connection = SnowparkConnection(connection_config=user_config)
                session = connection.get_session()

                st.session_state.snowpark_connection = connection
                st.session_state.connection_config = user_config
                display_connection_info(session, user_config)
            except Exception as e:
                st.sidebar.error(f"Failed to establish connection with provided details: {str(e)}")

# SQL Queries
_load_test_list_query = """
select aqh.query_tag as load_test_name
    , sum(aqh.calls) as total_query_count
    , datediff(minutes, min(aqh.interval_start_time), max(aqh.interval_start_time)) as test_duration_minutes
from snowflake.account_usage.aggregate_query_history aqh
where TRUE
    and startswith(aqh.query_tag, 'LOADTEST ')
group by load_test_name
order by load_test_name desc
;"""

_load_test_data_query = """
SELECT
    interval_start_time
    , SUM(calls) / 60 as QPS
    , SUM(total_elapsed_time:"PERCENTILE"::number * calls) / SUM(calls) as  total_elapsed_time
    , SUM(percentage_scanned_from_cache:"PERCENTILE"::number * calls) / SUM(calls)  percentage_scanned_from_cache
    , SUM(compilation_time:"PERCENTILE"::number * calls) / SUM(calls) as   compilation_time
    , SUM(queued_provisioning_time:"PERCENTILE"::number * calls) / SUM(calls) as   queued_provisioning_time
    , SUM(queued_overload_time:"PERCENTILE"::number * calls) / SUM(calls) as   queued_overload_time
    , SUM(queued_repair_time:"PERCENTILE"::number * calls) / SUM(calls) as   queued_repair_time
    , SUM(transaction_blocked_time:"PERCENTILE"::number * calls) / SUM(calls) as   transaction_blocked_time
    , SUM(execution_time:"PERCENTILE"::number * calls) / SUM(calls) as   execution_time
    , SUM(hybrid_table_requests_throttled_count) as hybrid_table_requests_throttled_count
    , CASE
        WHEN LOWER(query_text) LIKE '%select%' THEN 'read'
        WHEN LOWER(query_text) LIKE '%insert%' OR LOWER(query_text) LIKE '%merge%' OR
        LOWER(query_text) LIKE '%update%' OR LOWER(query_text) LIKE '%delete%' THEN 'write'
        ELSE 'other'
      END as query_type
    , query_text
FROM snowflake.account_usage.aggregate_query_history
WHERE TRUE
    and query_tag = ?
group by all
ORDER BY interval_start_time DESC
;"""


_load_test_summary_query = """
WITH times AS (
    SELECT
        MIN(interval_start_time) as min_start_time,
        MAX(interval_start_time) as max_end_time
    FROM snowflake.account_usage.aggregate_query_history
    WHERE query_tag = ?
),
filtered_data AS (
    SELECT
        CASE
            WHEN LOWER(query_text) LIKE '%select%' THEN 'read'
            WHEN LOWER(query_text) LIKE '%insert%' OR LOWER(query_text) LIKE '%merge%' OR
            LOWER(query_text) LIKE '%update%' OR LOWER(query_text) LIKE '%delete%' THEN 'write'
            ELSE 'other'
        END as query_type,
        calls,
        total_elapsed_time,
        interval_start_time
    FROM snowflake.account_usage.aggregate_query_history
    JOIN times ON
        interval_start_time >= DATEADD(minute, 5, min_start_time) AND
        interval_start_time <= DATEADD(minute, -1, max_end_time)
    WHERE query_tag = ?
)
SELECT
    query_type,
    SUM(calls) / DATEDIFF(second, MIN(interval_start_time), MAX(interval_start_time)) as average_qps,
    AVG(total_elapsed_time:"PERCENTILE"::number) as average_total_elapsed_time
FROM filtered_data
GROUP BY query_type
ORDER BY average_qps DESC
;"""


_cluster_count_query = """
with test_times as
(
    SELECT warehouse_name
    , min(interval_end_time) as test_start
    , max(interval_end_time) as test_end
    FROM snowflake.account_usage.aggregate_query_history l
    WHERE TRUE
        and query_tag = ?
    group by all
    limit 1
)
, wh_resume_time as
(
    select timestamp as resume_time
    from snowflake.account_usage.warehouse_events_history weh
    inner join test_times on test_times.warehouse_name = weh.warehouse_name and weh.timestamp < test_times.test_start
    where EVENT_NAME = 'RESUME_WAREHOUSE'
    order by timestamp desc
    limit 1
)
, cluster_counts as
(
    select timestamp as CC_START
        , lead(timestamp) over (order by timestamp asc) as CC_END
        , iff(event_name = 'RESUME_CLUSTER', 1, -1) as CLUSTER_CHANGE_COUNT
        , sum(CLUSTER_CHANGE_COUNT) over (order by timestamp asc) as CLUSTER_COUNT
    from snowflake.account_usage.warehouse_events_history weh
    inner join wh_resume_time on weh.timestamp >= wh_resume_time.resume_time
    inner join test_times on weh.timestamp < test_times.test_end and weh.warehouse_name = test_times.warehouse_name
    where TRUE
    and event_state = 'COMPLETED'
    and event_name in ('RESUME_CLUSTER', 'SUSPEND_CLUSTER')
    order by cc_start asc
)

select l.interval_end_time
, cluster_counts.cluster_count
FROM snowflake.account_usage.aggregate_query_history l
inner join cluster_counts on l.interval_end_time >= cluster_counts.cc_start and l.interval_end_time <= cluster_counts.cc_end
inner join test_times on l.interval_end_time >= test_times.test_start and l.interval_end_time <= test_times.test_end and l.warehouse_name = test_times.warehouse_name
group by all
order by l.interval_end_time asc
;"""

def fetch_data(query: str, params: list = None) -> pd.DataFrame:
    try:
        df = st.session_state.snowpark_connection.get_session().sql(query, params).to_pandas()
    except Exception as e:
        st.error(f"Query error: {e}")
        df = None
    return df

def plot_time_series(df, x, y, color, title):
    fig = px.line(data_frame=df, x=x, y=y, color=color, title=title)
    fig.update_layout(legend_title_text='Query Type')
    st.plotly_chart(fig, use_container_width=True)

def plot_stacked_bar(df, x, y, title):
    fig = px.bar(data_frame=df, x=x, y=y, title=title)
    fig.update_layout(barmode='stack', xaxis_title='Time', yaxis_title='Duration (ms)')
    st.plotly_chart(fig, use_container_width=True)

def main():
    st.title("Load Test Results Analysis")

    if st.session_state.snowpark_connection is None:
        st.warning("Please connect to Snowflake using the sidebar.")
        return

    load_test_list = fetch_data(_load_test_list_query)

    if load_test_list is None or load_test_list.empty:
        st.warning("No load tests found. Please check your connection and data.")
        return

    st.header("Load Test Runs")
    st.dataframe(data=load_test_list, use_container_width=True, hide_index=True)

    load_test_names = load_test_list["LOAD_TEST_NAME"].tolist()
    load_test_names.insert(0, "Select a load test")
    load_test = st.selectbox(label="Choose a load test", options=load_test_names)

    if load_test != "Select a load test":
        percentile = st.selectbox(label="Use Percentile", options=["p90", "p99"])

        aggregate_usage_df = fetch_data(_load_test_data_query.replace("PERCENTILE", percentile), [load_test])

        if aggregate_usage_df is None or aggregate_usage_df.empty:
            st.warning("No data available for the selected load test.")
            return

        st.header("Performance Metrics")

        col1, col2 = st.columns(2)
        with col1:
            plot_time_series(aggregate_usage_df, "INTERVAL_START_TIME", "QPS", "QUERY_TYPE", "Queries Per Second")
        with col2:
            plot_time_series(aggregate_usage_df, "INTERVAL_START_TIME", "EXECUTION_TIME", "QUERY_TYPE", "Execution Time")

        st.subheader("Breakdown of Total Execution Time")
        time_columns = [
            "COMPILATION_TIME", "QUEUED_PROVISIONING_TIME", "QUEUED_OVERLOAD_TIME",
            "QUEUED_REPAIR_TIME", "TRANSACTION_BLOCKED_TIME", "EXECUTION_TIME"
        ]
        plot_stacked_bar(aggregate_usage_df, "INTERVAL_START_TIME", time_columns, "Components of Query Execution Time")

        col3, col4 = st.columns(2)
        with col3:
            plot_time_series(aggregate_usage_df, "INTERVAL_START_TIME", "HYBRID_TABLE_REQUESTS_THROTTLED_COUNT", "QUERY_TYPE", "Hybrid Table Requests Throttled Count")
        with col4:
            cluster_count_df = fetch_data(_cluster_count_query, [load_test])
            if cluster_count_df is not None and not cluster_count_df.empty:
                plot_time_series(cluster_count_df, "INTERVAL_END_TIME", "CLUSTER_COUNT", None, "Cluster Count Over Time")
            else:
                st.warning("No cluster count data available.")

        st.header("Summary Statistics")
        st.write("Note: Automatically excludes first 5 minutes and last minute of data")
        summary_df = fetch_data(_load_test_summary_query.replace("PERCENTILE", percentile), [load_test, load_test])
        if summary_df is not None and not summary_df.empty:
            st.dataframe(summary_df, hide_index=True, use_container_width=True)
        else:
            st.warning("No summary data available.")

        with st.expander("Show SQL Queries"):
            st.subheader("Load Test List Query")
            st.code(body=_load_test_list_query, language="sql")
            st.subheader("Load Test Data Query")
            st.code(body=_load_test_data_query.replace("PERCENTILE", percentile).replace("?", f"'{load_test}'"), language="sql")
            st.subheader("Cluster Count Query")
            st.code(body=_cluster_count_query.replace("?", f"'{load_test}'"), language="sql")
            st.subheader("Load Test Summary Query")
            st.code(body=_load_test_summary_query.replace("PERCENTILE", percentile).replace("?", f"'{load_test}'"), language="sql")

if __name__ == "__main__":
    main()
