"""
Snowflake Stress Test Dashboard

A Streamlit application to analyze and visualize stress test results
across different warehouse configurations (Gen1 vs Gen2, various sizes).

Features:
- Real-time query history analysis
- Gen1 vs Gen2 performance comparison
- Warehouse size scaling analysis
- Interactive visualizations
"""

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from snowflake_connection import SnowflakeConnection

# Page configuration
st.set_page_config(
    page_title="Snowflake Stress Test Dashboard",
    page_icon="❄️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for better styling
st.markdown("""
<style>
    .metric-card {
        background-color: #f0f2f6;
        border-radius: 10px;
        padding: 20px;
        margin: 10px 0;
    }
    .stMetric {
        background-color: #ffffff;
        border-radius: 8px;
        padding: 15px;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
    }
    .comparison-better {
        color: #28a745;
        font-weight: bold;
    }
    .comparison-worse {
        color: #dc3545;
        font-weight: bold;
    }
</style>
""", unsafe_allow_html=True)

# Constants
WAREHOUSE_PREFIX = "STRESS_TEST"
QUERY_TAG_PREFIX = "STRESS_TEST"  # Must match locustfile-snowflake.py
CREDITS_PER_HOUR = {
    "XSMALL": 1, "SMALL": 2, "MEDIUM": 4,
    "LARGE": 8, "XLARGE": 16, "2XLARGE": 32
}


# ============================================================
# Session State Management
# ============================================================

def init_session_state():
    """Initialize session state variables."""
    if 'connection' not in st.session_state:
        st.session_state.connection = None
    if 'connected' not in st.session_state:
        st.session_state.connected = False
    if 'warehouse_data' not in st.session_state:
        st.session_state.warehouse_data = None


# ============================================================
# Database Connection
# ============================================================

@st.cache_resource
def get_connection(connection_name: str):
    """Create and cache Snowflake connection."""
    try:
        conn = SnowflakeConnection.from_snow_cli(connection_name)
        return conn
    except Exception as e:
        st.error(f"Connection failed: {e}")
        return None


def connect_sidebar():
    """Render connection sidebar."""
    st.sidebar.header("❄️ Connection")

    # List available connections
    connection_name = st.sidebar.text_input(
        "Connection Name",
        value="stress_test",
        help="Name from ~/.snowflake/config.toml"
    )

    if st.sidebar.button("Connect", type="primary"):
        with st.spinner("Connecting to Snowflake..."):
            conn = get_connection(connection_name)
            if conn:
                st.session_state.connection = conn
                st.session_state.connected = True
                st.sidebar.success("✅ Connected!")

                # Show connection details
                with st.sidebar.expander("Connection Details"):
                    st.write(f"**Database:** {conn.current_database}")
                    st.write(f"**Schema:** {conn.current_schema}")
                    st.write(f"**Warehouse:** {conn.current_warehouse}")

    return st.session_state.connected


# ============================================================
# Data Queries
# ============================================================

def row_to_dict(row) -> dict:
    """Convert a Snowpark Row to a dictionary."""
    try:
        return row.as_dict()
    except AttributeError:
        # For regular Row objects, try to access by column names
        try:
            return dict(row)
        except (TypeError, ValueError):
            # Last resort: use index-based access if we know the structure
            return {str(i): v for i, v in enumerate(row)}


def get_recent_test_runs(conn, hours_back: int = 24) -> pd.DataFrame:
    """
    Get list of recent stress test runs by parsing QUERY_TAG values.

    Query tags have format: STRESS_TEST|<warehouse>|<run_id>
    """
    query = f"""
    SELECT DISTINCT
        SPLIT_PART(QUERY_TAG, '|', 3) as RUN_ID,
        SPLIT_PART(QUERY_TAG, '|', 2) as WAREHOUSE,
        MIN(START_TIME) as START_TIME,
        MAX(END_TIME) as END_TIME,
        COUNT(*) as QUERY_COUNT,
        AVG(TOTAL_ELAPSED_TIME) as AVG_LATENCY_MS,
        PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY TOTAL_ELAPSED_TIME) as P95_LATENCY_MS
    FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY(
        DATEADD('hours', -{hours_back}, CURRENT_TIMESTAMP()),
        CURRENT_TIMESTAMP(),
        RESULT_LIMIT => 10000
    ))
    WHERE QUERY_TAG LIKE '{QUERY_TAG_PREFIX}%'
      AND QUERY_TYPE = 'SELECT'
    GROUP BY 1, 2
    ORDER BY START_TIME DESC
    """
    try:
        results = conn.fetch(query)
        if results:
            return pd.DataFrame([row_to_dict(row) for row in results])
        return pd.DataFrame()
    except Exception as e:
        st.error(f"Error fetching test runs: {e}")
        return pd.DataFrame()


def get_query_history_by_tag(
    conn,
    warehouse_name: str = None,
    run_id: str = None,
    hours_back: int = 2
) -> pd.DataFrame:
    """
    Get query history filtered by query tag components.

    Args:
        conn: Snowflake connection
        warehouse_name: Filter by warehouse (optional)
        run_id: Filter by specific test run ID (optional)
        hours_back: Hours to look back
    """
    # Build tag filter
    if run_id and warehouse_name:
        tag_filter = f"QUERY_TAG = '{QUERY_TAG_PREFIX}|{warehouse_name}|{run_id}'"
    elif warehouse_name:
        tag_filter = f"QUERY_TAG LIKE '{QUERY_TAG_PREFIX}|{warehouse_name}|%'"
    elif run_id:
        tag_filter = f"QUERY_TAG LIKE '{QUERY_TAG_PREFIX}|%|{run_id}'"
    else:
        tag_filter = f"QUERY_TAG LIKE '{QUERY_TAG_PREFIX}%'"

    query = f"""
    SELECT
        QUERY_ID,
        QUERY_TEXT,
        QUERY_TAG,
        WAREHOUSE_NAME,
        EXECUTION_STATUS,
        START_TIME,
        END_TIME,
        TOTAL_ELAPSED_TIME,
        COMPILATION_TIME,
        EXECUTION_TIME,
        QUEUED_PROVISIONING_TIME,
        QUEUED_OVERLOAD_TIME,
        BYTES_SCANNED,
        ROWS_PRODUCED
    FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY(
        DATEADD('hours', -{hours_back}, CURRENT_TIMESTAMP()),
        CURRENT_TIMESTAMP(),
        RESULT_LIMIT => 10000
    ))
    WHERE {tag_filter}
      AND QUERY_TYPE = 'SELECT'
    ORDER BY START_TIME DESC
    """
    try:
        results = conn.fetch(query)
        if results:
            return pd.DataFrame([row_to_dict(row) for row in results])
        return pd.DataFrame()
    except Exception as e:
        st.error(f"Error fetching query history: {e}")
        return pd.DataFrame()


def get_test_warehouses(conn) -> pd.DataFrame:
    """Get list of stress test warehouses using SHOW WAREHOUSES."""
    query = f"SHOW WAREHOUSES LIKE '{WAREHOUSE_PREFIX}%'"
    try:
        results = conn.fetch(query)
        if results:
            rows = []
            for row in results:
                d = row.as_dict()
                # resource_constraint tells us Gen1 vs Gen2
                resource = d.get('resource_constraint', 'STANDARD')
                gen = 'Gen2' if 'GEN_2' in str(resource) else 'Gen1'
                rows.append({
                    'NAME': d.get('name', 'Unknown'),
                    'STATE': d.get('state', 'Unknown'),
                    'SIZE': d.get('size', 'Unknown'),
                    'GENERATION': gen,
                    'AUTO_SUSPEND': d.get('auto_suspend', 0),
                })
            return pd.DataFrame(rows)
        return pd.DataFrame()
    except Exception as e:
        st.error(f"Error fetching warehouses: {e}")
        return pd.DataFrame()


def test_queries(conn) -> dict:
    """
    Test all the key queries used by this dashboard and return results.
    This helps debug what's working and what's not.
    """
    results = {}

    # Test 1: Basic connectivity
    try:
        r = conn.fetch("SELECT CURRENT_USER(), CURRENT_ROLE(), CURRENT_WAREHOUSE()")
        if r:
            row = r[0]
            results['connection'] = {
                'status': 'OK',
                'user': row[0],
                'role': row[1],
                'warehouse': row[2]
            }
    except Exception as e:
        results['connection'] = {'status': 'ERROR', 'error': str(e)}

    # Test 2: SHOW WAREHOUSES
    try:
        r = conn.fetch(f"SHOW WAREHOUSES LIKE '{WAREHOUSE_PREFIX}%'")
        results['show_warehouses'] = {
            'status': 'OK',
            'count': len(r) if r else 0,
            'sample': str(r[0]) if r else 'No warehouses found'
        }
    except Exception as e:
        results['show_warehouses'] = {'status': 'ERROR', 'error': str(e)}

    # Test 3: QUERY_HISTORY access
    try:
        r = conn.fetch("""
            SELECT COUNT(*) as cnt
            FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY(
                DATEADD('hours', -1, CURRENT_TIMESTAMP()),
                CURRENT_TIMESTAMP(),
                RESULT_LIMIT => 100
            ))
        """)
        if r:
            results['query_history'] = {
                'status': 'OK',
                'recent_queries': row_to_dict(r[0]).get('CNT', 0)
            }
    except Exception as e:
        results['query_history'] = {'status': 'ERROR', 'error': str(e)}

    # Test 4: Tagged queries count
    try:
        r = conn.fetch(f"""
            SELECT COUNT(*) as cnt
            FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY(
                DATEADD('hours', -24, CURRENT_TIMESTAMP()),
                CURRENT_TIMESTAMP(),
                RESULT_LIMIT => 10000
            ))
            WHERE QUERY_TAG LIKE '{QUERY_TAG_PREFIX}%'
        """)
        if r:
            results['tagged_queries'] = {
                'status': 'OK',
                'count': row_to_dict(r[0]).get('CNT', 0)
            }
    except Exception as e:
        results['tagged_queries'] = {'status': 'ERROR', 'error': str(e)}

    return results


# ============================================================
# Visualization Components
# ============================================================

def render_metric_card(label: str, value, delta=None, delta_color="normal"):
    """Render a styled metric card."""
    st.metric(label=label, value=value, delta=delta, delta_color=delta_color)


def render_comparison_chart(gen1_data: dict, gen2_data: dict, title: str):
    """Render Gen1 vs Gen2 comparison bar chart."""
    metrics = ['avg_elapsed_ms', 'median_elapsed_ms', 'p95_elapsed_ms', 'p99_elapsed_ms']
    labels = ['Average', 'Median', 'P95', 'P99']

    gen1_values = [gen1_data.get(m, 0) or 0 for m in metrics]
    gen2_values = [gen2_data.get(m, 0) or 0 for m in metrics]

    fig = go.Figure(data=[
        go.Bar(name='Gen1', x=labels, y=gen1_values, marker_color='#636EFA'),
        go.Bar(name='Gen2', x=labels, y=gen2_values, marker_color='#00CC96')
    ])

    fig.update_layout(
        title=title,
        barmode='group',
        yaxis_title='Latency (ms)',
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        height=400
    )

    return fig


def render_time_series_chart(df: pd.DataFrame, warehouse_name: str):
    """Render time series chart for a warehouse."""
    if df.empty:
        st.warning(f"No time series data for {warehouse_name}")
        return

    fig = make_subplots(
        rows=2, cols=2,
        subplot_titles=('Query Count', 'Average Latency', 'P95 Latency', 'Queue Time')
    )

    # Query count
    fig.add_trace(
        go.Scatter(x=df['TIME_BUCKET'], y=df['QUERY_COUNT'], mode='lines', name='Queries'),
        row=1, col=1
    )

    # Average latency
    fig.add_trace(
        go.Scatter(x=df['TIME_BUCKET'], y=df['AVG_LATENCY_MS'], mode='lines', name='Avg Latency'),
        row=1, col=2
    )

    # P95 latency
    fig.add_trace(
        go.Scatter(x=df['TIME_BUCKET'], y=df['P95_LATENCY_MS'], mode='lines', name='P95 Latency'),
        row=2, col=1
    )

    # Queue time
    fig.add_trace(
        go.Scatter(x=df['TIME_BUCKET'], y=df['AVG_QUEUE_MS'], mode='lines', name='Queue Time'),
        row=2, col=2
    )

    fig.update_layout(height=500, showlegend=False, title_text=f"Metrics Over Time: {warehouse_name}")
    return fig


def render_size_comparison_chart(size_data: dict):
    """Render warehouse size comparison chart."""
    sizes = list(size_data.keys())
    avg_latencies = [size_data[s].get('avg_elapsed_ms', 0) or 0 for s in sizes]
    queries = [size_data[s].get('total_queries', 0) or 0 for s in sizes]

    fig = make_subplots(
        rows=1, cols=2,
        subplot_titles=('Average Latency by Size', 'Total Queries by Size')
    )

    fig.add_trace(
        go.Bar(x=sizes, y=avg_latencies, marker_color='#636EFA', name='Latency'),
        row=1, col=1
    )

    fig.add_trace(
        go.Bar(x=sizes, y=queries, marker_color='#00CC96', name='Queries'),
        row=1, col=2
    )

    fig.update_layout(height=400, showlegend=False)
    return fig


# ============================================================
# Main Pages
# ============================================================

def page_overview():
    """Render overview page."""
    st.header("📊 Stress Test Overview")

    if not st.session_state.connected:
        st.info("👈 Please connect to Snowflake using the sidebar")
        return

    conn = st.session_state.connection

    # Time range selector
    hours_back = st.slider("Analysis Time Range (hours)", 1, 24, 4)

    # Section 1: Test Warehouses
    st.subheader("🏭 Test Warehouses")
    warehouses_df = get_test_warehouses(conn)

    if warehouses_df.empty:
        st.warning("No stress test warehouses found. Run `python warehouse_manager.py setup stress_test` first.")
    else:
        st.dataframe(warehouses_df, use_container_width=True, hide_index=True)

    # Section 2: Recent Test Runs (using query tags)
    st.subheader("🏃 Recent Test Runs")
    st.caption(f"Queries are identified by QUERY_TAG starting with `{QUERY_TAG_PREFIX}`")

    test_runs_df = get_recent_test_runs(conn, hours_back)

    if test_runs_df.empty:
        st.info("""
        No stress test runs found in the last {hours_back} hours.

        **To run a stress test:**
        ```bash
        cd tests && locust -f locustfile-snowflake.py \\
            --connection stress_test \\
            --warehouse STRESS_TEST_XSMALL_GEN1 \\
            --users 20 --spawn-rate 2 --run-time 2m --headless
        ```

        Queries will be tagged with `STRESS_TEST|<warehouse>|<run_id>` for tracking.
        """)
    else:
        # Format the dataframe for display
        display_df = test_runs_df.copy()
        if 'AVG_LATENCY_MS' in display_df.columns:
            display_df['AVG_LATENCY_MS'] = display_df['AVG_LATENCY_MS'].round(1)
        if 'P95_LATENCY_MS' in display_df.columns:
            display_df['P95_LATENCY_MS'] = display_df['P95_LATENCY_MS'].round(1)

        st.dataframe(display_df, use_container_width=True, hide_index=True)

        # Quick stats
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Total Runs", len(test_runs_df))
        with col2:
            total_queries = test_runs_df['QUERY_COUNT'].sum() if 'QUERY_COUNT' in test_runs_df else 0
            st.metric("Total Queries", f"{total_queries:,}")
        with col3:
            avg_latency = test_runs_df['AVG_LATENCY_MS'].mean() if 'AVG_LATENCY_MS' in test_runs_df else 0
            st.metric("Avg Latency", f"{avg_latency:.1f} ms")
        with col4:
            warehouses_tested = test_runs_df['WAREHOUSE'].nunique() if 'WAREHOUSE' in test_runs_df else 0
            st.metric("Warehouses Tested", warehouses_tested)


def page_gen_comparison():
    """Render Gen1 vs Gen2 comparison page."""
    st.header("⚡ Gen1 vs Gen2 Comparison")

    if not st.session_state.connected:
        st.info("👈 Please connect to Snowflake using the sidebar")
        return

    conn = st.session_state.connection
    hours_back = st.slider("Analysis Time Range (hours)", 1, 24, 4, key="gen_hours")

    st.caption(f"Comparing stress test queries tagged with `{QUERY_TAG_PREFIX}`")

    # Size selector
    sizes = ['XSMALL', 'SMALL', 'MEDIUM']
    selected_size = st.selectbox("Select Warehouse Size", sizes)

    # Get data for Gen1 and Gen2 using query tags
    gen1_wh = f"STRESS_TEST_{selected_size}_GEN1"
    gen2_wh = f"STRESS_TEST_{selected_size}_GEN2"

    # Fetch query history filtered by query tag
    gen1_history = get_query_history_by_tag(conn, warehouse_name=gen1_wh, hours_back=hours_back)
    gen2_history = get_query_history_by_tag(conn, warehouse_name=gen2_wh, hours_back=hours_back)

    # Warning about fair comparison
    gen1_count = len(get_query_history_by_tag(conn, warehouse_name=gen1_wh, hours_back=hours_back)) if conn else 0
    gen2_count = len(get_query_history_by_tag(conn, warehouse_name=gen2_wh, hours_back=hours_back)) if conn else 0

    if gen1_count > 0 and gen2_count > 0:
        ratio = max(gen1_count, gen2_count) / max(min(gen1_count, gen2_count), 1)
        if ratio > 2:
            st.warning(f"⚠️ **Unequal test sizes**: Gen1 has {gen1_count} queries, Gen2 has {gen2_count}. Run equal tests for fair comparison.")

    col1, col2 = st.columns(2)

    with col1:
        st.subheader("🔵 Gen1 (Standard)")
        if not gen1_history.empty:
            gen1_metrics = {
                'total_queries': len(gen1_history),
                'avg_elapsed_ms': gen1_history['TOTAL_ELAPSED_TIME'].mean(),
                'median_elapsed_ms': gen1_history['TOTAL_ELAPSED_TIME'].median(),
                'p95_elapsed_ms': gen1_history['TOTAL_ELAPSED_TIME'].quantile(0.95),
                'p99_elapsed_ms': gen1_history['TOTAL_ELAPSED_TIME'].quantile(0.99),
            }
            render_metric_card("Total Queries", gen1_metrics['total_queries'])
            render_metric_card("Avg Latency", f"{gen1_metrics['avg_elapsed_ms']:.1f} ms")
            render_metric_card("P95 Latency", f"{gen1_metrics['p95_elapsed_ms']:.1f} ms")
        else:
            st.warning(f"No tagged queries found for {gen1_wh}")
            gen1_metrics = {}

    with col2:
        st.subheader("🟢 Gen2 (Standard - Next Gen)")
        if not gen2_history.empty:
            gen2_metrics = {
                'total_queries': len(gen2_history),
                'avg_elapsed_ms': gen2_history['TOTAL_ELAPSED_TIME'].mean(),
                'median_elapsed_ms': gen2_history['TOTAL_ELAPSED_TIME'].median(),
                'p95_elapsed_ms': gen2_history['TOTAL_ELAPSED_TIME'].quantile(0.95),
                'p99_elapsed_ms': gen2_history['TOTAL_ELAPSED_TIME'].quantile(0.99),
            }

            # Calculate improvement vs Gen1
            if gen1_metrics:
                gen1_avg = gen1_metrics.get('avg_elapsed_ms', 1) or 1
                gen2_avg = gen2_metrics['avg_elapsed_ms']
                improvement = ((gen1_avg - gen2_avg) / gen1_avg * 100) if gen1_avg > 0 else 0
            else:
                improvement = 0

            render_metric_card("Total Queries", gen2_metrics['total_queries'])
            render_metric_card(
                "Avg Latency",
                f"{gen2_metrics['avg_elapsed_ms']:.1f} ms",
                delta=f"{improvement:.1f}% faster" if improvement > 0 else f"{-improvement:.1f}% slower",
                delta_color="normal" if improvement > 0 else "inverse"
            )
            render_metric_card("P95 Latency", f"{gen2_metrics['p95_elapsed_ms']:.1f} ms")
        else:
            st.warning(f"No tagged queries found for {gen2_wh}")
            gen2_metrics = {}

    # Comparison chart
    if gen1_metrics and gen2_metrics:
        st.subheader("📊 Latency Comparison")
        fig = render_comparison_chart(gen1_metrics, gen2_metrics, f"{selected_size} Warehouse: Gen1 vs Gen2")
        st.plotly_chart(fig, use_container_width=True)

        # Summary comparison table
        st.subheader("📋 Summary")
        comparison_data = {
            'Metric': ['Total Queries', 'Avg Latency (ms)', 'Median (ms)', 'P95 (ms)', 'P99 (ms)'],
            'Gen1': [
                gen1_metrics['total_queries'],
                f"{gen1_metrics['avg_elapsed_ms']:.1f}",
                f"{gen1_metrics['median_elapsed_ms']:.1f}",
                f"{gen1_metrics['p95_elapsed_ms']:.1f}",
                f"{gen1_metrics['p99_elapsed_ms']:.1f}",
            ],
            'Gen2': [
                gen2_metrics['total_queries'],
                f"{gen2_metrics['avg_elapsed_ms']:.1f}",
                f"{gen2_metrics['median_elapsed_ms']:.1f}",
                f"{gen2_metrics['p95_elapsed_ms']:.1f}",
                f"{gen2_metrics['p99_elapsed_ms']:.1f}",
            ],
            'Difference': [
                f"{gen2_metrics['total_queries'] - gen1_metrics['total_queries']:+d}",
                f"{((gen1_metrics['avg_elapsed_ms'] - gen2_metrics['avg_elapsed_ms']) / gen1_metrics['avg_elapsed_ms'] * 100):+.1f}%",
                f"{((gen1_metrics['median_elapsed_ms'] - gen2_metrics['median_elapsed_ms']) / gen1_metrics['median_elapsed_ms'] * 100):+.1f}%",
                f"{((gen1_metrics['p95_elapsed_ms'] - gen2_metrics['p95_elapsed_ms']) / gen1_metrics['p95_elapsed_ms'] * 100):+.1f}%",
                f"{((gen1_metrics['p99_elapsed_ms'] - gen2_metrics['p99_elapsed_ms']) / gen1_metrics['p99_elapsed_ms'] * 100):+.1f}%",
            ]
        }
        st.dataframe(pd.DataFrame(comparison_data), use_container_width=True, hide_index=True)

    # How to run comparison tests
    with st.expander("🧪 How to Run a Fair Comparison Test"):
        st.markdown(f"""
        **For accurate comparison, run identical tests on both warehouses:**

        ```bash
        # Step 1: Run Gen1 test
        cd tests && locust -f locustfile-snowflake.py \\
            --connection stress_test \\
            --warehouse {gen1_wh} \\
            --users 20 --spawn-rate 2 --run-time 2m --headless

        # Step 2: Run Gen2 test (same parameters)
        locust -f locustfile-snowflake.py \\
            --connection stress_test \\
            --warehouse {gen2_wh} \\
            --users 20 --spawn-rate 2 --run-time 2m --headless
        ```

        **Tips for fair comparison:**
        - Use same number of users and duration
        - Run tests back-to-back to minimize time differences
        - Consider running 2-3 tests each to account for variance
        - Check that both warehouses are "warm" (resumed) before testing
        """)


def page_warehouse_details():
    """Render detailed warehouse analysis page."""
    st.header("🔍 Warehouse Details")

    if not st.session_state.connected:
        st.info("👈 Please connect to Snowflake using the sidebar")
        return

    conn = st.session_state.connection
    hours_back = st.slider("Analysis Time Range (hours)", 1, 24, 4, key="detail_hours")

    # Get warehouses
    warehouses_df = get_test_warehouses(conn)

    if warehouses_df.empty:
        # Fall back to hardcoded list
        warehouse_list = [
            "STRESS_TEST_XSMALL_GEN1", "STRESS_TEST_XSMALL_GEN2",
            "STRESS_TEST_SMALL_GEN1", "STRESS_TEST_SMALL_GEN2",
            "STRESS_TEST_MEDIUM_GEN1", "STRESS_TEST_MEDIUM_GEN2"
        ]
    else:
        warehouse_list = warehouses_df['NAME'].tolist()

    # Warehouse selector
    selected_wh = st.selectbox("Select Warehouse", warehouse_list)

    st.caption(f"Showing queries tagged with `{QUERY_TAG_PREFIX}|{selected_wh}|*`")

    # Get query history filtered by query tag
    query_history = get_query_history_by_tag(conn, warehouse_name=selected_wh, hours_back=hours_back)

    if query_history.empty:
        st.warning(f"No tagged stress test queries found for {selected_wh} in the last {hours_back} hours.")
        st.info("Run a stress test with query tagging enabled to see results here.")
        return

    # Calculate metrics from query history
    st.subheader("📈 Performance Metrics")

    col1, col2, col3, col4 = st.columns(4)
    with col1:
        render_metric_card("Total Queries", len(query_history))
    with col2:
        avg_latency = query_history['TOTAL_ELAPSED_TIME'].mean()
        render_metric_card("Avg Latency", f"{avg_latency:.1f} ms")
    with col3:
        p95_latency = query_history['TOTAL_ELAPSED_TIME'].quantile(0.95)
        render_metric_card("P95 Latency", f"{p95_latency:.1f} ms")
    with col4:
        max_latency = query_history['TOTAL_ELAPSED_TIME'].max()
        render_metric_card("Max Latency", f"{max_latency:.1f} ms")

    # Time series chart from query history
    if 'START_TIME' in query_history.columns:
        st.subheader("📉 Metrics Over Time")

        # Aggregate by minute
        query_history['MINUTE'] = pd.to_datetime(query_history['START_TIME']).dt.floor('min')
        time_agg = query_history.groupby('MINUTE').agg({
            'TOTAL_ELAPSED_TIME': ['count', 'mean'],
            'BYTES_SCANNED': 'sum'
        }).reset_index()
        time_agg.columns = ['MINUTE', 'QUERY_COUNT', 'AVG_LATENCY_MS', 'BYTES_SCANNED']

        if not time_agg.empty:
            fig = make_subplots(rows=1, cols=2, subplot_titles=('Query Count per Minute', 'Avg Latency per Minute'))
            fig.add_trace(
                go.Scatter(x=time_agg['MINUTE'], y=time_agg['QUERY_COUNT'], mode='lines+markers', name='Queries'),
                row=1, col=1
            )
            fig.add_trace(
                go.Scatter(x=time_agg['MINUTE'], y=time_agg['AVG_LATENCY_MS'], mode='lines+markers', name='Latency'),
                row=1, col=2
            )
            fig.update_layout(height=350, showlegend=False)
            st.plotly_chart(fig, use_container_width=True)

    # Query history table
    with st.expander("📜 Query Details", expanded=True):
        display_cols = ['START_TIME', 'TOTAL_ELAPSED_TIME', 'EXECUTION_STATUS', 'QUERY_TAG', 'QUERY_TEXT']
        available_cols = [c for c in display_cols if c in query_history.columns]
        st.dataframe(
            query_history[available_cols].head(100),
            use_container_width=True,
            hide_index=True
        )


def page_run_test():
    """Render test runner page."""
    st.header("🚀 Run Stress Test")

    st.markdown("""
    ### Quick Start Commands

    Run these commands in your terminal to execute stress tests:
    """)

    # Test configuration
    col1, col2 = st.columns(2)

    with col1:
        warehouse = st.selectbox(
            "Warehouse",
            ["STRESS_TEST_XSMALL_GEN1", "STRESS_TEST_XSMALL_GEN2",
             "STRESS_TEST_SMALL_GEN1", "STRESS_TEST_SMALL_GEN2",
             "STRESS_TEST_MEDIUM_GEN1", "STRESS_TEST_MEDIUM_GEN2"]
        )
        users = st.number_input("Concurrent Users", 10, 200, 50)

    with col2:
        duration = st.selectbox("Duration", ["2m", "5m", "10m", "30m"])
        connection = st.text_input("Connection Name", "stress_test")

    # Generate command
    cmd = f"""cd tests && locust -f locustfile-snowflake.py \\
    --connection {connection} \\
    --warehouse {warehouse} \\
    --users {users} \\
    --spawn-rate 2 \\
    --run-time {duration} \\
    --headless \\
    --html ../results/{warehouse}_{duration}.html"""

    st.code(cmd, language="bash")

    st.markdown("---")

    st.markdown("""
    ### Warehouse Management

    ```bash
    # Setup warehouses
    python warehouse_manager.py setup stress_test

    # List warehouses
    python warehouse_manager.py list stress_test

    # Suspend all (stop credit usage)
    python warehouse_manager.py suspend stress_test

    # Cleanup (drop all)
    python warehouse_manager.py cleanup stress_test
    ```
    """)


def page_debug():
    """Debug page to test queries and show what's working."""
    st.header("🔧 Debug & Query Testing")

    if not st.session_state.connected:
        st.info("👈 Please connect to Snowflake using the sidebar")
        return

    conn = st.session_state.connection

    st.markdown("""
    This page tests the queries used by this dashboard to help diagnose issues.
    """)

    if st.button("Run Query Tests", type="primary"):
        with st.spinner("Testing queries..."):
            results = test_queries(conn)

        st.subheader("Test Results")

        # Connection test
        conn_result = results.get('connection', {})
        if conn_result.get('status') == 'OK':
            st.success(f"✅ **Connection**: User={conn_result.get('user')}, Role={conn_result.get('role')}, Warehouse={conn_result.get('warehouse')}")
        else:
            st.error(f"❌ **Connection**: {conn_result.get('error')}")

        # SHOW WAREHOUSES test
        wh_result = results.get('show_warehouses', {})
        if wh_result.get('status') == 'OK':
            st.success(f"✅ **SHOW WAREHOUSES**: Found {wh_result.get('count')} stress test warehouses")
            if wh_result.get('count', 0) == 0:
                st.warning("No STRESS_TEST_* warehouses found. Run: `python warehouse_manager.py setup stress_test`")
        else:
            st.error(f"❌ **SHOW WAREHOUSES**: {wh_result.get('error')}")

        # QUERY_HISTORY test
        qh_result = results.get('query_history', {})
        if qh_result.get('status') == 'OK':
            st.success(f"✅ **QUERY_HISTORY Access**: {qh_result.get('recent_queries')} queries in last hour")
        else:
            st.error(f"❌ **QUERY_HISTORY Access**: {qh_result.get('error')}")

        # Tagged queries test
        tag_result = results.get('tagged_queries', {})
        if tag_result.get('status') == 'OK':
            count = tag_result.get('count', 0)
            if count > 0:
                st.success(f"✅ **Tagged Queries**: {count} queries with STRESS_TEST tag in last 24 hours")
            else:
                st.warning(f"⚠️ **Tagged Queries**: No queries found with `{QUERY_TAG_PREFIX}` tag. Run a stress test first!")
        else:
            st.error(f"❌ **Tagged Queries**: {tag_result.get('error')}")

    st.markdown("---")

    # Manual query tester
    st.subheader("🔍 Manual Query Tester")

    default_query = f"""SELECT
    QUERY_ID,
    QUERY_TAG,
    WAREHOUSE_NAME,
    START_TIME,
    TOTAL_ELAPSED_TIME
FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY(
    DATEADD('hours', -2, CURRENT_TIMESTAMP()),
    CURRENT_TIMESTAMP(),
    RESULT_LIMIT => 50
))
WHERE QUERY_TAG LIKE '{QUERY_TAG_PREFIX}%'
ORDER BY START_TIME DESC"""

    query = st.text_area("Enter SQL Query", value=default_query, height=200)

    if st.button("Execute Query"):
        with st.spinner("Executing..."):
            try:
                results = conn.fetch(query)
                if results:
                    df = pd.DataFrame([row_to_dict(row) for row in results])
                    st.success(f"✅ Query returned {len(df)} rows")
                    st.dataframe(df, use_container_width=True, hide_index=True)
                else:
                    st.info("Query returned no results")
            except Exception as e:
                st.error(f"❌ Query failed: {e}")


# ============================================================
# Main App
# ============================================================

def main():
    """Main application entry point."""
    init_session_state()

    # Sidebar
    st.sidebar.title("❄️ Stress Test Dashboard")
    connect_sidebar()

    # Navigation
    st.sidebar.markdown("---")
    page = st.sidebar.radio(
        "Navigation",
        ["Overview", "Gen1 vs Gen2", "Warehouse Details", "Run Test", "🔧 Debug"],
        label_visibility="collapsed"
    )

    # Render selected page
    if page == "Overview":
        page_overview()
    elif page == "Gen1 vs Gen2":
        page_gen_comparison()
    elif page == "Warehouse Details":
        page_warehouse_details()
    elif page == "Run Test":
        page_run_test()
    elif page == "🔧 Debug":
        page_debug()

    # Footer
    st.sidebar.markdown("---")
    st.sidebar.caption("Snowflake Stress Test Dashboard v1.0")


if __name__ == "__main__":
    main()
