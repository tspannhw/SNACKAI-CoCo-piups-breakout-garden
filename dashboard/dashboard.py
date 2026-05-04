"""Local Streamlit dashboard for PiUPS Breakout Garden IoT sensor data.

Displays real-time sensor readings from:
  - BH1745 RGB Color (color bars)
  - LTR559 Light/Proximity (gauges)
  - VL53L1X Distance (time series)
  - BME680 Weather (temp/humidity/pressure)
  - LSM303D Motion (accelerometer/magnetometer)
  - System health metrics

Run locally: streamlit run dashboard/dashboard.py
Deploy to Snowflake:
  CREATE STREAMLIT IOT_LAB.SENSORS.PIUPS_DASHBOARD
    ROOT_LOCATION = '@IOT_LAB.SENSORS.STREAMLIT_STAGE'
    MAIN_FILE = 'dashboard.py'
    QUERY_WAREHOUSE = 'INGEST';
"""

import streamlit as st
from datetime import datetime

st.set_page_config(
    page_title="PiUPS IoT Sensors",
    page_icon="🌡️",
    layout="wide",
)

st.title("PiUPS Breakout Garden IoT Dashboard")
st.caption("Real-time sensor data from Raspberry Pi via Snowpipe Streaming v2")

conn = st.connection("snowflake")

# Sidebar controls
with st.sidebar:
    st.header("Controls")
    time_window = st.selectbox("Time Window", ["1 hour", "6 hours", "24 hours", "7 days"], index=1)
    auto_refresh = st.checkbox("Auto-refresh (30s)", value=True)
    device_filter = st.text_input("Device ID", value="")

    time_map = {"1 hour": 1, "6 hours": 6, "24 hours": 24, "7 days": 168}
    hours_back = time_map[time_window]

device_clause = f"AND device_id = '{device_filter}'" if device_filter else ""

# Latest reading
st.header("Latest Reading")
latest_query = f"""
SELECT *
FROM IOT_LAB.SENSORS.BREAKOUT_GARDEN_DATA
WHERE reading_ts > DATEADD('HOUR', -{hours_back}, CURRENT_TIMESTAMP())
  AND (bh1745_red IS NOT NULL OR ltr559_lux IS NOT NULL OR vl53l1x_distance_mm IS NOT NULL)
  {device_clause}
ORDER BY reading_ts DESC
LIMIT 1
"""
latest_df = conn.query(latest_query)

if latest_df.empty:
    st.warning("No PiUPS data found in the selected time window.")
    st.stop()

latest = latest_df.iloc[0]

# KPI row
col1, col2, col3, col4, col5, col6 = st.columns(6)
with col1:
    st.metric("Temperature", f"{latest.get('TEMPERATURE_C', '--')} C")
with col2:
    st.metric("Humidity", f"{latest.get('HUMIDITY_PERCENT', '--')}%")
with col3:
    st.metric("Light", f"{latest.get('LTR559_LUX', '--')} lux")
with col4:
    st.metric("Distance", f"{latest.get('VL53L1X_DISTANCE_MM', '--')} mm")
with col5:
    st.metric("Pressure", f"{latest.get('PRESSURE_HPA', '--')} hPa")
with col6:
    st.metric("CPU Temp", f"{latest.get('CPU_TEMP_C', '--')} C")

# Weather / Environment (BME680)
st.header("Weather / Environment (BME680)")
wx_query = f"""
SELECT reading_ts, temperature_c, humidity_percent, pressure_hpa, gas_resistance_ohms
FROM IOT_LAB.SENSORS.BREAKOUT_GARDEN_DATA
WHERE reading_ts > DATEADD('HOUR', -{hours_back}, CURRENT_TIMESTAMP())
  AND temperature_c IS NOT NULL
  AND (bh1745_red IS NOT NULL OR ltr559_lux IS NOT NULL)
  {device_clause}
ORDER BY reading_ts
"""
wx_df = conn.query(wx_query)

if not wx_df.empty:
    col1, col2 = st.columns(2)
    with col1:
        st.subheader("Temperature & Humidity")
        st.line_chart(wx_df.set_index("READING_TS")[["TEMPERATURE_C", "HUMIDITY_PERCENT"]])
    with col2:
        st.subheader("Pressure (hPa)")
        st.line_chart(wx_df.set_index("READING_TS")["PRESSURE_HPA"])

# Light & Distance
st.header("Light & Distance (LTR559 + VL53L1X)")
ld_query = f"""
SELECT reading_ts, ltr559_lux, ltr559_proximity, vl53l1x_distance_mm
FROM IOT_LAB.SENSORS.BREAKOUT_GARDEN_DATA
WHERE reading_ts > DATEADD('HOUR', -{hours_back}, CURRENT_TIMESTAMP())
  AND (ltr559_lux IS NOT NULL OR vl53l1x_distance_mm IS NOT NULL)
  {device_clause}
ORDER BY reading_ts
"""
ld_df = conn.query(ld_query)

if not ld_df.empty:
    col1, col2, col3 = st.columns(3)
    with col1:
        st.subheader("Ambient Light (lux)")
        st.line_chart(ld_df.set_index("READING_TS")["LTR559_LUX"])
    with col2:
        st.subheader("Proximity")
        st.line_chart(ld_df.set_index("READING_TS")["LTR559_PROXIMITY"])
    with col3:
        st.subheader("Distance (mm)")
        st.line_chart(ld_df.set_index("READING_TS")["VL53L1X_DISTANCE_MM"])

# Color (BH1745)
st.header("Color (BH1745)")
color_query = f"""
SELECT reading_ts, bh1745_red, bh1745_green, bh1745_blue, bh1745_clear
FROM IOT_LAB.SENSORS.BREAKOUT_GARDEN_DATA
WHERE reading_ts > DATEADD('HOUR', -{hours_back}, CURRENT_TIMESTAMP())
  AND bh1745_red IS NOT NULL
  {device_clause}
ORDER BY reading_ts
"""
color_df = conn.query(color_query)

if not color_df.empty:
    st.subheader("RGB + Clear Channels")
    st.line_chart(color_df.set_index("READING_TS")[["BH1745_RED", "BH1745_GREEN", "BH1745_BLUE", "BH1745_CLEAR"]])

# Motion (LSM303D)
st.header("Motion (LSM303D)")
motion_query = f"""
SELECT reading_ts,
       lsm303d_accel_x, lsm303d_accel_y, lsm303d_accel_z,
       lsm303d_mag_x, lsm303d_mag_y, lsm303d_mag_z
FROM IOT_LAB.SENSORS.BREAKOUT_GARDEN_DATA
WHERE reading_ts > DATEADD('HOUR', -{hours_back}, CURRENT_TIMESTAMP())
  AND lsm303d_accel_x IS NOT NULL
  {device_clause}
ORDER BY reading_ts
"""
motion_df = conn.query(motion_query)

if not motion_df.empty:
    col1, col2 = st.columns(2)
    with col1:
        st.subheader("Accelerometer (g)")
        st.line_chart(motion_df.set_index("READING_TS")[["LSM303D_ACCEL_X", "LSM303D_ACCEL_Y", "LSM303D_ACCEL_Z"]])
    with col2:
        st.subheader("Magnetometer")
        st.line_chart(motion_df.set_index("READING_TS")[["LSM303D_MAG_X", "LSM303D_MAG_Y", "LSM303D_MAG_Z"]])

# System Health
st.header("System Health")
sys_query = f"""
SELECT reading_ts, cpu_temp_c, cpu_percent, memory_percent, disk_usage_percent
FROM IOT_LAB.SENSORS.BREAKOUT_GARDEN_DATA
WHERE reading_ts > DATEADD('HOUR', -{hours_back}, CURRENT_TIMESTAMP())
  AND (bh1745_red IS NOT NULL OR ltr559_lux IS NOT NULL)
  {device_clause}
ORDER BY reading_ts
"""
sys_df = conn.query(sys_query)

if not sys_df.empty:
    col1, col2 = st.columns(2)
    with col1:
        st.subheader("CPU Temperature & Usage")
        st.line_chart(sys_df.set_index("READING_TS")[["CPU_TEMP_C", "CPU_PERCENT"]])
    with col2:
        st.subheader("Memory & Disk Usage")
        st.line_chart(sys_df.set_index("READING_TS")[["MEMORY_PERCENT", "DISK_USAGE_PERCENT"]])

# Anomalies
st.header("Anomaly Detection")
anomaly_query = f"""
SELECT reading_ts, device_id, anomaly_type, temperature_c, cpu_temp_c, ltr559_lux, vl53l1x_distance_mm
FROM IOT_LAB.SENSORS.V_PIUPS_ANOMALIES
WHERE reading_ts > DATEADD('HOUR', -{hours_back}, CURRENT_TIMESTAMP())
  {device_clause}
ORDER BY reading_ts DESC
LIMIT 50
"""
anomaly_df = conn.query(anomaly_query)

if not anomaly_df.empty:
    st.warning(f"{len(anomaly_df)} anomalies detected in the last {time_window}")
    st.dataframe(anomaly_df, use_container_width=True)
else:
    st.success("No anomalies detected. All readings within normal range.")

# Footer
st.divider()
st.caption(f"Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | "
           f"Data source: IOT_LAB.SENSORS.BREAKOUT_GARDEN_DATA | "
           f"Ingest: Snowpipe Streaming v2 High Speed")

if auto_refresh:
    import time
    time.sleep(30)
    st.rerun()
