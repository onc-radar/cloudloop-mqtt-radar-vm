import sqlite3
import pandas as pd
import streamlit as st
import json

# --- Page Configuration ---
st.set_page_config(page_title="Iridium Dashboard", page_icon="🛰️", layout="wide")
st.title("🛰️ Cloudloop Iridium Dashboard")

DB_FILE = "cloudloop_messages.db"

# --- Data Loading ---
def load_data():
    try:
        conn = sqlite3.connect(DB_FILE)
        # Pull the most important columns, newest first
        query = """
            SELECT timestamp, momsn, latitude, longitude, decoded_text, parsed_data 
            FROM messages 
            ORDER BY timestamp DESC
        """
        df = pd.read_sql_query(query, conn)
        conn.close()
        return df
    except Exception as e:
        st.error(f"Error loading database: {e}")
        return pd.DataFrame()

# Load the data
df = load_data()

if df.empty:
    st.warning("No messages found in the database yet.")
else:
    # --- Live Map Section ---
    st.subheader("📍 Device Tracking")
    
    # Filter out rows that don't have GPS coordinates
    map_data = df.dropna(subset=['latitude', 'longitude'])
    
    if not map_data.empty:
        # Streamlit's st.map automatically reads 'latitude' and 'longitude' columns
        st.map(map_data, zoom=12)
    else:
        st.info("No GPS coordinates available to display on the map.")

    st.divider()

    # --- Latest Telemetry Section ---
    st.subheader("📊 Latest Sensor Data")
    
    # Find the most recent message that actually has parsed sensor data
    parsed_rows = df.dropna(subset=['parsed_data'])
    if not parsed_rows.empty:
        latest_data_str = parsed_rows.iloc[0]['parsed_data']
        try:
            telemetry_dict = json.loads(latest_data_str)
            # Display it as nice metric cards
            cols = st.columns(len(telemetry_dict))
            for i, (key, value) in enumerate(telemetry_dict.items()):
                cols[i % len(cols)].metric(label=key, value=value)
        except:
            st.code(latest_data_str)
    else:
        st.info("No parsed telemetry data available yet.")

    st.divider()

    # --- Raw Message Table ---
    st.subheader("📨 Message History")
    
    # Display the full dataframe as an interactive table
    st.dataframe(
        df, 
        use_container_width=True,
        column_config={
            "timestamp": "Time Received",
            "momsn": "Message ID",
            "latitude": "Lat",
            "longitude": "Lon",
            "decoded_text": "Message / Preview",
            "parsed_data": "Extracted Data"
        }
    )

    # Add a refresh button
    if st.button("🔄 Refresh Data"):
        st.rerun()
