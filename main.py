import os
import json
import base64
import ssl
import time
import sqlite3
from dotenv import load_dotenv
import paho.mqtt.client as mqtt
from paho.mqtt.enums import CallbackAPIVersion

# 1. Load Environment Variables
load_dotenv()

# --- Configuration ---
BROKER_HOST = "mqtt.cloudloop.com"
BROKER_PORT = 8883
DB_FILE = "cloudloop_messages.db"

ACCOUNT_ID = os.getenv("CL_ACCOUNT_ID")
THING_ID = os.getenv("CL_THING_ID")
CA_CERT = os.getenv("CERT_CA")
CLIENT_CERT = os.getenv("CERT_CLIENT")
PRIVATE_KEY = os.getenv("CERT_KEY")

# Topics
TOPIC_MO = f"lingo/{ACCOUNT_ID}/{THING_ID}/MO"
TOPIC_MT = f"lingo/{ACCOUNT_ID}/{THING_ID}/MT"

# --- Database Logic ---
def init_db():
    """Initializes the SQLite database."""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
            topic TEXT,
            device_id TEXT,
            raw_json TEXT,
            decoded_text TEXT,
            latitude REAL,
            longitude REAL
        )
    ''')
    conn.commit()
    conn.close()

def save_to_db(topic, raw_json, decoded_text, lat=None, lon=None):
    """Inserts received data into the database."""
    try:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO messages (topic, device_id, raw_json, decoded_text, latitude, longitude)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (topic, THING_ID, json.dumps(raw_json), decoded_text, lat, lon))
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"❌ Database Error: {e}")

# --- Sending Logic ---
def send_device_message(client, text):
    """Encodes and publishes a message TO the Iridium device."""
    try:
        # Base64 encode for Cloudloop requirement
        encoded_payload = base64.b64encode(text.encode('utf-8')).decode('utf-8')
        payload = {"message": encoded_payload}
        
        result = client.publish(TOPIC_MT, json.dumps(payload))
        
        if result.rc == mqtt.MQTT_ERR_SUCCESS:
            print(f"📤 Successfully queued MT message: '{text}'")
        else:
            print(f"❌ Failed to publish MT message. Error code: {result.rc}")
    except Exception as e:
        print(f"⚠️ Error sending message: {e}")

# --- MQTT Callbacks ---
def on_connect(client, userdata, flags, rc, properties=None):
    if rc == 0:
        print(f"✅ Connected to Cloudloop Broker")
        client.subscribe(TOPIC_MO)
        print(f"📡 Subscribed to MO: {TOPIC_MO}")
        print("🚀 Ready! Type a message and press Enter to send. Type 'q' to quit.")
    else:
        print(f"❌ Connection failed with code {rc}")

def on_message(client, userdata, msg):
    try:
        data = json.loads(msg.payload.decode('utf-8'))
        b64_message = data.get("message", "")
        decoded_text = ""
        
        if b64_message:
            # We use errors='replace' so it doesn't crash on binary data
            decoded_text = base64.b64decode(b64_message).decode('utf-8', errors='replace')
            print(f"\n📩 Incoming Message: {decoded_text}")
        
        # Extract metadata
        sbd_data = data.get("sbd", {})
        loc = sbd_data.get("location", {})
        lat = loc.get("latitude")
        lon = loc.get("longitude")

        # Save to DB
        save_to_db(msg.topic, data, decoded_text, lat, lon)
        print(f"💾 Message saved to database.")

    except Exception as e:
        print(f"⚠️ Error processing incoming message: {e}")

# --- Main Execution ---
if __name__ == "__main__":
    # Check config
    if not all([ACCOUNT_ID, THING_ID, CA_CERT, CLIENT_CERT, PRIVATE_KEY]):
        print("❌ Error: Missing configuration in .env")
        exit(1)

    init_db()

    client = mqtt.Client(CallbackAPIVersion.VERSION2)
    client.tls_set(
        ca_certs=CA_CERT,
        certfile=CLIENT_CERT,
        keyfile=PRIVATE_KEY,
        cert_reqs=ssl.CERT_REQUIRED,
        tls_version=ssl.PROTOCOL_TLSv1_2
    )

    client.on_connect = on_connect
    client.on_message = on_message

    print(f"🔄 Connecting to {BROKER_HOST}...")
    client.connect(BROKER_HOST, BROKER_PORT, 60)
    
    # Start background networking
    client.loop_start()

    try:
        while True:
            # The interactive prompt
            val = input("\n> ")
            if val.lower() == 'q':
                break
            if val:
                send_device_message(client, val)
            time.sleep(0.5)
    except KeyboardInterrupt:
        pass
    finally:
        print("\n🛑 Shutting down...")
        client.loop_stop()
        client.disconnect()
