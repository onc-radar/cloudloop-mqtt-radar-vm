import os
import json
import base64
import ssl
import time
import sqlite3
from datetime import datetime
from dotenv import load_dotenv
import paho.mqtt.client as mqtt
from paho.mqtt.enums import CallbackAPIVersion

# --- Setup ---
load_dotenv()
BROKER_HOST = "mqtt.cloudloop.com"
BROKER_PORT = 8883
DB_FILE = "cloudloop_messages.db"
DOWNLOAD_DIR = "downloads"

# Credentials from .env
ACCOUNT_ID = os.getenv("CL_ACCOUNT_ID")
THING_ID = os.getenv("CL_THING_ID")
CA_CERT = os.getenv("CERT_CA")
CLIENT_CERT = os.getenv("CERT_CLIENT")
PRIVATE_KEY = os.getenv("CERT_KEY")

# Topics
TOPIC_MO = f"lingo/{ACCOUNT_ID}/{THING_ID}/MO"
TOPIC_MT = f"lingo/{ACCOUNT_ID}/{THING_ID}/MT"

# --- Database & File Logic ---
def init_db():
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
            file_path TEXT,
            latitude REAL,
            longitude REAL
        )
    ''')
    conn.commit()
    conn.close()
    if not os.path.exists(DOWNLOAD_DIR):
        os.makedirs(DOWNLOAD_DIR)

def save_to_db(topic, raw_json, decoded_text, file_path=None, lat=None, lon=None):
    try:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO messages (topic, device_id, raw_json, decoded_text, file_path, latitude, longitude)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (topic, THING_ID, json.dumps(raw_json), decoded_text, file_path, lat, lon))
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"❌ DB Error: {e}")

# --- MQTT Callbacks ---
def on_message(client, userdata, msg):
    try:
        data = json.loads(msg.payload.decode('utf-8'))
        b64_message = data.get("message", "")
        if not b64_message:
            return

        raw_bytes = base64.b64decode(b64_message)
        file_path = None
        decoded_text = ""

        # Logic to determine if it's binary or text
        # 1. Check for GZIP header (0x1f 0x8b) or if it's not valid UTF-8
        try:
            decoded_text = raw_bytes.decode('utf-8')
            print(f"\n📩 Text Message: {decoded_text}")
        except UnicodeDecodeError:
            # It's binary! Let's save it.
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"rx_{timestamp}.bin"
            
            # Try to guess extension from content (optional)
            if raw_bytes.startswith(b'\x1f\x8b'):
                filename = f"rx_{timestamp}.gz"
            
            file_path = os.path.join(DOWNLOAD_DIR, filename)
            with open(file_path, "wb") as f:
                f.write(raw_bytes)
            
            decoded_text = f"[BINARY DATA SAVED TO {filename}]"
            print(f"\n📦 Binary received and saved to: {file_path}")

        # Metadata
        sbd = data.get("sbd", {})
        loc = sbd.get("location", {})
        
        save_to_db(msg.topic, data, decoded_text, file_path, loc.get("latitude"), loc.get("longitude"))

    except Exception as e:
        print(f"⚠️ Error: {e}")

# --- Standard MQTT Boilerplate ---
def on_connect(client, userdata, flags, rc, properties=None):
    if rc == 0:
        print(f"✅ Connected. Subscribed to {TOPIC_MO}")
        client.subscribe(TOPIC_MO)
        print("Type a message to send, or 'q' to quit.")
    else:
        print(f"❌ Auth failed: {rc}")

def send_device_message(client, text):
    payload = {"message": base64.b64encode(text.encode('utf-8')).decode('utf-8')}
    client.publish(TOPIC_MT, json.dumps(payload))
    print(f"📤 Queued: {text}")

# --- Execution ---
if __name__ == "__main__":
    init_db()
    client = mqtt.Client(CallbackAPIVersion.VERSION2)
    client.tls_set(ca_certs=CA_CERT, certfile=CLIENT_CERT, keyfile=PRIVATE_KEY,
                   cert_reqs=ssl.CERT_REQUIRED, tls_version=ssl.PROTOCOL_TLSv1_2)
    client.on_connect = on_connect
    client.on_message = on_message
    client.connect(BROKER_HOST, BROKER_PORT, 60)
    client.loop_start()

    try:
        while True:
            val = input("> ")
            if val.lower() == 'q': break
            if val: send_device_message(client, val)
            time.sleep(0.5)
    except KeyboardInterrupt: pass
    finally:
        client.loop_stop()
        client.disconnect()
