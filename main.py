import os
import json
import base64
import ssl
import time
import sqlite3
import gzip
import shutil
from datetime import datetime
from dotenv import load_dotenv
import paho.mqtt.client as mqtt
from paho.mqtt.enums import CallbackAPIVersion

# 1. Load Environment Variables
load_dotenv()

# --- Configuration ---
BROKER_HOST = "mqtt.cloudloop.com"
BROKER_PORT = 8883
DB_FILE = "cloudloop_messages.db"
DOWNLOAD_DIR = "downloads"

ACCOUNT_ID = os.getenv("CL_ACCOUNT_ID")
THING_ID = os.getenv("CL_THING_ID")
CA_CERT = os.getenv("CERT_CA")
CLIENT_CERT = os.getenv("CERT_CLIENT")
PRIVATE_KEY = os.getenv("CERT_KEY")

TOPIC_MO = f"lingo/{ACCOUNT_ID}/{THING_ID}/MO"
TOPIC_MT = f"lingo/{ACCOUNT_ID}/{THING_ID}/MT"

# --- Database & File Logic ---
def init_db():
    """Initializes the SQLite database with the parsed_data column."""
    if not os.path.exists(DOWNLOAD_DIR):
        os.makedirs(DOWNLOAD_DIR)
        
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS messages (
            cl_id TEXT PRIMARY KEY,
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
            topic TEXT,
            momsn INTEGER,
            decoded_text TEXT,
            file_path TEXT,
            latitude REAL,
            longitude REAL,
            raw_json TEXT,
            parsed_data TEXT
        )
    ''')
    conn.commit()
    conn.close()

def save_to_db(cl_id, topic, momsn, decoded_text, file_path, lat, lon, raw_json, parsed_data=None):
    """Saves the message and any parsed sensor data to SQLite."""
    try:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        
        # Convert the parsed dictionary to a JSON string for SQLite storage
        parsed_json_str = json.dumps(parsed_data) if parsed_data else None

        cursor.execute('''
            INSERT OR IGNORE INTO messages 
            (cl_id, topic, momsn, decoded_text, file_path, latitude, longitude, raw_json, parsed_data)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (cl_id, topic, momsn, decoded_text, file_path, lat, lon, json.dumps(raw_json), parsed_json_str))
        is_new = cursor.rowcount > 0
        conn.commit()
        conn.close()
        return is_new
    except Exception as e:
        print(f"❌ Database Error: {e}")
        return False

# --- Sensor Parsing Logic ---
def parse_sensor_data(file_path):
    """
    Reads an extracted text file and attempts to parse it into structured data.
    Modify the logic inside here to match your specific hardware's payload format.
    """
    parsed_dict = {}
    try:
        with open(file_path, 'r', encoding='utf-8', errors='replace') as f:
            content = f.read().strip()
            
            if not content:
                return parsed_dict

            # 1. Attempt JSON parsing
            if content.startswith('{') or content.startswith('['):
                try:
                    return json.loads(content)
                except json.JSONDecodeError:
                    pass # Fall through to custom parsing
            
            # 2. Attempt Key-Value or CSV parsing
            lines = content.split('\n')
            for i, line in enumerate(lines):
                line = line.strip()
                if not line:
                    continue
                
                # Example: "Temperature: 25.4" or "Temp=25.4"
                if ':' in line:
                    parts = line.split(':', 1)
                    parsed_dict[parts[0].strip()] = parts[1].strip()
                elif '=' in line:
                    parts = line.split('=', 1)
                    parsed_dict[parts[0].strip()] = parts[1].strip()
                elif ',' in line:
                    parts = line.split(',', 1)
                    # For basic CSV, we just create generic keys if there is no header
                    parsed_dict[f"column_{i}"] = parts[1].strip()
                else:
                    parsed_dict[f"line_{i}"] = line

    except Exception as e:
        print(f"⚠️ Parsing error on {file_path}: {e}")
        
    return parsed_dict

# --- MQTT Callbacks ---
def on_connect(client, userdata, flags, rc, properties=None):
    if rc == 0:
        print(f"✅ Connected. Subscribed to {TOPIC_MO}")
        client.subscribe(TOPIC_MO)
        print("🚀 Ready! Type a message to send or 'q' to quit.")
    else:
        print(f"❌ Connection failed: {rc}")

def on_message(client, userdata, msg):
    try:
        data = json.loads(msg.payload.decode('utf-8'))
        cl_id = data.get("id")
        b64_msg = data.get("message", "")
        
        if not cl_id or not b64_msg: return

        raw_bytes = base64.b64decode(b64_msg)
        sbd = data.get("sbd", {})
        momsn = sbd.get("momsn")
        loc = sbd.get("location", {})
        
        file_path = None
        decoded_text = ""
        parsed_telemetry = None

        # --- Binary & GZIP Handling ---
        try:
            # Check for GZIP Magic Bytes: \x1f\x8b
            if raw_bytes.startswith(b'\x1f\x8b'):
                gz_filename = f"msg_{cl_id}.gz"
                file_path = os.path.join(DOWNLOAD_DIR, gz_filename)
                
                if not os.path.exists(file_path):
                    with open(file_path, "wb") as f:
                        f.write(raw_bytes)
                
                # Extraction
                extracted_path = file_path.replace(".gz", ".txt")
                try:
                    with gzip.open(file_path, 'rb') as f_in:
                        with open(extracted_path, 'wb') as f_out:
                            shutil.copyfileobj(f_in, f_out)
                    
                    # Read preview
                    with open(extracted_path, 'r', errors='replace') as f_view:
                        preview = f_view.read(50).strip()
                    decoded_text = f"[GZIP EXTRACTED] Preview: {preview}..."
                    
                    # RUN PARSER ON EXTRACTED DATA
                    parsed_telemetry = parse_sensor_data(extracted_path)
                    
                except Exception as e:
                    decoded_text = f"[GZIP ERROR: {e}]"
            
            else:
                # Regular UTF-8 Text
                decoded_text = raw_bytes.decode('utf-8')

        except UnicodeDecodeError:
            # General Binary (Non-GZIP)
            filename = f"msg_{cl_id}.bin"
            file_path = os.path.join(DOWNLOAD_DIR, filename)
            if not os.path.exists(file_path):
                with open(file_path, "wb") as f:
                    f.write(raw_bytes)
            decoded_text = f"[BINARY SAVED: {filename}]"

        # Persistence
        if save_to_db(cl_id, msg.topic, momsn, decoded_text, file_path, 
                      loc.get("latitude"), loc.get("longitude"), data, parsed_telemetry):
            print(f"\n📩 NEW [{momsn}]: {decoded_text}")
            if parsed_telemetry:
                print(f"📊 Parsed Data: {parsed_telemetry}")
        else:
            print(f"♻️  Duplicate: {cl_id}")

    except Exception as e:
        print(f"⚠️ Error: {e}")

def send_device_message(client, text):
    payload = {"message": base64.b64encode(text.encode('utf-8')).decode('utf-8')}
    client.publish(TOPIC_MT, json.dumps(payload))
    print(f"📤 Queued: {text}")

# --- Main ---
if __name__ == "__main__":
    if not all([ACCOUNT_ID, THING_ID, CA_CERT, CLIENT_CERT, PRIVATE_KEY]):
        print("❌ Error: Missing configuration in .env file.")
        exit(1)

    init_db()
    client = mqtt.Client(CallbackAPIVersion.VERSION2)
    
    try:
        client.tls_set(ca_certs=CA_CERT, certfile=CLIENT_CERT, keyfile=PRIVATE_KEY,
                       cert_reqs=ssl.CERT_REQUIRED, tls_version=ssl.PROTOCOL_TLSv1_2)
    except FileNotFoundError as e:
        print(f"❌ Certificate Error: {e}")
        exit(1)
        
    client.on_connect = on_connect
    client.on_message = on_message
    
    print(f"🔄 Connecting to {BROKER_HOST}...")
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
