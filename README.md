# WARNIGN this is not the main branch this is just some old files and the important auto sync scripts dont use these files

# Cloudloop Iridium MQTT Bridge

A professional Python-based interface for interacting with the Cloudloop MQTT Broker. This tool enables bi-directional communication with remote Iridium satellite devices (RockBLOCK, RockREMOTE, etc.) using authenticated TLS 1.2 connections.

## 🚀 Key Features
- **Real-time Monitoring**: Subscribes to Mobile Originated (MO) messages sent from the field.
- **Bi-directional Communication**: Interactive terminal to send Mobile Terminated (MT) messages to devices.
- **Local SQLite Persistence**: Automatically logs every message, metadata, and GPS coordinate to a local database for historical analysis.
- **Security-First**: Uses Certificate-based authentication and environment variables to keep credentials out of source code.
- **Robust Error Handling**: Handles Base64 decoding and connection retries.

---

## 🛠️ Setup & Installation

### 1. Environment Setup
Clone the repository and create an isolated Python environment:
git clone https://github.com/YOUR_USERNAME/cloudloop-mqtt.git
cd cloudloop-mqtt
python3 -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate
pip install -r requirements.txt

### 2. Certificate Placement
Create a 'certs/' directory in the root folder. Place your three Cloudloop certificates inside:
- CloudloopMQTT.pem
- YOUR_DEVICE-certificate.pem.crt
- YOUR_DEVICE-private.pem.crt

### 3. Configuration (.env)
Copy the example environment file and fill in your specific IDs:
cp .env.example .env
nano .env

Ensure the paths in .env match your certificate filenames exactly.

---

## 📊 Data & Usage

### Running the Bridge
python main.py

- Incoming messages appear in the console and save to the DB automatically.
- Type any text and press Enter to send a message to the device.
- Type 'q' to safely disconnect and exit.

### Querying History
Since data is stored in 'cloudloop_messages.db', you can query it anytime:
sqlite3 cloudloop_messages.db "SELECT timestamp, decoded_text FROM messages ORDER BY timestamp DESC LIMIT 5;"

---

## 🔒 Security & Git
This repository is pre-configured to ignore sensitive files.
- .env (Contains your Account/Thing IDs)
- certs/ (Contains your private keys)
- *.db (Contains your message history)

Ensure these remain untracked to keep your satellite account secure.
