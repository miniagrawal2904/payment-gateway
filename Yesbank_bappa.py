import os
import zipfile
import re
import base64
import pandas as pd
import psycopg2
from datetime import datetime
from msal import PublicClientApplication
import requests

# ---------- CONFIG ----------
CLIENT_ID = "974f8b00-627f-4aea-b048-f55fc22a605b"
TENANT_ID = "ebda27e9-560f-48c1-bf94-2119f83863d6"
USER_EMAIL = "mini.agrawal@progfin.in"
SENDER_EMAIL = "deepak1.singh@progfin.in"
DOWNLOAD_DIR = "/Users/miniagrawal/Downloads"

DB_CONFIG = {
    "dbname": "lms",
    "user": "postgres",
    "password": "Gds80p^M*fOx",
    "host": "qa.clz6pqqb00si.ap-south-1.rds.amazonaws.com",
    "port": "5432"
}

SCOPES = ["Mail.Read"]

# ---------- STEP 1: AUTHENTICATE VIA DEVICE CODE FLOW ----------
app = PublicClientApplication(client_id=CLIENT_ID, authority=f"https://login.microsoftonline.com/{TENANT_ID}")

flow = app.initiate_device_flow(scopes=SCOPES)
if "user_code" not in flow:
    raise Exception("❌ Failed to create device flow. Check your client ID and tenant.")

print(flow["message"])  # User instruction

# Acquire token
result = app.acquire_token_by_device_flow(flow)  # Blocks until login
if "access_token" not in result:
    raise Exception(f"❌ Authentication failed: {result.get('error_description')}")

access_token = result["access_token"]
headers = {"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"}

# ---------- STEP 2: GET LATEST EMAIL ----------
search_url = "https://graph.microsoft.com/v1.0/me/messages"
params = {
    "$top": 10,
    "$orderby": "receivedDateTime desc",
    "$search": f'"from:{SENDER_EMAIL}" AND "WU 0032"'
}

mail_r = requests.get(search_url, headers=headers, params=params)
mail_data = mail_r.json()

if "value" not in mail_data or not mail_data["value"]:
    raise Exception("⚠️ No matching WU 0032 emails found in your mailbox.")

latest_email = mail_data["value"][0]
print("📧 Latest email received at:", latest_email["receivedDateTime"])
print("📌 Subject:", latest_email["subject"])

# ---------- STEP 3: DOWNLOAD ATTACHMENTS ----------
attachments_url = f"https://graph.microsoft.com/v1.0/me/messages/{latest_email['id']}/attachments"
att_r = requests.get(attachments_url, headers=headers)
attachments = att_r.json().get("value", [])

print("📎 Attachments found:")
for att in attachments:
    print(f"- Name: {att.get('name')} | Type: {att.get('@odata.type')} | Size: {att.get('size')}")

file_paths = []
for att in attachments:
    if att["@odata.type"] == "#microsoft.graph.fileAttachment":
        name = att["name"]
        if name.lower().endswith(".zip") or name.lower().endswith(".csv"):
            file_path = os.path.join(DOWNLOAD_DIR, name)
            with open(file_path, "wb") as f:
                f.write(base64.b64decode(att["contentBytes"]))
            file_paths.append(file_path)
            print(f"✅ Downloaded: {file_path}")

if not file_paths:
    raise Exception("❌ No usable attachment (.zip or .csv) found in email.")

# ---------- STEP 4: HANDLE ATTACHMENTS ----------
csv_files = []

for file_path in file_paths:
    if file_path.lower().endswith(".zip"):
        extract_dir = os.path.join(DOWNLOAD_DIR, "yesbank_bappa")
        os.makedirs(extract_dir, exist_ok=True)
        with zipfile.ZipFile(file_path, "r") as zip_ref:
            zip_ref.extractall(extract_dir)
            csv_files.extend([os.path.join(extract_dir, f) for f in zip_ref.namelist() if f.lower().endswith(".csv")])
    elif file_path.lower().endswith(".csv"):
        csv_files.append(file_path)

if not csv_files:
    raise Exception("❌ No CSV file found in attachments.")
print("✅ CSV files ready:", [os.path.basename(f) for f in csv_files])


# ---------- STEP 5: FIND LATEST CSV BY DATE ----------
def extract_date_from_filename(filename):
    match = re.search(r'(\d{8})', filename)  # looks for YYYYMMDD
    if match:
        return datetime.strptime(match.group(1), "%Y%m%d")
    return None

latest_file = None
latest_date = None
for f in csv_files:
    file_date = extract_date_from_filename(f)
    if file_date and (latest_date is None or file_date > latest_date):
        latest_date = file_date
        latest_file = f

if not latest_file:
    print("⚠️ Could not determine latest report from filename. Using first CSV file.")
    latest_file = csv_files[0]
else:
    print(f"✅ Latest CSV report determined: {os.path.basename(latest_file)} (Date: {latest_date.strftime('%Y-%m-%d')})")

df = pd.read_csv(latest_file)

# Convert date columns
date_cols = ["TXN_DATE", "VALUE_DATE", "DAT_POST"]
for col in date_cols:
    if col in df.columns:
        df[col] = pd.to_datetime(df[col], errors='coerce')

conn = psycopg2.connect(**DB_CONFIG)
cursor = conn.cursor()

# Create table with unique constraint for UPSERT
create_table_query = """
CREATE TABLE IF NOT EXISTS yesbank_bappa (
    ROWNUM INT,
    TXN_DATE TIMESTAMP,
    COD_ACCT_NO VARCHAR(50),
    NARRATION TEXT,
    VALUE_DATE TIMESTAMP,
    CHEQUE_NO VARCHAR(50),
    DRCR_FLAG VARCHAR(5),
    AMOUNT NUMERIC,
    DAT_POST TIMESTAMP,
    RUNNING_BALANCE NUMERIC,
    URN VARCHAR(100),
    BANKREFERENCENUMBER VARCHAR(100),
    inserted_at TIMESTAMP DEFAULT NOW(),
    CONSTRAINT yesbank_bappa_unique UNIQUE (URN, BANKREFERENCENUMBER)
);
"""
cursor.execute(create_table_query)
conn.commit()

# UPSERT query
columns = ','.join(df.columns)
placeholders = ','.join(['%s'] * len(df.columns))
update_assignments = ','.join([f"{col}=EXCLUDED.{col}" for col in df.columns])

insert_query = f"""
INSERT INTO yesbank_bappa ({columns})
VALUES ({placeholders})
ON CONFLICT (URN, BANKREFERENCENUMBER) DO UPDATE
SET {update_assignments},
    inserted_at = NOW();
"""

for _, row in df.iterrows():
    values = [None if pd.isna(v) else v for v in row]
    cursor.execute(insert_query, tuple(values))

conn.commit()
cursor.close()
conn.close()
print("✅ Latest WU 0032 CSV report loaded into Postgres (with UPSERT) successfully!")
