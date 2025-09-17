import os
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
SUBJECT_LINE = "Bapa 0032"
EXPECTED_FILE = "Bapa 0032.csv"
DOWNLOAD_DIR = "/Users/miniagrawal/Downloads"

DB_CONFIG = {
    "dbname": "lms",
    "user": "postgres",
    "password": "Gds80p^M*fOx",
    "host": "qa.clz6pqqb00si.ap-south-1.rds.amazonaws.com",
    "port": "5432"
}

SCOPES = ["Mail.Read"]

# ---------- AUTHENTICATE ----------
app = PublicClientApplication(CLIENT_ID, authority=f"https://login.microsoftonline.com/{TENANT_ID}")
flow = app.initiate_device_flow(scopes=SCOPES)
print(flow["message"])
result = app.acquire_token_by_device_flow(flow)
if "access_token" not in result:
    raise Exception(f"❌ Authentication failed: {result.get('error_description')}")
access_token = result["access_token"]
headers = {"Authorization": f"Bearer {access_token}"}

# ---------- GET EMAIL ----------
search_url = "https://graph.microsoft.com/v1.0/me/messages"
params = {"$top": 50, "$orderby": "receivedDateTime desc"}
mail_r = requests.get(search_url, headers=headers, params=params)
mail_data = mail_r.json()

filtered_emails = [
    e for e in mail_data.get("value", [])
    if SENDER_EMAIL.lower() in e.get("from", {}).get("emailAddress", {}).get("address", "").lower()
    and SUBJECT_LINE.lower() in e.get("subject", "").lower()
]

if not filtered_emails:
    raise Exception(f"⚠️ No emails found from {SENDER_EMAIL} with subject '{SUBJECT_LINE}'")

latest_email = filtered_emails[0]

# ---------- DOWNLOAD ATTACHMENT ----------
attachments_url = f"https://graph.microsoft.com/v1.0/me/messages/{latest_email['id']}/attachments"
att_r = requests.get(attachments_url, headers=headers)
attachments = att_r.json().get("value", [])

file_paths = []
for att in attachments:
    if att["@odata.type"] == "#microsoft.graph.fileAttachment" and att["name"] == EXPECTED_FILE:
        path = os.path.join(DOWNLOAD_DIR, att["name"])
        with open(path, "wb") as f:
            f.write(base64.b64decode(att["contentBytes"]))
        file_paths.append(path)
        print(f"✅ Downloaded: {path}")

if not file_paths:
    raise Exception(f"❌ No attachment found named {EXPECTED_FILE}")

# ---------- LOAD CSV INTO POSTGRES ----------
df = pd.read_csv(file_paths[0])

conn = psycopg2.connect(**DB_CONFIG)
cursor = conn.cursor()

# Create table if not exists
create_table_query = """
CREATE TABLE IF NOT EXISTS bapa_report (
    ROWNUM INT,
    TXN_DATE DATE,
    COD_ACCT_NO VARCHAR(50),
    NARRATION TEXT,
    VALUE_DATE DATE,
    CHEQUE_NO VARCHAR(50),
    DRCR_FLAG VARCHAR(10),
    AMOUNT NUMERIC,
    DAT_POST DATE,
    RUNNING_BALANCE NUMERIC,
    URN NUMERIC,
    BANKREFERENCENUMBER VARCHAR(100)
);
"""
cursor.execute(create_table_query)
conn.commit()

# Insert data
columns = ','.join(df.columns)
placeholders = ','.join(['%s'] * len(df.columns))
print(placeholders)
print(columns)
insert_query = f"INSERT INTO bapa_report ({columns}) VALUES ({placeholders})"
for _, row in df.iterrows():
    values = [None if pd.isna(v) else v for v in row]
    cursor.execute(insert_query, tuple(values))

conn.commit()
cursor.close()
conn.close()
print("✅ Bapa report loaded into Postgres successfully!")

