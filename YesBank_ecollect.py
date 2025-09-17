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
SUBJECT_LINE = "E-collect"
EXPECTED_FILE = "E-collect.csv"
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

# Filter emails by sender and subject
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

# ---------- LOAD CSV ----------
df = pd.read_csv(file_paths[0])
print(df)

# ---------- CLEAN DATE COLUMNS ----------
date_cols = ["MASTER_DUE_DATE","RETURNED_AT","TRANS_RECEIVED_AT","TRANS_SETTLED_AT","TRANS_TRANSFER_DATE"]
for col in date_cols:
    if col in df.columns:
        df[col] = pd.to_datetime(df[col], errors='coerce')  # invalid strings → NaT → NULL

# ---------- CONNECT TO POSTGRES ----------
conn = psycopg2.connect(**DB_CONFIG)
cursor = conn.cursor()
table_name = "ecollect_report"

# Drop table if exists
cursor.execute(f"DROP TABLE IF EXISTS {table_name};")
conn.commit()

# ---------- CREATE TABLE ----------
create_table_query = """
CREATE TABLE IF NOT EXISTS ecollect_report (
    AMOUNT NUMERIC,
    BENE_ACCOUNT_NO VARCHAR(150),
    CREDIT_REF VARCHAR(100),
    CUSTOMER_SUBCODE VARCHAR(150),
    CUSTOMER_SUBCODE_EMAIL VARCHAR(150),
    CUSTOMER_SUBCODE_MOBILE VARCHAR(150),
    CUST_CODE VARCHAR(150),
    CUST_NAME VARCHAR(150),
    INVOICE_AMT_TOL_PCT NUMERIC,
    INVOICE_NO VARCHAR(150),
    KEY_VALUE VARCHAR(150),
    MAIN_CREDIT_AC VARCHAR(150),
    MASTER_DUE_DATE TIMESTAMP,
    MASTER_RMTR_ACCT_NO VARCHAR(150),
    MASTER_RMTR_ADDRESS TEXT,
    MASTER_RMTR_NAME VARCHAR(150),
    MAX_CREDIT_AMT NUMERIC,
    MIN_CREDIT_AMT NUMERIC,
    NOTIFY_RESULT VARCHAR(150),
    NOTIFY_STATUS VARCHAR(150),
    ORD VARCHAR(150),
    RECR_IFSC VARCHAR(150),
    REMITTER_CODE VARCHAR(150),
    RETURNED_AT TIMESTAMP,
    RETURN_REF VARCHAR(150),
    RMTR_ACCOUNT_IFSC VARCHAR(150),
    RMTR_ACCOUNT_NO VARCHAR(150),
    RMTR_ADD TEXT,
    RMTR_EMAIL VARCHAR(150),
    RMTR_EMAIL_NOTIFY_REF VARCHAR(150),
    RMTR_FULL_NAME VARCHAR(150),
    RMTR_MOBILE VARCHAR(150),
    RMTR_SMS_NOTIFY_REF VARCHAR(150),
    RMTR_TO_BENE_NOTE TEXT,
    SETTLE_REF VARCHAR(150),
    TRANSACTION_REF_NO VARCHAR(150),
    TRANSFER_TYPE VARCHAR(150),
    TRANS_RECEIVED_AT TIMESTAMP,
    TRANS_SETTLED_AT TIMESTAMP,
    TRANS_STATUS VARCHAR(150),
    TRANS_TRANSFER_DATE TIMESTAMP,
    UDF11 VARCHAR(150),
    UDF12 VARCHAR(150),
    UDF13 VARCHAR(150),
    UDF14 VARCHAR(150),
    UDF15 VARCHAR(150),
    UDF16 VARCHAR(150),
    UDF17 VARCHAR(150),
    UDF18 VARCHAR(150),
    UDF19 VARCHAR(150),
    UDF20 VARCHAR(150),
    VALIDATION_RESULT VARCHAR(150),
    VALIDATION_STATUS VARCHAR(150)
);
"""
cursor.execute(create_table_query)
conn.commit()
print("✅ Table 'ecollect_report' created successfully!")

# ---------- INSERT DATA ----------
# Ensure DataFrame has all columns
for col in [c.split()[0] for c in create_table_query.split('\n') if c.strip() and 'VARCHAR' in c or 'NUMERIC' in c or 'TIMESTAMP' in c]:
    if col not in df.columns:
        df[col] = None

# Lowercase columns for insertion
df.columns = [c.lower() for c in df.columns]
columns_str = ','.join(df.columns)
placeholders = ','.join(['%s'] * len(df.columns))
insert_query = f"INSERT INTO {table_name} ({columns_str}) VALUES ({placeholders})"

for _, row in df.iterrows():
    values = [None if pd.isna(v) else v for v in row]
    cursor.execute(insert_query, tuple(values))

conn.commit()
cursor.close()
conn.close()
print("✅ E-Collect report loaded into Postgres successfully!")
