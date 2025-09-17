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

# ---------- ENSURE REQUIRED COLUMNS ----------
required_columns = [
    "AMOUNT","BENE_ACCOUNT_NO","CREDIT_REF","CUSTOMER_SUBCODE","CUSTOMER_SUBCODE_EMAIL","CUSTOMER_SUBCODE_MOBILE",
    "CUST_CODE","CUST_NAME","INVOICE_AMT_TOL_PCT","INVOICE_NO","KEY_VALUE","MAIN_CREDIT_AC","MASTER_DUE_DATE",
    "MASTER_RMTR_ACCT_NO","MASTER_RMTR_ADDRESS","MASTER_RMTR_NAME","MAX_CREDIT_AMT","MIN_CREDIT_AMT",
    "NOTIFY_RESULT","NOTIFY_STATUS","ORD","RECR_IFSC","REMITTER_CODE","RETURNED_AT","RETURN_REF","RMTR_ACCOUNT_IFSC",
    "RMTR_ACCOUNT_NO","RMTR_ADD","RMTR_EMAIL","RMTR_EMAIL_NOTIFY_REF","RMTR_FULL_NAME","RMTR_MOBILE",
    "RMTR_SMS_NOTIFY_REF","RMTR_TO_BENE_NOTE","SETTLE_REF","TRANSACTION_REF_NO","TRANSFER_TYPE","TRANS_RECEIVED_AT",
    "TRANS_SETTLED_AT","TRANS_STATUS","TRANS_TRANSFER_DATE","UDF11","UDF12","UDF13","UDF14","UDF15","UDF16","UDF17",
    "UDF18","UDF19","UDF20","VALIDATION_RESULT","VALIDATION_STATUS"
]

# Add missing columns
for col in required_columns:
    if col not in df.columns:
        df[col] = None

# Reorder columns
df = df[required_columns]

# ---------- CLEAN DATE COLUMNS ----------
date_cols = ["MASTER_DUE_DATE","RETURNED_AT","TRANS_RECEIVED_AT","TRANS_SETTLED_AT","TRANS_TRANSFER_DATE"]
for col in date_cols:
    df[col] = pd.to_datetime(df[col], errors='coerce')  # invalid strings → NaT → NULL

# ---------- CONNECT TO POSTGRES ----------
conn = psycopg2.connect(**DB_CONFIG)
cursor = conn.cursor()

table_name = "ecollect_report"

# Drop table if exists
cursor.execute(f"DROP TABLE IF EXISTS {table_name};")
conn.commit()

# ---------- CREATE TABLE ----------
create_columns = []
for col in required_columns:
    col_lower = col.lower()
    if "DATE" in col or "AT" in col:
        create_columns.append(f"{col_lower} TIMESTAMP")
    elif "AMT" in col or col=="AMOUNT":
        create_columns.append(f"{col_lower} NUMERIC")
    else:
        create_columns.append(f"{col_lower} VARCHAR(255)")

create_table_query = f"CREATE TABLE {table_name} ({', '.join(create_columns)});"
cursor.execute(create_table_query)
conn.commit()

# ---------- INSERT DATA ----------
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
