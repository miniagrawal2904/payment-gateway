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

print(flow["message"])  # Instructs the user to go to URL and enter code

# Acquire token
result = app.acquire_token_by_device_flow(flow)  # This will block until user logs in
if "access_token" not in result:
    raise Exception(f"❌ Authentication failed: {result.get('error_description')}")

access_token = result["access_token"]
headers = {"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"}

# ---------- STEP 2: GET LATEST EMAIL ----------
search_url = "https://graph.microsoft.com/v1.0/me/messages"
params = {
    "$top": 5,
    "$orderby": "receivedDateTime desc",
    "$search": f'"from:{SENDER_EMAIL}" AND "E-Collect"'
}

mail_r = requests.get(search_url, headers=headers, params=params)
mail_data = mail_r.json()
print(mail_data)

if "value" not in mail_data or not mail_data["value"]:
    raise Exception("⚠️ No emails found from the sender in your mailbox.")

latest_email = mail_data["value"][0]
print("Latest email received at:", latest_email["receivedDateTime"])
print("Subject:", latest_email["subject"])

# ---------- STEP 3: DOWNLOAD ATTACHMENT ----------
attachments_url = f"https://graph.microsoft.com/v1.0/me/messages/{latest_email['id']}/attachments"
att_r = requests.get(attachments_url, headers=headers)
attachments = att_r.json().get("value", [])

print("📎 Attachments found:")
for att in attachments:
    print(f"- Name: {att.get('name')} | Type: {att.get('@odata.type')} | Size: {att.get('size')}")

file_paths = []
for att in attachments:
    if att["@odata.type"] == "#microsoft.graph.fileAttachment":
        if att["name"].endswith(".zip") or att["name"].endswith(".xlsx"):
            file_path = os.path.join(DOWNLOAD_DIR, att["name"])
            with open(file_path, "wb") as f:
                f.write(base64.b64decode(att["contentBytes"]))
            file_paths.append(file_path)
            print(f"✅ Downloaded attachment: {file_path}")

if not file_paths:
    raise Exception("❌ No usable attachment (.zip or .xlsx) found in email.")


# ---------- STEP 4: HANDLE ATTACHMENTS ----------
excel_files = []

for file_path in file_paths:
    if file_path.endswith(".zip"):
        extract_dir = os.path.join(DOWNLOAD_DIR, "yesbank_ecollect")
        os.makedirs(extract_dir, exist_ok=True)
        with zipfile.ZipFile(file_path, "r") as zip_ref:
            zip_ref.extractall(extract_dir)
            excel_files.extend([os.path.join(extract_dir, f) for f in zip_ref.namelist() if f.endswith(".xlsx")])
    elif file_path.endswith(".xlsx"):
        excel_files.append(file_path)

if not excel_files:
    raise Exception("❌ No Excel file found in attachments.")
print("✅ Excel files ready:", [os.path.basename(f) for f in excel_files])


# ---------- STEP 5: FIND LATEST EXCEL BY DATE ----------
def extract_date_from_filename(filename):
    match = re.search(r'(\d{8})', filename)  # looks for YYYYMMDD
    if match:
        return datetime.strptime(match.group(1), "%Y%m%d")
    return None

latest_file = None
latest_date = None
for f in excel_files:
    file_date = extract_date_from_filename(f)
    if file_date and (latest_date is None or file_date > latest_date):
        latest_date = file_date
        latest_file = f

if not latest_file:
    print("⚠️ Could not determine latest report from filename. Using first Excel file.")
    latest_file = excel_files[0]
else:
    print(f"✅ Latest Excel report determined: {os.path.basename(latest_file)} (Date: {latest_date.strftime('%Y-%m-%d')})")

# ---------- STEP 6: LOAD EXCEL INTO POSTGRES ----------
df = pd.read_excel(latest_file)

# Convert date columns
date_cols = ["MASTER_DUE_DATE", "RETURNED_AT", "TRANS_RECEIVED_AT", "TRANS_SETTLED_AT", "TRANS_TRANSFER_DATE"]
for col in date_cols:
    if col in df.columns:
        df[col] = pd.to_datetime(df[col], errors='coerce')

conn = psycopg2.connect(**DB_CONFIG)
cursor = conn.cursor()

# Create table if not exists
create_table_query = """
CREATE TABLE IF NOT EXISTS yesbank_ecollect (
    AMOUNT NUMERIC,
    BENE_ACCOUNT_NO VARCHAR(50),
    CREDIT_REF VARCHAR(100),
    CUSTOMER_SUBCODE VARCHAR(100),
    CUSTOMER_SUBCODE_EMAIL VARCHAR(255),
    CUSTOMER_SUBCODE_MOBILE VARCHAR(50),
    CUST_CODE VARCHAR(50),
    CUST_NAME VARCHAR(255),
    INVOICE_AMT_TOL_PCT NUMERIC,
    INVOICE_NO VARCHAR(100),
    KEY_VALUE VARCHAR(100),
    MAIN_CREDIT_AC VARCHAR(100),
    MASTER_DUE_DATE TIMESTAMP,
    MASTER_RMTR_ACCT_NO VARCHAR(100),
    MASTER_RMTR_ADDRESS TEXT,
    MASTER_RMTR_NAME VARCHAR(255),
    MAX_CREDIT_AMT NUMERIC,
    MIN_CREDIT_AMT NUMERIC,
    NOTIFY_RESULT VARCHAR(100),
    NOTIFY_STATUS VARCHAR(100),
    ORD VARCHAR(50),
    RECR_IFSC VARCHAR(50),
    REMITTER_CODE VARCHAR(50),
    RETURNED_AT TIMESTAMP,
    RETURN_REF VARCHAR(100),
    RMTR_ACCOUNT_IFSC VARCHAR(50),
    RMTR_ACCOUNT_NO VARCHAR(100),
    RMTR_ADD TEXT,
    RMTR_EMAIL VARCHAR(255),
    RMTR_EMAIL_NOTIFY_REF VARCHAR(100),
    RMTR_FULL_NAME VARCHAR(255),
    RMTR_MOBILE VARCHAR(50),
    RMTR_SMS_NOTIFY_REF VARCHAR(100),
    RMTR_TO_BENE_NOTE TEXT,
    SETTLE_REF VARCHAR(100),
    TRANSACTION_REF_NO VARCHAR(100),
    TRANSFER_TYPE VARCHAR(50),
    TRANS_RECEIVED_AT TIMESTAMP,
    TRANS_SETTLED_AT TIMESTAMP,
    TRANS_STATUS VARCHAR(50),
    TRANS_TRANSFER_DATE TIMESTAMP,
    UDF11 VARCHAR(255),
    UDF12 VARCHAR(255),
    UDF13 VARCHAR(255),
    UDF14 VARCHAR(255),
    UDF15 VARCHAR(255),
    UDF16 VARCHAR(255),
    UDF17 VARCHAR(255),
    UDF18 VARCHAR(255),
    UDF19 VARCHAR(255),
    UDF20 VARCHAR(255),
    VALIDATION_RESULT VARCHAR(100),
    VALIDATION_STATUS VARCHAR(100)
);
"""
cursor.execute(create_table_query)
conn.commit()

# Insert data
columns = ','.join(df.columns)
placeholders = ','.join(['%s'] * len(df.columns))
insert_query = f"INSERT INTO yesbank_ecollect ({columns}) VALUES ({placeholders})"

for _, row in df.iterrows():
    values = []
    for v in row:
        if pd.isna(v):   # catches NaN and NaT
            values.append(None)
        else:
            values.append(v)
    cursor.execute(insert_query, tuple(values))
    

conn.commit()
cursor.close()
conn.close()
print("✅ Latest E-Collect report loaded into Postgres successfully!")