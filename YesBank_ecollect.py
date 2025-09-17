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
TABLE_NAME = "ecollect_report"


# ---------- FUNCTIONS ----------
def authenticate_graph():
    app = PublicClientApplication(
        CLIENT_ID, authority=f"https://login.microsoftonline.com/{TENANT_ID}"
    )
    flow = app.initiate_device_flow(scopes=SCOPES)
    print(flow["message"])
    result = app.acquire_token_by_device_flow(flow)
    if "access_token" not in result:
        raise Exception(f"❌ Authentication failed: {result.get('error_description')}")
    return {"Authorization": f"Bearer {result['access_token']}"}


def get_latest_email(headers, sender, subject):
    search_url = "https://graph.microsoft.com/v1.0/me/messages"
    params = {"$top": 50, "$orderby": "receivedDateTime desc"}
    mail_r = requests.get(search_url, headers=headers, params=params)
    mail_data = mail_r.json()

    filtered = [
        e for e in mail_data.get("value", [])
        if sender.lower() in e.get("from", {}).get("emailAddress", {}).get("address", "").lower()
        and subject.lower() in e.get("subject", "").lower()
    ]
    if not filtered:
        raise Exception(f"⚠️ No emails found from {sender} with subject '{subject}'")
    return filtered[0]


def download_attachment(headers, email_id, expected_file, download_dir):
    url = f"https://graph.microsoft.com/v1.0/me/messages/{email_id}/attachments"
    att_r = requests.get(url, headers=headers)
    attachments = att_r.json().get("value", [])
    file_paths = []

    for att in attachments:
        if att["@odata.type"] == "#microsoft.graph.fileAttachment" and att["name"] == expected_file:
            path = os.path.join(download_dir, att["name"])
            with open(path, "wb") as f:
                f.write(base64.b64decode(att["contentBytes"]))
            file_paths.append(path)
            print(f"✅ Downloaded: {path}")

    if not file_paths:
        raise Exception(f"❌ No attachment found named {expected_file}")
    return file_paths[0]


def clean_dataframe(path):
    df = pd.read_csv(path)
    date_cols = ["MASTER_DUE_DATE", "RETURNED_AT", "TRANS_RECEIVED_AT", "TRANS_SETTLED_AT", "TRANS_TRANSFER_DATE"]
    for col in date_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")
    return df


def connect_db():
    return psycopg2.connect(**DB_CONFIG)


def create_table(cursor):
    create_query = """
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
    cursor.execute(f"DROP TABLE IF EXISTS {TABLE_NAME};")
    cursor.execute(create_query)


def insert_data(cursor, df):
    df.columns = [c.lower() for c in df.columns]
    columns_str = ",".join(df.columns)
    placeholders = ",".join(["%s"] * len(df.columns))
    insert_query = f"INSERT INTO {TABLE_NAME} ({columns_str}) VALUES ({placeholders})"

    for _, row in df.iterrows():
        values = [None if pd.isna(v) else v for v in row]
        cursor.execute(insert_query, tuple(values))


# ---------- MAIN ----------
def main():
    headers = authenticate_graph()
    email = get_latest_email(headers, SENDER_EMAIL, SUBJECT_LINE)
    file_path = download_attachment(headers, email["id"], EXPECTED_FILE, DOWNLOAD_DIR)
    df = clean_dataframe(file_path)

    conn = connect_db()
    cursor = conn.cursor()

    create_table(cursor)
    conn.commit()
    print("✅ Table created successfully!")

    insert_data(cursor, df)
    conn.commit()

    cursor.close()
    conn.close()
    print("✅ E-Collect report loaded into Postgres successfully!")


if __name__ == "__main__":
    main()
