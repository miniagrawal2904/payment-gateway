import os
import base64
import pandas as pd
import psycopg2
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
TABLE_NAME = "bapa_report"


# ---------- FUNCTIONS ----------
def authenticate_graph():
    """Authenticate with Microsoft Graph API and return headers with token."""
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
    """Fetch latest email from given sender and subject."""
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
    """Download attachment from email and return file path."""
    url = f"https://graph.microsoft.com/v1.0/me/messages/{email_id}/attachments"
    att_r = requests.get(url, headers=headers)
    attachments = att_r.json().get("value", [])

    for att in attachments:
        if att["@odata.type"] == "#microsoft.graph.fileAttachment" and att["name"] == expected_file:
            path = os.path.join(download_dir, att["name"])
            with open(path, "wb") as f:
                f.write(base64.b64decode(att["contentBytes"]))
            print(f"✅ Downloaded: {path}")
            return path

    raise Exception(f"❌ No attachment found named {expected_file}")


def load_csv(path):
    """Load CSV file into DataFrame."""
    return pd.read_csv(path)


def connect_db():
    """Connect to PostgreSQL."""
    return psycopg2.connect(**DB_CONFIG)


def create_table(cursor):
    """Create table if not exists."""
    query = f"""
    CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
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
    cursor.execute(query)


def insert_data(cursor, df):
    """Insert DataFrame into Postgres table."""
    columns = ",".join(df.columns)
    placeholders = ",".join(["%s"] * len(df.columns))
    insert_query = f"INSERT INTO {TABLE_NAME} ({columns}) VALUES ({placeholders})"

    for _, row in df.iterrows():
        values = [None if pd.isna(v) else v for v in row]
        cursor.execute(insert_query, tuple(values))


# ---------- MAIN ----------
def main():
    headers = authenticate_graph()
    email = get_latest_email(headers, SENDER_EMAIL, SUBJECT_LINE)
    file_path = download_attachment(headers, email["id"], EXPECTED_FILE, DOWNLOAD_DIR)

    df = load_csv(file_path)

    conn = connect_db()
    cursor = conn.cursor()

    create_table(cursor)
    conn.commit()

    insert_data(cursor, df)
    conn.commit()

    cursor.close()
    conn.close()
    print("✅ Bapa report loaded into Postgres successfully!")


if __name__ == "__main__":
    main()
