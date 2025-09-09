import os
import time
import glob
import pandas as pd
import psycopg2
from pathlib import Path
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException

# ---------------- Config ----------------
Portal_link = "https://onboarding.payu.in/app/account/signin?first_visit_url=https%3A%2F%2Fpayu.in%2Fbusiness&last_visit_url=https%3A%2F%2Fpayu.in%2F"
Login = "payments@progfin.com"
Password = "Hhpl@123"
DOWNLOAD_DIR = str(Path.home() / "Downloads")   # change if needed

# ---------------- Selenium Setup ----------------
chrome_options = webdriver.ChromeOptions()
prefs = {"download.default_directory": DOWNLOAD_DIR,
         "download.prompt_for_download": False,
         "download.directory_upgrade": True,
         "safebrowsing.enabled": True}
chrome_options.add_experimental_option("prefs", prefs)

driver = webdriver.Chrome(options=chrome_options)
driver.get(Portal_link)
driver.maximize_window()
wait = WebDriverWait(driver, 20)

# ---------------- Login ----------------
wait.until(EC.presence_of_element_located((By.ID, "email"))).send_keys(Login)
driver.find_element(By.ID, "password").send_keys(Password)
driver.find_element(By.XPATH, "//button[@type='submit' and contains(text(), 'Login')]").click()

# ---------------- Select MID and Dashboard ----------------
MID = wait.until(EC.element_to_be_clickable((By.XPATH, "//span[text()='Progfin Private Limited']")))
MID.click()
wait.until(EC.element_to_be_clickable((By.XPATH, "//*[@id='app']/div[1]/div/div/button[3]"))).click()

# ---------------- Skip Tour ----------------
try:
    skip_tour = wait.until(EC.element_to_be_clickable((By.XPATH, "//p[text()='Skip tour']")), 5)
    skip_tour.click()
except TimeoutException:
    print("Skip tour not found, continuing...")

# ---------------- Navigate to Reports ----------------
wait.until(EC.element_to_be_clickable((By.ID, "reports"))).click()
wait.until(EC.element_to_be_clickable((By.XPATH, "//p[text()='Transaction']"))).click()
wait.until(EC.element_to_be_clickable((By.XPATH, "//button[p[text()='Transactions']]"))).click()
wait.until(EC.element_to_be_clickable((By.XPATH, "//*[@id='dropdown-option-TRANSACTION']/p"))).click()
wait.until(EC.element_to_be_clickable((By.XPATH, "//button[contains(.,'Today')]"))).click()
wait.until(EC.element_to_be_clickable((By.XPATH, "//p[normalize-space(text())='Yesterday']"))).click()
wait.until(EC.element_to_be_clickable((By.XPATH, "//p[normalize-space()='XLSX']"))).click()
wait.until(EC.element_to_be_clickable((By.XPATH, "//button[.//p[normalize-space()='Generate Report']]"))).click()
download_btn = wait.until(EC.element_to_be_clickable((By.XPATH, "//button[contains(@class,'tw-bg-primary-500') and .//p[text()='Download']]")))
time.sleep(1)  # Give a short pause to ensure the button is interactable
download_btn.click()

# ---------------- Wait for Download ----------------
time.sleep(20) 

# ---------------- Get Latest Excel ----------------
def get_latest_excel(download_dir, report_type="transaction"):
    pattern = f"{download_dir}/{report_type}*.xlsx"
    list_of_files = glob.glob(pattern)
    if not list_of_files:
        raise FileNotFoundError(f"No {report_type} Excel files found in {download_dir}")
    return max(list_of_files, key=os.path.getctime)

excel_file = get_latest_excel(DOWNLOAD_DIR)

def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    # normalize excel headers to match db schema
    df.columns = (
        df.columns.str.strip()                # remove extra spaces
                 .str.lower()                 # lowercase
                 .str.replace(" ", "_")       # spaces -> underscores
                 .str.replace("(inr)", "_inr")# fix amount(inr)
    )
    return df

def insert_data_to_db(excel_file, table_name="payu_transaction"):
    import pandas as pd
    import psycopg2
    from datetime import datetime

    # --- Load Excel ---
    df = pd.read_excel(excel_file)  # replace with your file path

    # --- Fix data types ---

    # Convert date columns to datetime, replace invalid dates with None
    date_cols = ['settlement_date', 'addedon', 'conversion_date']
    for col in date_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')

    # Replace NaN in numeric columns with None
    numeric_cols = ['amount', 'transaction_fee', 'discount', 'additional_charges', 
                    'amount_inr', 'cgst', 'sgst', 'igst', 'merchant_subvention_amount', 
                    'service_fees', 'tsp_charges', 'convenience_fee', 'mer_service_fee']
    for col in numeric_cols:
        if col in df.columns:
            df[col] = df[col].apply(lambda x: None if pd.isna(x) else x)

    # --- Connect to PostgreSQL ---
    conn = psycopg2.connect(
            host="qa.clz6pqqb00si.ap-south-1.rds.amazonaws.com",
            database="lms",
            user="postgres",
            password="Gds80p^M*fOx",
            port="5432",
            sslmode="require"
        )
    cursor = conn.cursor()

    # --- Prepare dynamic insert query ---
    rename_map = {
    "amount(inr)": "amount_inr",  # DB column is likely amount_inr
    # add more mappings if needed
}

    df.rename(columns=rename_map, inplace=True)
    columns = df.columns.tolist()

# Quote column names for SQL if they have special characters
    columns_quoted = [f'"{col}"' if not col.isidentifier() else col for col in columns]

    placeholders = ', '.join(['%s'] * len(columns_quoted))

    insert_query = f"""
        INSERT INTO payu_transaction ({', '.join(columns_quoted)})
        VALUES ({placeholders})
        ON CONFLICT (id) DO UPDATE SET
        {', '.join([f'{col} = EXCLUDED.{col}' for col in columns_quoted if col.lower() != 'id'])};
    """

# Insert rows
    for _, row in df.iterrows():
        # Use **original column names** to access DataFrame values
        values = [row[col] if not pd.isna(row[col]) else None for col in columns]
        try:
            print(insert_query)
            cursor.execute(insert_query, values)
        except Exception as e:
            conn.rollback()  # rollback only this failed command


    # --- Commit and close ---
    conn.commit()
    cursor.close()
    conn.close()
    print("✅ Data inserted successfully!")
insert_data_to_db(excel_file)