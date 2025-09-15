from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
import time, pandas as pd, psycopg2, os, glob
from datetime import datetime


# ------------------- Initialize Driver -------------------
def init_driver(start_url, download_dir):
    options = webdriver.ChromeOptions()
    options.add_argument("--start-maximized")
    prefs = {"download.default_directory": download_dir}
    options.add_experimental_option("prefs", prefs)
    driver = webdriver.Chrome(options=options)
    driver.get(start_url)
    return driver


# ------------------- Login -------------------
def login_to_portal(driver, username, password):
    wait = WebDriverWait(driver, 20)
    try:
        wait.until(EC.presence_of_element_located((By.XPATH, "//*[@id='textinput-8-input-9']"))).send_keys(username)
        driver.find_element(By.XPATH, "//button[@data-testid='unified-auth-continue']").click()

        password_field = wait.until(
            EC.presence_of_element_located((By.XPATH, "//input[@placeholder='Enter password']"))
        )
        password_field.send_keys(password)
        driver.find_element(By.XPATH, "//button[@data-analytics-name='Login']").click()
        time.sleep(10)  # wait for redirect
    except TimeoutException:
        print("Login failed - element not found.")
        driver.quit()
        raise

    # Announcement popup
    try:
        ok_button = WebDriverWait(driver, 5).until(
            EC.element_to_be_clickable((By.XPATH, "//*[normalize-space(text())='Ok, got it!']"))
        )
        ok_button.click()
        print("Clicked on 'Ok, got it!' popup.")
    except TimeoutException:
        print("Popup not found. Continuing...")


# ------------------- Navigate & Download Reports -------------------
def navigate_and_download_reports(driver, reports_list, download_dir):
    wait = WebDriverWait(driver, 20)
    downloaded_files = {}

    # Click Reports tab
    print("Navigating to Reports...")
    wait.until(EC.element_to_be_clickable((By.XPATH, "//p[text()='Reports']"))).click()

    # Click Download Report button
    print("Clicking Download Report button...")
    wait.until(EC.element_to_be_clickable((By.XPATH, "//button[.//div[text()='Download Report']]"))).click()
    time.sleep(3)  # allow modal to appear

    for report_name in reports_list:
        if not isinstance(report_name, str):
            raise ValueError(f"Invalid report name: {report_name}. Must be a string.")

        try:
            print(f"\n=== Processing report: {report_name} ===")

            # Step 1: Click on report dropdown
            dropdown_button = wait.until(
                EC.element_to_be_clickable((By.XPATH, "//p[normalize-space()='Select A Report']/ancestor::button"))
            )
            dropdown_button.click()

            # Step 2: Select the report dynamically
            report_option = wait.until(EC.element_to_be_clickable(
                (By.XPATH, f"//button[@role='option']//p[normalize-space()='{report_name}']")
            ))
            driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", report_option)
            report_option.click()

            # Step 3: Select Excel format
            format_dropdown = wait.until(EC.element_to_be_clickable(
                (By.XPATH, "//p[normalize-space()='Excel, CSV or More']/ancestor::button")
            ))
            format_dropdown.click()
            excel_option = wait.until(EC.element_to_be_clickable((By.XPATH, "//p[normalize-space()='Excel']")))
            excel_option.click()

             # Step 4: Click 'What will you receive in this report?'
            print("Step 5: Click 'What will you receive in this report?'...")
            what_will_you_receive = wait.until(
            EC.element_to_be_clickable((By.XPATH, "//p[normalize-space()='What will you receive in this report?']"))
        )
            driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", what_will_you_receive)
            what_will_you_receive.click()

            # Step 5: Select duration → Yesterday
            duration_dropdown = wait.until(EC.element_to_be_clickable(
                (By.XPATH, "//p[normalize-space()='Select duration covered in each report']/ancestor::button")
            ))
            duration_dropdown.click()
            yesterday_option = wait.until(
                EC.element_to_be_clickable((By.XPATH, "//button[@role='option']//p[normalize-space()='Yesterday']"))
            )
            yesterday_option.click()

            # Step 6: Start download
            start_download_button = wait.until(
                EC.element_to_be_clickable((By.XPATH, "//div[text()='Start Download']"))
            )
            start_download_button.click()
            print(f"✅ {report_name} report queued for download.")

            # Step 7: Go to Downloads page
            driver.get("https://dashboard.razorpay.com/app/reports/downloads")

            first_report_xpath = "(//button[@aria-label='Download Report'])[1]"
            first_report = wait.until(EC.presence_of_element_located((By.XPATH, first_report_xpath)))
            initial_text = first_report.text.strip()

            def new_report_buffered(driver):
                try:
                    current_text = driver.find_element(By.XPATH, first_report_xpath).text.strip()
                    return current_text if current_text and current_text != initial_text else False
                except:
                    return False

            try:
                buffered_text = WebDriverWait(driver, 40, poll_frequency=1).until(new_report_buffered)
                print("✅ New report buffered:", buffered_text)
            except TimeoutException:
                print("⚠️ Timeout waiting for new report. Using the latest available.")

            latest_download_button = wait.until(EC.element_to_be_clickable((By.XPATH, first_report_xpath)))
            latest_download_button.click()
            print("Clicked on the newest report.")

            # Wait for file to appear in Downloads
            downloaded_file = None
            for _ in range(60):  # up to 2 minutes
                downloaded_file = get_latest_excel(download_dir)
                if downloaded_file:
                    print(f"✅ File downloaded: {downloaded_file}")
                    break
                time.sleep(2)

            if downloaded_file:
                downloaded_files[report_name] = downloaded_file
            else:
                print(f"❌ No Excel file found for {report_name}.")

        except TimeoutException as e:
            print(f"⚠️ Timeout while processing {report_name} report: {e}")

    return downloaded_files


# ------------------- Get Latest Excel -------------------
def get_latest_excel(download_dir):
    list_of_files = glob.glob(f"{download_dir}/*.xlsx")
    if not list_of_files:
        return None
    return max(list_of_files, key=os.path.getctime)


# ------------------- Insert Data to PostgreSQL -------------------
def insert_data_to_db(excel_file, table_name):
    df = pd.read_excel(excel_file)

    for col in ['created_at', 'on_hold_until', 'settlement_initiated_on']:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce', dayfirst=True)

    try:
        conn = psycopg2.connect(
            host="qa.clz6pqqb00si.ap-south-1.rds.amazonaws.com",
            database="lms",
            user="postgres",
            password="Gds80p^M*fOx",
            port="5432",
            sslmode="require"
        )
        cursor = conn.cursor()

        for _, row in df.iterrows():
            cursor.execute(f"""
                INSERT INTO {table_name} (
                    id, amount, currency, payment_id, notes, receipt,
                    created_at, contact, email, ARN, status, upi_mode,inserted_at 
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW()
                )
                ON CONFLICT (id) DO UPDATE
                SET amount      = EXCLUDED.amount,
                    currency    = EXCLUDED.currency,
                    payment_id  = EXCLUDED.payment_id,
                    notes       = EXCLUDED.notes,
                    receipt     = EXCLUDED.receipt,
                    created_at  = EXCLUDED.created_at,
                    contact     = EXCLUDED.contact,
                    email       = EXCLUDED.email,
                    ARN         = EXCLUDED.ARN,
                    status      = EXCLUDED.status,
                    upi_mode    = EXCLUDED.upi_mode,
                    inserted_at  = NOW()
            """, (
                row.get('id'), row.get('amount'), row.get('currency'), row.get('payment_id'),
                row.get('notes'), row.get('receipt'), row.get('created_at'),
                row.get('contact'), row.get('email'), row.get('ARN'),
                row.get('status'), row.get('upi_mode')
            ))

        conn.commit()
        print(f"✅ Data upserted successfully into {table_name}!")

    except Exception as e:
        print(f"❌ Database insertion failed: {e}")
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()
    
    # ------------------- Main Execution -------------------
if __name__ == "__main__":
    portal_link = "https://dashboard.razorpay.com/app/reports/downloads"
    username = "payments@progfin.com"
    password = "hhpl@123"
    download_dir = "/Users/miniagrawal/Downloads"

    driver = None
    try:
        driver = init_driver(portal_link, download_dir)
        login_to_portal(driver, username, password)

        # Always a list of strings
        reports = ["Refunds"]
        downloaded_files = navigate_and_download_reports(driver, reports, download_dir)

        for report_name, file_path in downloaded_files.items():
            insert_data_to_db(file_path, "razorpay1_refunds")

    except Exception as e:
        print(f"❌ Script failed: {e}")

    finally:
        if driver:
            driver.quit()
