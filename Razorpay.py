from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
import time, pandas as pd, psycopg2, os, glob
from datetime import datetime

# ------------------- Initialize Driver -------------------
def init_driver(start_url):
    options = webdriver.ChromeOptions()
    options.add_argument("--start-maximized")
    prefs = {"download.default_directory": "/Users/miniagrawal/Downloads"}
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
        time.sleep(5)
        password_field = wait.until(EC.presence_of_element_located((By.XPATH, "//input[@placeholder='Enter password']")))
        password_field.send_keys(password)
        driver.find_element(By.XPATH, "//button[@data-analytics-name='Login']").click()
        time.sleep(15)
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

# ------------------- Navigate & Download Report -------------------
def navigate_and_download_reports(driver, reports_list):
    wait = WebDriverWait(driver, 20)

    # Click Reports tab
    print("Navigating to Reports...")
    wait.until(EC.element_to_be_clickable((By.XPATH, "//p[text()='Reports']"))).click()

    # Click Download Report button
    print("Clicking Download Report button...")
    wait.until(EC.element_to_be_clickable((By.XPATH, "//button[.//div[text()='Download Report']]"))).click()
    time.sleep(5)  # wait for modal to load

    for report_to_select in reports_list:
        try:
            print(f"\n=== Processing report: {report_to_select} ===")

            # Step 1: Open report dropdown
            print("Step 1: Click on report dropdown...")
            dropdown = wait.until(EC.element_to_be_clickable((By.XPATH, "//button[contains(@placeholder, 'Select A Report')]")))
            driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", dropdown)
            dropdown.click()

            # Step 2: Select the report dynamically (Transfers / Payments / Refunds)
            print(f"Step 2: Select '{report_to_select}' from dropdown...")
            report_option = wait.until(EC.element_to_be_clickable(
                (By.XPATH, f"//button[@role='option']//p[normalize-space()='{report_to_select}']")
            ))
            driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", report_option)
            report_option.click()

            # Step 3: Select Excel format
            print("Step 3: Click on format dropdown...")
            format_dropdown = wait.until(EC.element_to_be_clickable(
                (By.XPATH, "//p[normalize-space()='Excel, CSV or More']/ancestor::button")
            ))
            driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", format_dropdown)
            format_dropdown.click()

            print("Step 4: Select 'Excel' report...")
            excel_option = wait.until(EC.element_to_be_clickable((By.XPATH, "//p[normalize-space()='Excel']")))
            driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", excel_option)
            excel_option.click()

            # Step 5: Click 'What will you receive in this report?'
            print("Step 5: Click 'What will you receive in this report?'...")
            what_will_you_receive = wait.until(
                EC.element_to_be_clickable((By.XPATH, "//p[normalize-space()='What will you receive in this report?']"))
            )
            driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", what_will_you_receive)
            what_will_you_receive.click()

            # Step 6: Select duration
            print("Step 6: Click on Select duration dropdown...")
            duration_dropdown = wait.until(
                EC.element_to_be_clickable((By.XPATH, "//p[normalize-space()='Select duration covered in each report']/ancestor::button"))
            )
            driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", duration_dropdown)
            duration_dropdown.click()

            print("Step 7: Select 'Yesterday' duration...")
            yesterday_option = wait.until(
                EC.element_to_be_clickable((By.XPATH, "//button[@role='option']//p[normalize-space()='Yesterday']"))
            )
            driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", yesterday_option)
            yesterday_option.click()

            # Step 8: Start download
            print("Step 8: Click Start Download...")
            start_download_button = wait.until(
                EC.element_to_be_clickable((By.XPATH, "//div[text()='Start Download']"))
            )
            driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", start_download_button)
            start_download_button.click()

            print(f"✅ {report_to_select} report queued for download.")

            # Navigate to Downloads page
            print("Navigating to Downloads page...")
            driver.get("https://dashboard.razorpay.com/app/reports/downloads")

            # Instead of a fixed 20s sleep, wait only until the first report appears
            first_report_xpath = "(//button[@aria-label='Download Report'])[1]"
            first_report = wait.until(EC.presence_of_element_located((By.XPATH, first_report_xpath)))

            print("Waiting for the newest report to buffer...")

            # Store initial reference (if any)
            initial_text = first_report.text.strip()

            # Use WebDriverWait with a custom polling function (faster than manual loop)
            def new_report_buffered(driver):
                try:
                    current_text = driver.find_element(By.XPATH, first_report_xpath).text.strip()
                    return current_text if current_text and current_text != initial_text else False
                except:
                    return False

            try:
                buffered_text = WebDriverWait(driver, 30, poll_frequency=1).until(new_report_buffered)
                print("✅ New report buffered:", buffered_text)
            except TimeoutException:
                print("⚠️ Timeout waiting for new report. Using the latest available.")

            # Click the latest download button (no extra scrolling needed usually)
            latest_download_button = wait.until(EC.element_to_be_clickable((By.XPATH, first_report_xpath)))
            latest_download_button.click()
            print("Clicked on the newest report.")
            # Wait for file in Downloads
            download_dir = "/Users/miniagrawal/Downloads"
            downloaded_file = None
            for _ in range(60):  # wait up to 2 minutes
                downloaded_file = get_latest_excel(download_dir, report_to_select)
                if downloaded_file:
                    print(f"✅ File downloaded: {downloaded_file}")
                    break
                time.sleep(2)

            if not downloaded_file:
                print(f"❌ Script failed: No {report_to_select} Excel files found in {download_dir}.")

        except TimeoutException as e:
            print(f"⚠️ Timeout while processing {report_to_select} report: {e}")


# ------------------- Get Latest Excel -------------------
def get_latest_excel(download_dir, report_type):
    report_type = report_type.lower()
    if report_type == "transfers":
        pattern = "transfers*.xlsx"
    elif report_type == "payments":
        pattern = "payments*.xlsx"
    elif report_type == "refunds":
        pattern = "scrooge_refunds*.xlsx"
    else:
        raise ValueError(f"Unsupported report type: {report_type}")

    list_of_files = glob.glob(f"{download_dir}/{pattern}")
    if not list_of_files:
        return None
    return max(list_of_files, key=os.path.getctime)



# ------------------- Insert Data to PostgreSQL -------------------
def insert_data_to_db(excel_file, table_name):
    df = pd.read_excel(excel_file)
    for col in ['created_at', 'on_hold_until', 'settlement_initiated_on']:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], format='%d/%m/%Y %H:%M:%S', errors='coerce')

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
            created_at = row['created_at'].to_pydatetime() if pd.notnull(row.get('created_at')) else None
            on_hold_until = row['on_hold_until'].to_pydatetime() if pd.notnull(row.get('on_hold_until')) else None
            settlement_initiated_on = row['settlement_initiated_on'].to_pydatetime() if pd.notnull(row.get('settlement_initiated_on')) else None

            if table_name == "razorpay1_transfers": 
                # ---------------- TRANSFERS ----------------
                cursor.execute(f"""
                    INSERT INTO {table_name} (
                        id, source, recipient, recipient_details, amount, currency, 
                        amount_reversed, notes, fees, on_hold, on_hold_until, 
                        created_at, recipient_settlement_id, settlement_initiated_on, 
                        settlement_utr, settlement_status, tax
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (id) DO UPDATE
                    SET source = EXCLUDED.source,
                        recipient = EXCLUDED.recipient,
                        recipient_details = EXCLUDED.recipient_details,
                        amount = EXCLUDED.amount,
                        currency = EXCLUDED.currency,
                        amount_reversed = EXCLUDED.amount_reversed,
                        notes = EXCLUDED.notes,
                        fees = EXCLUDED.fees,
                        on_hold = EXCLUDED.on_hold,
                        on_hold_until = EXCLUDED.on_hold_until,
                        created_at = EXCLUDED.created_at,
                        recipient_settlement_id = EXCLUDED.recipient_settlement_id,
                        settlement_initiated_on = EXCLUDED.settlement_initiated_on,
                        settlement_utr = EXCLUDED.settlement_utr,
                        settlement_status = EXCLUDED.settlement_status,
                        tax = EXCLUDED.tax
                """, (
                    row.get('id'), row.get('source'), row.get('recipient'), row.get('recipient_details'),
                    row.get('amount'), row.get('currency'), row.get('amount_reversed'),
                    row.get('notes'), row.get('fees'), row.get('on_hold'),
                    on_hold_until, created_at, row.get('recipient_settlement_id'),
                    settlement_initiated_on, row.get('settlement_utr'),
                    row.get('settlement_status'), row.get('tax')
                ))

            elif table_name == "razorpay1_payments":
   
                 # ---------------- PAYMENTS ----------------
                        cursor.execute(f"""
                            INSERT INTO {table_name} (
                                id, amount, currency, status, order_id, invoice_id, international, method,
                                amount_refunded, amount_transferred, refund_status, captured, description,
                                card_id, card, bank, wallet, vpa, email, contact, notes, fee, tax,
                                error_code, error_description, created_at, card_type, card_network,
                                Auth_code, Payments_ARN, Payments_RRN, flow, inserted_at, unique_id
                            ) VALUES (
                                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                %s, %s, NOW(), %s
                            )
                            ON CONFLICT (id) DO UPDATE
                            SET amount = EXCLUDED.amount,
                                currency = EXCLUDED.currency,
                                status = EXCLUDED.status,
                                order_id = EXCLUDED.order_id,
                                invoice_id = EXCLUDED.invoice_id,
                                international = EXCLUDED.international,
                                method = EXCLUDED.method,
                                amount_refunded = EXCLUDED.amount_refunded,
                                amount_transferred = EXCLUDED.amount_transferred,
                                refund_status = EXCLUDED.refund_status,
                                captured = EXCLUDED.captured,
                                description = EXCLUDED.description,
                                card_id = EXCLUDED.card_id,
                                card = EXCLUDED.card,
                                bank = EXCLUDED.bank,
                                wallet = EXCLUDED.wallet,
                                vpa = EXCLUDED.vpa,
                                email = EXCLUDED.email,
                                contact = EXCLUDED.contact,
                                notes = EXCLUDED.notes,
                                fee = EXCLUDED.fee,
                                tax = EXCLUDED.tax,
                                error_code = EXCLUDED.error_code,
                                error_description = EXCLUDED.error_description,
                                created_at = EXCLUDED.created_at,
                                card_type = EXCLUDED.card_type,
                                card_network = EXCLUDED.card_network,
                                Auth_code = EXCLUDED.Auth_code,
                                Payments_ARN = EXCLUDED.Payments_ARN,
                                Payments_RRN = EXCLUDED.Payments_RRN,
                                flow = EXCLUDED.flow,
                                inserted_at = NOW(),
                                unique_id = EXCLUDED.unique_id
                        """, (
                            row.get('id'), row.get('amount'), row.get('currency'), row.get('status'),
                            row.get('order_id'), row.get('invoice_id'), row.get('international'),
                            row.get('method'), row.get('amount_refunded'), row.get('amount_transferred'),
                            row.get('refund_status'), row.get('captured'), row.get('description'),
                            row.get('card_id'), row.get('card'), row.get('bank'), row.get('wallet'),
                            row.get('vpa'), row.get('email'), row.get('contact'), row.get('notes'),
                            row.get('fee'), row.get('tax'), row.get('error_code'),
                            row.get('error_description'), row.get('created_at'),
                            row.get('card_type'), row.get('card_network'), row.get('Auth_code'),
                            row.get('Payments_ARN'), row.get('Payments_RRN'), row.get('flow'),
                            row.get('unique_id')
                        ))


            elif table_name == "razorpay1_refunds":
                # ---------------- REFUNDS ----------------
                cursor.execute(f"""
                    INSERT INTO {table_name} (
                        id, amount, currency, payment_id, notes, receipt,
                        created_at, contact, email, ARN, status, upi_mode
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                    )
                    ON CONFLICT (id) DO UPDATE
                    SET amount = EXCLUDED.amount,
                        currency = EXCLUDED.currency,
                        payment_id = EXCLUDED.payment_id,
                        notes = EXCLUDED.notes,
                        receipt = EXCLUDED.receipt,
                        created_at = EXCLUDED.created_at,
                        contact = EXCLUDED.contact,
                        email = EXCLUDED.email,
                        ARN = EXCLUDED.ARN,
                        status = EXCLUDED.status,
                        upi_mode = EXCLUDED.upi_mode
                """, (
                    row.get('id'), row.get('amount'), row.get('currency'), row.get('payment_id'),
                    row.get('notes'), row.get('receipt'), created_at,
                    row.get('contact'), row.get('email'), row.get('ARN'),
                    row.get('status'), row.get('upi_mode')
                ))

        conn.commit()
        print(f"Data upserted successfully into {table_name}!")

    except Exception as e:
        print(f"Database insertion failed: {e}")
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

    def safe_download(report_name, table_name):
        """
        Wrapper to handle session crashes:
        - retries once by reopening browser & re-logging in
        """
        global driver
        try:
            print(f"=== Processing report: {report_name} ===")
            navigate_and_download_reports(driver, [report_name])
            latest_file = get_latest_excel(download_dir, report_name.lower())
            insert_data_to_db(latest_file, table_name)
        except Exception as e:
            if "invalid session id" in str(e).lower() or "session deleted" in str(e).lower():
                print(f"⚠️ Session crashed while processing {report_name}, restarting browser...")
                try:
                    driver.quit()
                except:
                    pass
                driver = init_driver(portal_link)
                login_to_portal(driver, username, password)
                print(f"🔄 Retrying {report_name} report after restart...")
                navigate_and_download_reports(driver, [report_name])
                latest_file = get_latest_excel(download_dir, report_name.lower())
                insert_data_to_db(latest_file, table_name)
            else:
                raise  # re-raise if error is not session-related

    driver = None
    try:
        # Start browser
        driver = init_driver(portal_link)
        login_to_portal(driver, username, password)

        # ---------- Transfers ----------
        safe_download("Transfers", "razorpay1_transfers")

        # ---------- Payments ----------
        safe_download("Payments", "razorpay1_payments")

        # ---------- Refunds ----------
        safe_download("Refunds", "razorpay1_refunds")

        print("✅ All reports downloaded and inserted successfully.")

    except Exception as e:
        print(f"❌ Script failed: {e}")

    finally:
        if driver:
            driver.quit()
