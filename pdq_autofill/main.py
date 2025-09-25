import argparse
import json
import os
import sys
from dataclasses import dataclass
from typing import Dict, List, Optional

from dotenv import load_dotenv
from playwright.sync_api import Browser, BrowserContext, Page, TimeoutError as PlaywrightTimeoutError, sync_playwright


FIELD_LABELS = {
    "fy22_turnover": [
        "a) FY 22",
        "FY 22",
        "FY22",
    ],
    "fy23_turnover": [
        "b) FY 23",
        "FY 23",
        "FY23",
    ],
    "estimated_current_fy_turnover": [
        "Estimated turnover for the current FY",
        "Estimated turnover for the current FY(Lakh)",
        "Estimated turnover",
    ],
    "ytd_turnover": [
        "YTD Turnover",
        "YTD Turnover (April'23 till date)(Lakh)",
        "YTD Turnover (April)",
    ],
}

TABLE_TITLE_TEXT = "Other Group Companies"

TABLE_COLUMN_LABELS = [
    ("entity_name", ["Entity Name", "Entity", "Company Name"]),
    ("business_type", ["Business Type", "Type"]),
    ("brand", ["Brand", "If in trade, mention Brand"]),
    ("promoter_name", ["Promoter Name", "Promoter"]),
    ("relationship_with_promoter", ["Relationship with Promoter", "Relationship"]),
    ("latest_fy_turnover_lakhs", ["Latest FY Turnover (Lakhs)", "Latest FY Turnover"]),
]


@dataclass
class ScriptOptions:
    url: str
    data_path: str
    storage_path: Optional[str]
    headed: bool
    manual_login: bool
    slowmo_ms: int


def read_json(path: str) -> Dict:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def launch_browser(headed: bool, slowmo_ms: int) -> Browser:
    playwright_context = sync_playwright().start()
    browser = playwright_context.chromium.launch(headless=not headed, slow_mo=slowmo_ms)
    # Attach playwright to browser for later stop
    browser._playwright_context = playwright_context  # type: ignore[attr-defined]
    return browser


def close_browser(browser: Browser) -> None:
    playwright_context = getattr(browser, "_playwright_context", None)
    browser.close()
    if playwright_context is not None:
        playwright_context.stop()


def login_if_configured(page: Page) -> bool:
    username = os.getenv("AUTH_USERNAME")
    password = os.getenv("AUTH_PASSWORD")
    user_sel = os.getenv("LOGIN_USER_SELECTOR")
    pass_sel = os.getenv("LOGIN_PASS_SELECTOR")
    submit_sel = os.getenv("LOGIN_SUBMIT_SELECTOR")

    if not (username and password and user_sel and pass_sel and submit_sel):
        return False

    try:
        page.wait_for_selector(user_sel, timeout=5000)
        page.fill(user_sel, username)
        page.fill(pass_sel, password)
        page.click(submit_sel)
        return True
    except PlaywrightTimeoutError:
        return False


def wait_until_loaded(page: Page) -> None:
    # Wait for the PD Questionnaire tab text to be visible or for any of the known fields
    for candidate in [
        "PD Questionnaire",
        *[labels[0] for labels in FIELD_LABELS.values()],
    ]:
        try:
            page.get_by_text(candidate, exact=False).first.wait_for(timeout=5000)
            return
        except PlaywrightTimeoutError:
            continue
    # Final safeguard: wait for network idle briefly
    page.wait_for_load_state("networkidle", timeout=5000)


def fill_field_by_labels(page: Page, label_texts: List[str], value: str) -> bool:
    # Try accessible label first
    for label in label_texts:
        try:
            page.get_by_label(label, exact=False).fill(str(value))
            return True
        except Exception:
            pass

    # Try finding text then nearest input
    for label in label_texts:
        try:
            text_locator = page.get_by_text(label, exact=False).first
            # Prefer the immediate input to the right or below
            # Heuristics using XPath to find the first input following the text
            input_locator = page.locator(
                f"xpath=(.//*[contains(normalize-space(.), '{label}')])[1]/following::input[1]"
            )
            if input_locator.count() > 0:
                input_locator.first.fill(str(value))
                return True
        except Exception:
            pass

    return False


def fill_pd_questionnaire_fields(page: Page, data: Dict) -> None:
    field_results: Dict[str, bool] = {}
    for key, labels in FIELD_LABELS.items():
        if key not in data:
            continue
        filled = fill_field_by_labels(page, labels, str(data[key]))
        field_results[key] = filled
        if not filled:
            print(f"[warn] Could not locate field for '{key}'. Check labels/selectors.")


def find_table_after_title(page: Page, title_text: str):
    try:
        header = page.get_by_text(title_text, exact=False).first
        # The table is usually near the header; search for the nearest table
        table = page.locator(
            "xpath=(.//*[contains(normalize-space(.), '" + title_text + "')])[1]/following::table[1]"
        )
        if table.count() > 0:
            return table.first
    except Exception:
        return None
    return None


def click_add_row_if_present(page: Page) -> None:
    # Look for a nearby button that adds a row
    for text in ["Add New Row", "Add Row", "+ Add", "Add"]:
        try:
            btn = page.get_by_role("button", name=text, exact=False).first
            btn.click()
            return
        except Exception:
            continue


def fill_table_row_by_headers(page: Page, table_locator, row_index: int, row_data: Dict) -> None:
    # Try to map inputs by column header text per TABLE_COLUMN_LABELS
    try:
        header_cells = table_locator.locator("thead tr th")
        body_rows = table_locator.locator("tbody tr")
        if body_rows.count() <= row_index:
            # Attempt to add rows until we have enough
            for _ in range(row_index - body_rows.count() + 1):
                click_add_row_if_present(page)
            body_rows = table_locator.locator("tbody tr")

        target_row = body_rows.nth(row_index)

        for key, header_aliases in TABLE_COLUMN_LABELS:
            if key not in row_data:
                continue
            # Find column index by matching header text
            col_index = None
            for i in range(header_cells.count()):
                header_text = header_cells.nth(i).inner_text().strip()
                for alias in header_aliases:
                    if alias.lower() in header_text.lower():
                        col_index = i
                        break
                if col_index is not None:
                    break

            if col_index is None:
                continue

            cell = target_row.locator("td").nth(col_index)
            input_in_cell = cell.locator("input, textarea, [contenteditable='true']").first
            if input_in_cell.count() > 0:
                input_in_cell.fill(str(row_data[key]))
            else:
                # Attempt to click and type
                try:
                    cell.click()
                    page.keyboard.type(str(row_data[key]))
                except Exception:
                    pass
    except Exception as exc:
        print(f"[warn] Could not fill table row {row_index + 1}: {exc}")


def fill_other_group_companies(page: Page, data: Dict) -> None:
    companies: List[Dict] = data.get("other_group_companies", [])
    if not companies:
        return

    table = find_table_after_title(page, TABLE_TITLE_TEXT)
    if table is None:
        print("[warn] Could not find 'Other Group Companies' table. Skipping table fill.")
        return

    # Ensure at least one row exists for each company
    for idx, company in enumerate(companies):
        fill_table_row_by_headers(page, table, idx, company)


def run(options: ScriptOptions) -> int:
    load_dotenv()
    browser = launch_browser(options.headed, options.slowmo_ms)
    context_kwargs = {}
    if options.storage_path and os.path.exists(options.storage_path):
        context_kwargs["storage_state"] = options.storage_path

    context: BrowserContext = browser.new_context(**context_kwargs)
    page: Page = context.new_page()
    page.set_default_timeout(15000)

    try:
        page.goto(options.url, wait_until="domcontentloaded")

        if options.manual_login and not options.storage_path:
            print("[info] Manual login enabled. Complete login in the opened browser.")
            logged_in_via_form = login_if_configured(page)
            wait_until_loaded(page)
            context.storage_state(path=".auth/storage.json")
            print("[info] Saved storage to .auth/storage.json")
            return 0

        if not options.manual_login:
            # Try automated login if configured; otherwise rely on storage or existing session
            _ = login_if_configured(page)

        wait_until_loaded(page)

        data = read_json(options.data_path)
        fill_pd_questionnaire_fields(page, data)
        fill_other_group_companies(page, data)

        # Optionally, take a screenshot for verification
        os.makedirs(".artifacts", exist_ok=True)
        screenshot_path = ".artifacts/pdq_after_fill.png"
        page.screenshot(path=screenshot_path, full_page=True)
        print(f"[info] Wrote screenshot: {screenshot_path}")

        return 0
    finally:
        context.close()
        close_browser(browser)


def parse_args(argv: Optional[List[str]] = None) -> ScriptOptions:
    parser = argparse.ArgumentParser(description="Autofill PD Questionnaire using Playwright")
    parser.add_argument("--url", default=os.getenv("PDQ_URL", ""), help="Target PD Questionnaire URL")
    parser.add_argument("--data", dest="data_path", default="pdq_autofill/data/sample_pdq.json", help="Path to JSON data file")
    parser.add_argument("--storage", dest="storage_path", default=None, help="Path to Playwright storage state JSON")
    parser.add_argument("--headed", action="store_true", help="Run with visible browser window")
    parser.add_argument("--manual-login", action="store_true", help="Open page for manual login and save storage")
    parser.add_argument("--slowmo", dest="slowmo_ms", type=int, default=0, help="Slow down actions by N ms for debugging")
    args = parser.parse_args(argv)

    if not args.url:
        print("[error] --url is required (or set PDQ_URL in environment)")
        parser.print_help()
        sys.exit(2)

    return ScriptOptions(
        url=args.url,
        data_path=args.data_path,
        storage_path=args.storage_path,
        headed=args.headed,
        manual_login=args.manual_login,
        slowmo_ms=args.slowmo_ms,
    )


if __name__ == "__main__":
    opts = parse_args()
    sys.exit(run(opts))

