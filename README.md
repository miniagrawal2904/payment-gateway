### PD Questionnaire Autofill Script

This repository contains a Python Playwright script that can autofill the PD Questionnaire form fields (FY22, FY23, Estimated Current FY, YTD Turnover) and optionally add rows to the "Other Group Companies" table.

### Prerequisites
- Python 3.9+
- Internet access

### Quick start
1. Create and activate a virtual environment:
   - macOS/Linux:
     ```bash
     python3 -m venv .venv && source .venv/bin/activate
     ```
   - Windows (PowerShell):
     ```bash
     python -m venv .venv; .venv\\Scripts\\Activate.ps1
     ```
2. Install dependencies and Playwright browsers:
   ```bash
   pip install -r requirements.txt
   python -m playwright install --with-deps
   ```
3. Copy `.env.example` to `.env` and fill values as needed (URL and optional login selectors):
   ```bash
   cp .env.example .env
   ```
4. Run once in manual login mode to capture an authenticated storage state (headed browser):
   ```bash
   python -m pdq_autofill.main --url "$PDQ_URL" --headed --manual-login --storage .auth/storage.json
   ```
   - Log into the application in the opened browser. Once the PD Questionnaire page is fully loaded, the script will save your session to `.auth/storage.json` and exit.

5. Subsequent runs (no login needed), using sample data:
   ```bash
   python -m pdq_autofill.main --url "$PDQ_URL" --storage .auth/storage.json --data pdq_autofill/data/sample_pdq.json
   ```

### Data file format
Edit or provide your own JSON file. Keys used:
```json
{
  "fy22_turnover": 120.5,
  "fy23_turnover": 150.75,
  "estimated_current_fy_turnover": 160.0,
  "ytd_turnover": 80.25,
  "other_group_companies": [
    {
      "entity_name": "Acme Trading Co.",
      "business_type": "Proprietorship",
      "brand": "Acme",
      "promoter_name": "John Doe",
      "relationship_with_promoter": "Self",
      "latest_fy_turnover_lakhs": 95.0
    }
  ]
}
```

### Notes and selectors
- The script prefers robust label-based selectors. If your environment uses custom markup, tweak the `FIELD_LABELS` and `TABLE_COLUMN_LABELS` in `pdq_autofill/main.py`.
- If a dedicated login page exists, you can set optional CSS selectors in `.env` to enable automated login instead of manual login: `LOGIN_USER_SELECTOR`, `LOGIN_PASS_SELECTOR`, `LOGIN_SUBMIT_SELECTOR`, `AUTH_USERNAME`, `AUTH_PASSWORD`.

### Troubleshooting
- If a field cannot be found, the script logs a warning and continues. Adjust the label text or selectors in the script if needed.
- To see what the script sees, run with `--headed`.
- To slow actions for debugging, add `--slowmo 250`.
# payment-gateway