#!/usr/bin/env python3
"""
Attendance X Persona Views Pipeline
Auto-generated cron pipeline — attendance & persona view dashboards

Ported from the DS_batches_data notebook, wrapped for unattended GitHub
Actions execution:
  - Auth via env vars / service account instead of Colab's interactive auth.
  - requests.post is patched to use a retry-hardened Session (connection
    resets / 5xx / 429 are retried automatically), matching the fix applied
    to the main Assignment Automation Pipeline for card 9913-style failures.
  - Any uncaught exception exits non-zero so the GitHub Actions run goes red.
"""

import os
import sys
import json
import time
import traceback

import requests
import pandas as pd
import gspread
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from gspread_dataframe import set_with_dataframe
from google.oauth2.service_account import Credentials

start_time = time.time()

# -------------------- ENV & AUTH --------------------
sec = os.getenv("ASHRITHA_SECRET_KEY")
User_name = os.getenv("METABASE_USERNAME") or os.getenv("USERNAME")
service_account_json = os.getenv("SERVICE_ACCOUNT_JSON")
MB_URL = os.getenv("METABASE_URL")

missing = [n for n, v in [
    ("ASHRITHA_SECRET_KEY", sec),
    ("METABASE_USERNAME/USERNAME", User_name),
    ("SERVICE_ACCOUNT_JSON", service_account_json),
    ("METABASE_URL", MB_URL),
] if not v]
if missing:
    raise ValueError(f"❌ Missing environment variables: {', '.join(missing)}")

service_info = json.loads(service_account_json)
creds = Credentials.from_service_account_info(
    service_info,
    scopes=[
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ],
)
gc = gspread.authorize(creds)

METABASE_BASE = "https://metabase-lierhfgoeiwhr.newtonschool.co"

# -------------------- RETRY-HARDENED SESSION --------------------
# Same fix as the main Assignment Automation Pipeline: ConnectionError /
# ECONNRESET, 429, and 5xx are retried at the transport level instead of
# failing the whole job on the first hiccup.
SESSION = requests.Session()
_adapter = HTTPAdapter(
    max_retries=Retry(
        total=4,
        connect=4,
        read=2,
        backoff_factor=5,             # 5s, 10s, 20s, 40s
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=frozenset(["POST", "GET"]),
        raise_on_status=False,
        respect_retry_after_header=True,
    ),
    pool_connections=10,
    pool_maxsize=10,
)
SESSION.mount("https://", _adapter)
SESSION.mount("http://", _adapter)

# Every requests.post(...) call in the ported notebook code below now goes
# through the retry-hardened session automatically — no need to edit each
# call site individually.
requests.post = SESSION.post

token = None


def refresh_metabase_token():
    global token
    res = SESSION.post(
        MB_URL,
        headers={"Content-Type": "application/json"},
        json={"username": User_name, "password": sec},
        timeout=(15, 60),
    )
    res.raise_for_status()
    token = res.json()["id"]
    print("✅ Metabase session token refreshed")


refresh_metabase_token()

print("🔎 ENV CHECK")
print(f"   MB user           : {'[SET]' if User_name else '[MISSING]'}")
print(f"   SA client_email   : {service_info.get('client_email')}")
print(f"   Token acquired    : {bool(token)}")

# ═══════════════════════════════════════════════════════════════════════════
# PIPELINE BODY (ported from notebook cells: 56-75)
# ═══════════════════════════════════════════════════════════════════════════
try:
    # ===== COC - Attendance =====

    # ──────────────────────────────────────────────────────────────────────
    # Cell 57
    # ──────────────────────────────────────────────────────────────────────
    import re
    import pandas as pd
    import requests
    from gspread_dataframe import set_with_dataframe

    # =====================================================================
    # CONFIG
    # =====================================================================

    METABASE_HOST = "https://metabase-lierhfgoeiwhr.newtonschool.co"
    LECTURE_ATTENDANCE_CARD_ID = 11634  # "user-level-lecture-level-attendance"

    OUTPUT_SHEET_ID = "10ZBb4jOFuyQE6sBPIqKe9tvi8O1GmJJlaxapy58Y9sQ"  # <-- update if this should target a different sheet
    STUDENT_LEVEL_WORKSHEET = "Lecture-Level-Attendance-StudentLevel"

    # Only keep batches whose name mentions one of these modules (case-insensitive,
    # substring match -- e.g. matches "DS Spreadsheets - January 2026").
    # Edit this list if the module naming differs from what's assumed here.
    MODULE_FILTER = ["Spreadsheet", "SQL"]

    # Maps a substring found in batch_name -> display Module name (used for the
    # new "Module" column). Extend as more modules show up in your data.
    MODULE_PATTERNS = {
        "spreadsheet": "Spreadsheets",
        "sql": "SQL",
        "python": "Python",
        "power bi": "Power BI",
        "tableau": "Tableau",
        "machine learning": "Machine Learning",
    }

    # ID columns that arrive comma-formatted (e.g. "3,380") -- need the commas
    # stripped before they can be treated as numbers.
    ID_COLS = ["course_id", "user_id", "lecture_id"]

    NUMERIC_COLS = [
        "class_number",
        "overall_attendance",
        "live_attended_flag",
        "live_attendance",
        "recorded_attendance",
        "live_60_per_watched_attendance",
        "recorded_40_per_watched_attendance",
        "overall_70_per_watched_attendance",
        "pct_watched",
        "watch_live_mins",
        "watch_recorded_mins",
    ]


    def _clean_numeric(series: pd.Series) -> pd.Series:
        """Strips thousand-separator commas (e.g. '3,380' -> 3380) before converting to numeric."""
        return pd.to_numeric(
            series.astype(str).str.replace(",", "", regex=False).str.strip(),
            errors="coerce",
        )


    def _parse_datetime_flexible(series: pd.Series) -> pd.Series:
        parsed = pd.to_datetime(series, format="ISO8601", errors="coerce")
        still_missing = parsed.isna() & series.notna()
        if still_missing.any():
            parsed.loc[still_missing] = pd.to_datetime(
                series[still_missing], format="%B %d, %Y, %I:%M %p", errors="coerce"
            )
        return parsed


    def extract_module(batch_name) -> str:
        """Derives a clean Module name from batch_name (e.g. 'DS SQL - February 2026' -> 'SQL')."""
        if pd.isna(batch_name):
            return "Other"
        name_lower = str(batch_name).lower()
        for substring, display_name in MODULE_PATTERNS.items():
            if substring in name_lower:
                return display_name
        return "Other"


    def load_and_merge_lecture_attendance_data(metabase_card_id, data_type_name):
        """Load lecture-level attendance data + Groomers + Master Data, filtered to Spreadsheets/SQL batches."""

        print(f"📥 Loading {data_type_name} Data")

        res = requests.post(
            f"{METABASE_HOST}/api/card/{metabase_card_id}/query/json",
            headers={"Content-Type": "application/json", "X-Metabase-Session": token},
            timeout=3600,
        )
        res.raise_for_status()

        df_raw = pd.DataFrame(res.json())
        print(f"  ✓ Loaded {len(df_raw)} rows")

        # Clean comma-formatted ID columns (course_id, user_id, lecture_id come
        # back as strings like "3,380" from this export)
        for col in ID_COLS:
            if col in df_raw.columns:
                df_raw[col] = _clean_numeric(df_raw[col])

        df_raw["user_id"] = df_raw["user_id"].astype("Int64").astype(str).str.strip()

        # New columns in this version of the raw data: au_batch_name, au_start_date
        if "au_start_date" in df_raw.columns:
            df_raw["au_start_date"] = _parse_datetime_flexible(df_raw["au_start_date"])
        if "lecture_start_timestamp" in df_raw.columns:
            df_raw["lecture_start_timestamp"] = _parse_datetime_flexible(df_raw["lecture_start_timestamp"])

        for col in NUMERIC_COLS:
            if col in df_raw.columns:
                df_raw[col] = pd.to_numeric(df_raw[col], errors="coerce")

        # NEW: Module column, derived from batch_name (not au_batch_name --
        # au_batch_name is the generic AU program name and doesn't mention the
        # module; batch_name is the one with "DS SQL - February 2026" style text)
        df_raw["Module"] = df_raw["batch_name"].apply(extract_module)

        # Filter to Spreadsheets/SQL batches only
        module_pattern = "|".join(MODULE_FILTER)
        df_raw = df_raw[df_raw["batch_name"].str.contains(module_pattern, case=False, na=False)].copy()
        print(f"  ✓ {len(df_raw)} rows after filtering to batches matching: {MODULE_FILTER}")

        # Filter by approved students
        df_filtered = df_raw[df_raw["user_id"].isin(approved_ids)].copy()

        # Merge with Groomers
        df_merged = df_filtered.merge(
            df_groomers_clean[["user_id", "Enrolled Status", "Phase"]],
            on="user_id",
            how="left",
        )

        # Merge with Master Data (dynamically)
        if master_data_available:
            available_master_cols = ["user_id"]
            desired_cols = [
                 'Persona', 'Age Bracket', 'Apti Bucket', 'CTC Bracket', "Learner's Brackets",
                       '12th Bucket', 'Placeability Bucket', 'Background', 'Financial Status Cleaned','Placeability Buckets',
                       'Employment Status', 'Batch', 'Batch Name','Grad CGPA',' Grad bin','Working ','Work ex bracket',
                'Age bracket','CTC bracket','Fin Bucket'
            ]
            for col in desired_cols:
                if col in df_master.columns:
                    available_master_cols.append(col)

            df_merged = df_merged.merge(
                df_master[available_master_cols].drop_duplicates(subset="user_id"),
                on="user_id",
                how="left",
            )

        return df_merged


    # ═══════════════════════════════════════════════════════════════════════════
    # STEP 1: Load Groomers Data (Once)
    # ═══════════════════════════════════════════════════════════════════════════
    print("📥 STEP 1: Load Groomers Data")

    workbook_groomers = gc.open("Groomers")
    worksheet_groomers = workbook_groomers.worksheet("Groomers")
    data_groomers = worksheet_groomers.get_all_values()

    df_groomers = pd.DataFrame(data_groomers[1:], columns=data_groomers[0])
    df_groomers = df_groomers.rename(columns={"UserID": "user_id"})
    df_groomers["user_id"] = df_groomers["user_id"].astype(str).str.strip()

    df_groomers_clean = df_groomers[
        (df_groomers["Enrolled Status"] != "Refund Requested")
        & (df_groomers["Phase"] != "Unavailable")
        & (df_groomers["Enrolled Status"] != "DPD/Foreclosed")
    ].copy()

    approved_ids = df_groomers_clean["user_id"].unique().tolist()
    print(f"✓ Approved Students: {len(approved_ids)}")

    # ═══════════════════════════════════════════════════════════════════════════
    # STEP 2: Load Master Data (Once)
    # ═══════════════════════════════════════════════════════════════════════════
    print("\n📥 STEP 2: Load Master Data")

    try:
        workbook_master = gc.open("DS Full program - All Intake 2026")
        worksheet_master = workbook_master.worksheet("Master Data 2023-2026")
        data_master = worksheet_master.get_all_values()

        df_master = pd.DataFrame(data_master[1:], columns=data_master[0])
        df_master = df_master.rename(columns={"User ID ": "user_id"})
        df_master["user_id"] = df_master["user_id"].astype(str).str.strip()
        df_master = df_master[~df_master["Persona"].isin(["NF", "#N/A"]) & df_master["Persona"].notna()].copy()

        print(f"✓ Loaded {len(df_master)} rows from Master Data")
        master_data_available = True
    except Exception as e:
        print(f"⚠️  Could not load Master Data: {e}")
        master_data_available = False

    # ═══════════════════════════════════════════════════════════════════════════
    # STEP 3: Lecture-Level Attendance (from card 11634), Spreadsheets/SQL only
    # ═══════════════════════════════════════════════════════════════════════════
    print("\n" + "=" * 80)
    print("PROCESSING: LECTURE-LEVEL ATTENDANCE (SPREADSHEETS + SQL BATCHES ONLY)")
    print("=" * 80)

    df_lecture_attendance = load_and_merge_lecture_attendance_data(
        LECTURE_ATTENDANCE_CARD_ID, "Lecture-Level Attendance"
    )
    print(f"✓ Merged lecture-level attendance for {len(df_lecture_attendance)} approved student-lecture rows")

    if len(df_lecture_attendance) > 0:
        print(f"  Batches included: {sorted(df_lecture_attendance['batch_name'].unique())}")
        print(f"  Modules found: {sorted(df_lecture_attendance['Module'].unique())}")

    # ═══════════════════════════════════════════════════════════════════════════
    # STEP 4: Upload to Google Sheets (Student Level)
    # ═══════════════════════════════════════════════════════════════════════════
    print("\n" + "=" * 80)
    print("UPLOADING TO GOOGLE SHEETS")
    print("=" * 80)

    sheet = gc.open_by_key(OUTPUT_SHEET_ID)

    print(f"\n☁️  Uploading {STUDENT_LEVEL_WORKSHEET}...")
    try:
        worksheet = sheet.worksheet(STUDENT_LEVEL_WORKSHEET)
        worksheet.clear()
        set_with_dataframe(worksheet, df_lecture_attendance, include_index=False, include_column_header=True)
        print(f"  ✓ {STUDENT_LEVEL_WORKSHEET}")
    except Exception:
        worksheet = sheet.add_worksheet(STUDENT_LEVEL_WORKSHEET, rows=5000, cols=150)
        set_with_dataframe(worksheet, df_lecture_attendance, include_index=False, include_column_header=True)
        print(f"  ✓ Created {STUDENT_LEVEL_WORKSHEET}")

    # ===== WOW - Attendance =====

    # ──────────────────────────────────────────────────────────────────────
    # Cell 59
    # ──────────────────────────────────────────────────────────────────────
    import re
    import pandas as pd
    import requests
    from gspread_dataframe import set_with_dataframe

    # =====================================================================
    # CONFIG
    # =====================================================================

    METABASE_HOST = "https://metabase-lierhfgoeiwhr.newtonschool.co"
    LECTURE_ATTENDANCE_CARD_ID = 11789  # "user-level-lecture-level-attendance"

    OUTPUT_SHEET_ID = "1z72t5HaE3ombCHo6kbpM3bqhnuTEBNR6L8jncdQZ7Y8"  # <-- update if this should target a different sheet
    STUDENT_LEVEL_WORKSHEET = "Class-Grouped-Attendance"

    # Only keep batches whose name mentions one of these modules (case-insensitive,
    # substring match -- e.g. matches "DS Spreadsheets - January 2026").
    # Edit this list if the module naming differs from what's assumed here.
    MODULE_FILTER = ["Spreadsheet", "SQL"]

    # Maps a substring found in batch_name -> display Module name (used for the
    # new "Module" column). Extend as more modules show up in your data.
    MODULE_PATTERNS = {
        "spreadsheet": "Spreadsheets",
        "sql": "SQL",
        "python": "Python",
        "power bi": "Power BI",
        "tableau": "Tableau",
        "machine learning": "Machine Learning",
    }

    # ID columns that arrive comma-formatted (e.g. "3,380") -- need the commas
    # stripped before they can be treated as numbers.
    ID_COLS = ["course_id", "user_id"]

    NUMERIC_COLS = [
        "live_attended_flag",
        "live_attendance"
    ]


    def _clean_numeric(series: pd.Series) -> pd.Series:
        """Strips thousand-separator commas (e.g. '3,380' -> 3380) before converting to numeric."""
        return pd.to_numeric(
            series.astype(str).str.replace(",", "", regex=False).str.strip(),
            errors="coerce",
        )


    def _parse_datetime_flexible(series: pd.Series) -> pd.Series:
        parsed = pd.to_datetime(series, format="ISO8601", errors="coerce")
        still_missing = parsed.isna() & series.notna()
        if still_missing.any():
            parsed.loc[still_missing] = pd.to_datetime(
                series[still_missing], format="%B %d, %Y, %I:%M %p", errors="coerce"
            )
        return parsed


    def extract_module(batch_name) -> str:
        """Derives a clean Module name from batch_name (e.g. 'DS SQL - February 2026' -> 'SQL')."""
        if pd.isna(batch_name):
            return "Other"
        name_lower = str(batch_name).lower()
        for substring, display_name in MODULE_PATTERNS.items():
            if substring in name_lower:
                return display_name
        return "Other"


    def load_and_merge_lecture_attendance_data(metabase_card_id, data_type_name):
        """Load lecture-level attendance data + Groomers + Master Data, filtered to Spreadsheets/SQL batches."""

        print(f"📥 Loading {data_type_name} Data")

        res = requests.post(
            f"{METABASE_HOST}/api/card/{metabase_card_id}/query/json",
            headers={"Content-Type": "application/json", "X-Metabase-Session": token},
            timeout=3600,
        )
        res.raise_for_status()

        df_raw = pd.DataFrame(res.json())
        print(f"  ✓ Loaded {len(df_raw)} rows")

        # Clean comma-formatted ID columns (course_id, user_id, lecture_id come
        # back as strings like "3,380" from this export)
        for col in ID_COLS:
            if col in df_raw.columns:
                df_raw[col] = _clean_numeric(df_raw[col])

        df_raw["user_id"] = df_raw["user_id"].astype("Int64").astype(str).str.strip()

        # New columns in this version of the raw data: au_batch_name, au_start_date
        if "au_start_date" in df_raw.columns:
            df_raw["au_start_date"] = _parse_datetime_flexible(df_raw["au_start_date"])
        if "lecture_start_timestamp" in df_raw.columns:
            df_raw["lecture_start_timestamp"] = _parse_datetime_flexible(df_raw["lecture_start_timestamp"])

        for col in NUMERIC_COLS:
            if col in df_raw.columns:
                df_raw[col] = pd.to_numeric(df_raw[col], errors="coerce")

        # NEW: Module column, derived from batch_name (not au_batch_name --
        # au_batch_name is the generic AU program name and doesn't mention the
        # module; batch_name is the one with "DS SQL - February 2026" style text)
        df_raw["Module"] = df_raw["batch_name"].apply(extract_module)

        # Filter to Spreadsheets/SQL batches only
        module_pattern = "|".join(MODULE_FILTER)
        df_raw = df_raw[df_raw["batch_name"].str.contains(module_pattern, case=False, na=False)].copy()
        print(f"  ✓ {len(df_raw)} rows after filtering to batches matching: {MODULE_FILTER}")

        # Filter by approved students
        df_filtered = df_raw[df_raw["user_id"].isin(approved_ids)].copy()

        # Merge with Groomers
        df_merged = df_filtered.merge(
            df_groomers_clean[["user_id", "Enrolled Status", "Phase"]],
            on="user_id",
            how="left",
        )

        # Merge with Master Data (dynamically)
        if master_data_available:
            available_master_cols = ["user_id"]
            desired_cols = [
                 'Persona', 'Age Bracket', 'Apti Bucket', 'CTC Bracket', "Learner's Brackets",
                       '12th Bucket', 'Placeability Bucket', 'Background', 'Financial Status Cleaned','Placeability Buckets',
                       'Employment Status', 'Batch', 'Batch Name','Grad CGPA',' Grad bin','Working ','Work ex bracket',
                'Age bracket','CTC bracket','Fin Bucket'
            ]
            for col in desired_cols:
                if col in df_master.columns:
                    available_master_cols.append(col)

            df_merged = df_merged.merge(
                df_master[available_master_cols].drop_duplicates(subset="user_id"),
                on="user_id",
                how="left",
            )

        return df_merged


    # ═══════════════════════════════════════════════════════════════════════════
    # STEP 1: Load Groomers Data (Once)
    # ═══════════════════════════════════════════════════════════════════════════
    print("📥 STEP 1: Load Groomers Data")

    workbook_groomers = gc.open("Groomers")
    worksheet_groomers = workbook_groomers.worksheet("Groomers")
    data_groomers = worksheet_groomers.get_all_values()

    df_groomers = pd.DataFrame(data_groomers[1:], columns=data_groomers[0])
    df_groomers = df_groomers.rename(columns={"UserID": "user_id"})
    df_groomers["user_id"] = df_groomers["user_id"].astype(str).str.strip()

    df_groomers_clean = df_groomers[
        (df_groomers["Enrolled Status"] != "Refund Requested")
        & (df_groomers["Phase"] != "Unavailable")
        & (df_groomers["Enrolled Status"] != "DPD/Foreclosed")
    ].copy()

    approved_ids = df_groomers_clean["user_id"].unique().tolist()
    print(f"✓ Approved Students: {len(approved_ids)}")

    # ═══════════════════════════════════════════════════════════════════════════
    # STEP 2: Load Master Data (Once)
    # ═══════════════════════════════════════════════════════════════════════════
    print("\n📥 STEP 2: Load Master Data")

    try:
        workbook_master = gc.open("DS Full program - All Intake 2026")
        worksheet_master = workbook_master.worksheet("Master Data 2023-2026")
        data_master = worksheet_master.get_all_values()

        df_master = pd.DataFrame(data_master[1:], columns=data_master[0])
        df_master = df_master.rename(columns={"UserID": "user_id"})
        df_master["user_id"] = df_master["user_id"].astype(str).str.strip()
        df_master = df_master[~df_master["Persona"].isin(["NF", "#N/A"]) & df_master["Persona"].notna()].copy()

        print(f"✓ Loaded {len(df_master)} rows from Master Data")
        master_data_available = True
    except Exception as e:
        print(f"⚠️  Could not load Master Data: {e}")
        master_data_available = False

    # ═══════════════════════════════════════════════════════════════════════════
    # STEP 3: Lecture-Level Attendance (from card 11634), Spreadsheets/SQL only
    # ═══════════════════════════════════════════════════════════════════════════
    print("\n" + "=" * 80)
    print("PROCESSING: LECTURE-LEVEL ATTENDANCE (SPREADSHEETS + SQL BATCHES ONLY)")
    print("=" * 80)

    df_lecture_attendance = load_and_merge_lecture_attendance_data(
        LECTURE_ATTENDANCE_CARD_ID, "Lecture-Level Attendance"
    )
    print(f"✓ Merged lecture-level attendance for {len(df_lecture_attendance)} approved student-lecture rows")

    if len(df_lecture_attendance) > 0:
        print(f"  Batches included: {sorted(df_lecture_attendance['batch_name'].unique())}")
        print(f"  Modules found: {sorted(df_lecture_attendance['Module'].unique())}")

    # ═══════════════════════════════════════════════════════════════════════════
    # STEP 4: Upload to Google Sheets (Student Level)
    # ═══════════════════════════════════════════════════════════════════════════
    print("\n" + "=" * 80)
    print("UPLOADING TO GOOGLE SHEETS")
    print("=" * 80)

    sheet = gc.open_by_key(OUTPUT_SHEET_ID)

    print(f"\n☁️  Uploading {STUDENT_LEVEL_WORKSHEET}...")
    try:
        worksheet = sheet.worksheet(STUDENT_LEVEL_WORKSHEET)
        worksheet.clear()
        set_with_dataframe(worksheet, df_lecture_attendance, include_index=False, include_column_header=True)
        print(f"  ✓ {STUDENT_LEVEL_WORKSHEET}")
    except Exception:
        worksheet = sheet.add_worksheet(STUDENT_LEVEL_WORKSHEET, rows=5000, cols=150)
        set_with_dataframe(worksheet, df_lecture_attendance, include_index=False, include_column_header=True)
        print(f"  ✓ Created {STUDENT_LEVEL_WORKSHEET}")

    # ===== COC - Session Completion rate - =====

    # ──────────────────────────────────────────────────────────────────────
    # Cell 61
    # ──────────────────────────────────────────────────────────────────────
    import pandas as pd
    import requests
    from gspread_dataframe import set_with_dataframe

    # =====================================================================
    # CONFIG
    # =====================================================================

    METABASE_HOST = "https://metabase-lierhfgoeiwhr.newtonschool.co"
    SESSION_COMPLETION_CARD_ID = 11635  # "user-level-lecture-level-attendance-session-completion-rate"

    OUTPUT_SHEET_ID = "1yXKuiV27EmO8KyVHK4Cj54qgFSSbEMPj4m95qVfDWdA"  # <-- update if this should target a different sheet
    STUDENT_LEVEL_WORKSHEET = "Session-Completion-Rate-StudentLevel"

    # Only keep batches whose name mentions one of these modules (case-insensitive,
    # substring match). Set to None to disable this filter and keep all batches.
    MODULE_FILTER = ["Spreadsheet", "SQL"]

    # Maps a substring found in batch_name -> display Module name (used for the
    # new "Module" column). Extend as more modules show up in your data.
    MODULE_PATTERNS = {
        "spreadsheet": "Spreadsheets",
        "sql": "SQL",
        "python": "Python",
        "power bi": "Power BI",
        "tableau": "Tableau",
        "machine learning": "Machine Learning",
    }

    # ID columns that can arrive comma-formatted (e.g. "3,380") -- need the
    # commas stripped before they can be treated as numbers.
    ID_COLS = ["course_id", "user_id", "lecture_id"]

    NUMERIC_COLS = [
        "class_number",
        "overall_attendance",
        "live_attended_flag",
        "live_attendance",
        "recorded_attendance",
        "live_60_per_watched_attendance",
        "recorded_40_per_watched_attendance",
        "overall_70_per_watched_attendance",
        "pct_watched",
        "watch_live_mins",
        "watch_recorded_mins",
        "present_gt_95pct_flag",
        "present_gt_50pct_flag",
    ]


    def _clean_numeric(series: pd.Series) -> pd.Series:
        """Strips thousand-separator commas (e.g. '46,620' -> 46620) before converting to numeric."""
        return pd.to_numeric(
            series.astype(str).str.replace(",", "", regex=False).str.strip(),
            errors="coerce",
        )


    def _parse_datetime_flexible(series: pd.Series) -> pd.Series:
        parsed = pd.to_datetime(series, format="ISO8601", errors="coerce")
        still_missing = parsed.isna() & series.notna()
        if still_missing.any():
            parsed.loc[still_missing] = pd.to_datetime(
                series[still_missing], format="%B %d, %Y, %I:%M %p", errors="coerce"
            )
        return parsed


    def extract_module(batch_name) -> str:
        """Derives a clean Module name from batch_name (e.g. 'DS SQL - February 2026' -> 'SQL')."""
        if pd.isna(batch_name):
            return "Other"
        name_lower = str(batch_name).lower()
        for substring, display_name in MODULE_PATTERNS.items():
            if substring in name_lower:
                return display_name
        return "Other"


    def load_and_merge_session_completion_data(metabase_card_id, data_type_name):
        """Load session-completion-rate lecture data + Groomers + Master Data."""

        print(f"📥 Loading {data_type_name} Data")

        res = requests.post(
            f"{METABASE_HOST}/api/card/{metabase_card_id}/query/json",
            headers={"Content-Type": "application/json", "X-Metabase-Session": token},
            timeout=3600,
        )
        res.raise_for_status()

        df_raw = pd.DataFrame(res.json())
        print(f"  ✓ Loaded {len(df_raw)} rows")

        # Clean comma-formatted ID columns (course_id, user_id, lecture_id can
        # come back as strings like "46,620" from this export)
        for col in ID_COLS:
            if col in df_raw.columns:
                df_raw[col] = _clean_numeric(df_raw[col])

        df_raw["user_id"] = df_raw["user_id"].astype("Int64").astype(str).str.strip()

        # au_batch_name, au_start_date columns present in this version of the raw data
        if "au_start_date" in df_raw.columns:
            df_raw["au_start_date"] = _parse_datetime_flexible(df_raw["au_start_date"])
        if "lecture_start_timestamp" in df_raw.columns:
            df_raw["lecture_start_timestamp"] = _parse_datetime_flexible(df_raw["lecture_start_timestamp"])

        for col in NUMERIC_COLS:
            if col in df_raw.columns:
                df_raw[col] = pd.to_numeric(df_raw[col], errors="coerce")

        # NEW: Module column, derived from batch_name (not au_batch_name --
        # au_batch_name is the generic AU program name and doesn't mention the
        # module; batch_name is the one with "DS SQL - February 2026" style text)
        df_raw["Module"] = df_raw["batch_name"].apply(extract_module)

        # Optional: filter to specific modules by batch name
        if MODULE_FILTER:
            module_pattern = "|".join(MODULE_FILTER)
            df_raw = df_raw[df_raw["batch_name"].str.contains(module_pattern, case=False, na=False)].copy()
            print(f"  ✓ {len(df_raw)} rows after filtering to batches matching: {MODULE_FILTER}")

        # Filter by approved students
        df_filtered = df_raw[df_raw["user_id"].isin(approved_ids)].copy()

        # Merge with Groomers
        df_merged = df_filtered.merge(
            df_groomers_clean[["user_id", "Enrolled Status", "Phase"]],
            on="user_id",
            how="left",
        )

        # Merge with Master Data (dynamically)
        if master_data_available:
            available_master_cols = ["user_id"]
            desired_cols = [
                'Persona', 'Age Bracket', 'Apti Bucket', 'CTC Bracket', "Learner's Brackets",
                       '12th Bucket', 'Placeability Bucket', 'Background', 'Financial Status Cleaned','Placeability Buckets',
                       'Employment Status', 'Batch', 'Batch Name','Grad CGPA',' Grad bin','Working ','Work ex bracket',
                'Age bracket','CTC bracket','Fin Bucket'
            ]
            for col in desired_cols:
                if col in df_master.columns:
                    available_master_cols.append(col)

            df_merged = df_merged.merge(
                df_master[available_master_cols].drop_duplicates(subset="user_id"),
                on="user_id",
                how="left",
            )

        return df_merged


    # ═══════════════════════════════════════════════════════════════════════════
    # STEP 1: Load Groomers Data (Once)
    # ═══════════════════════════════════════════════════════════════════════════
    print("📥 STEP 1: Load Groomers Data")

    workbook_groomers = gc.open("Groomers")
    worksheet_groomers = workbook_groomers.worksheet("Groomers")
    data_groomers = worksheet_groomers.get_all_values()

    df_groomers = pd.DataFrame(data_groomers[1:], columns=data_groomers[0])
    df_groomers = df_groomers.rename(columns={"UserID": "user_id"})
    df_groomers["user_id"] = df_groomers["user_id"].astype(str).str.strip()

    df_groomers_clean = df_groomers[
        (df_groomers["Enrolled Status"] != "Refund Requested")
        & (df_groomers["Phase"] != "Unavailable")
        & (df_groomers["Enrolled Status"] != "DPD/Foreclosed")
    ].copy()

    approved_ids = df_groomers_clean["user_id"].unique().tolist()
    print(f"✓ Approved Students: {len(approved_ids)}")

    # ═══════════════════════════════════════════════════════════════════════════
    # STEP 2: Load Master Data (Once)
    # ═══════════════════════════════════════════════════════════════════════════
    print("\n📥 STEP 2: Load Master Data")

    try:
        workbook_master = gc.open("DS Full program - All Intake 2026")
        worksheet_master = workbook_master.worksheet("Master Data 2023-2026")
        data_master = worksheet_master.get_all_values()

        df_master = pd.DataFrame(data_master[1:], columns=data_master[0])
        df_master = df_master.rename(columns={"UserID": "user_id"})
        df_master["user_id"] = df_master["user_id"].astype(str).str.strip()
        df_master = df_master[~df_master["Persona"].isin(["NF", "#N/A"]) & df_master["Persona"].notna()].copy()

        print(f"✓ Loaded {len(df_master)} rows from Master Data")
        master_data_available = True
    except Exception as e:
        print(f"⚠️  Could not load Master Data: {e}")
        master_data_available = False

    # ═══════════════════════════════════════════════════════════════════════════
    # STEP 3: Session Completion Rate (Lecture Level, from card 11635)
    # ═══════════════════════════════════════════════════════════════════════════
    print("\n" + "=" * 80)
    print("PROCESSING: SESSION COMPLETION RATE (LECTURE LEVEL)")
    print("=" * 80)

    df_session_completion = load_and_merge_session_completion_data(
        SESSION_COMPLETION_CARD_ID, "Session Completion Rate"
    )
    print(f"✓ Merged session completion data for {len(df_session_completion)} approved student-lecture rows")

    if len(df_session_completion) > 0:
        print(f"  Batches included: {sorted(df_session_completion['batch_name'].unique())}")
        print(f"  Modules found: {sorted(df_session_completion['Module'].unique())}")

    # ═══════════════════════════════════════════════════════════════════════════
    # STEP 4: Upload to Google Sheets (Student Level)
    # ═══════════════════════════════════════════════════════════════════════════
    print("\n" + "=" * 80)
    print("UPLOADING TO GOOGLE SHEETS")
    print("=" * 80)

    sheet = gc.open_by_key(OUTPUT_SHEET_ID)

    print(f"\n☁️  Uploading {STUDENT_LEVEL_WORKSHEET}...")
    try:
        worksheet = sheet.worksheet(STUDENT_LEVEL_WORKSHEET)
        worksheet.clear()
        set_with_dataframe(worksheet, df_session_completion, include_index=False, include_column_header=True)
        print(f"  ✓ {STUDENT_LEVEL_WORKSHEET}")
    except Exception:
        worksheet = sheet.add_worksheet(STUDENT_LEVEL_WORKSHEET, rows=5000, cols=150)
        set_with_dataframe(worksheet, df_session_completion, include_index=False, include_column_header=True)
        print(f"  ✓ Created {STUDENT_LEVEL_WORKSHEET}")

    # ===== WOW - Session Completion rate =====

    # ──────────────────────────────────────────────────────────────────────
    # Cell 63
    # ──────────────────────────────────────────────────────────────────────
    import pandas as pd
    import requests
    from gspread_dataframe import set_with_dataframe

    # =====================================================================
    # CONFIG
    # =====================================================================

    METABASE_HOST = "https://metabase-lierhfgoeiwhr.newtonschool.co"
    SESSION_COMPLETION_CARD_ID = 11794  # "user-level-lecture-level-attendance-session-completion-rate"

    OUTPUT_SHEET_ID = "1z72t5HaE3ombCHo6kbpM3bqhnuTEBNR6L8jncdQZ7Y8"  # <-- update if this should target a different sheet
    STUDENT_LEVEL_WORKSHEET = "Session-Completion-Rate-Class-Grouped"

    # Only keep batches whose name mentions one of these modules (case-insensitive,
    # substring match). Set to None to disable this filter and keep all batches.
    MODULE_FILTER = ["Spreadsheet", "SQL"]

    # Maps a substring found in batch_name -> display Module name (used for the
    # new "Module" column). Extend as more modules show up in your data.
    MODULE_PATTERNS = {
        "spreadsheet": "Spreadsheets",
        "sql": "SQL",
        "python": "Python",
        "power bi": "Power BI",
        "tableau": "Tableau",
        "machine learning": "Machine Learning",
    }

    # ID columns that can arrive comma-formatted (e.g. "3,380") -- need the
    # commas stripped before they can be treated as numbers.
    ID_COLS = ["course_id", "user_id"]

    NUMERIC_COLS = [
        "overall_attendance",
        "live_attended_flag",
        "live_attendance",
        "recorded_attendance",
        "live_60_per_watched_attendance",
        "recorded_40_per_watched_attendance",
        "overall_70_per_watched_attendance",
        "pct_watched",
        "present_gt_95pct_flag",
        "present_gt_50pct_flag",
    ]


    def _clean_numeric(series: pd.Series) -> pd.Series:
        """Strips thousand-separator commas (e.g. '46,620' -> 46620) before converting to numeric."""
        return pd.to_numeric(
            series.astype(str).str.replace(",", "", regex=False).str.strip(),
            errors="coerce",
        )


    def _parse_datetime_flexible(series: pd.Series) -> pd.Series:
        parsed = pd.to_datetime(series, format="ISO8601", errors="coerce")
        still_missing = parsed.isna() & series.notna()
        if still_missing.any():
            parsed.loc[still_missing] = pd.to_datetime(
                series[still_missing], format="%B %d, %Y, %I:%M %p", errors="coerce"
            )
        return parsed


    def extract_module(batch_name) -> str:
        """Derives a clean Module name from batch_name (e.g. 'DS SQL - February 2026' -> 'SQL')."""
        if pd.isna(batch_name):
            return "Other"
        name_lower = str(batch_name).lower()
        for substring, display_name in MODULE_PATTERNS.items():
            if substring in name_lower:
                return display_name
        return "Other"


    def load_and_merge_session_completion_data(metabase_card_id, data_type_name):
        """Load session-completion-rate lecture data + Groomers + Master Data."""

        print(f"📥 Loading {data_type_name} Data")

        res = requests.post(
            f"{METABASE_HOST}/api/card/{metabase_card_id}/query/json",
            headers={"Content-Type": "application/json", "X-Metabase-Session": token},
            timeout=3600,
        )
        res.raise_for_status()

        df_raw = pd.DataFrame(res.json())
        print(f"  ✓ Loaded {len(df_raw)} rows")

        # Clean comma-formatted ID columns (course_id, user_id, lecture_id can
        # come back as strings like "46,620" from this export)
        for col in ID_COLS:
            if col in df_raw.columns:
                df_raw[col] = _clean_numeric(df_raw[col])

        df_raw["user_id"] = df_raw["user_id"].astype("Int64").astype(str).str.strip()

        # au_batch_name, au_start_date columns present in this version of the raw data
        if "au_start_date" in df_raw.columns:
            df_raw["au_start_date"] = _parse_datetime_flexible(df_raw["au_start_date"])
        if "lecture_start_timestamp" in df_raw.columns:
            df_raw["lecture_start_timestamp"] = _parse_datetime_flexible(df_raw["lecture_start_timestamp"])

        for col in NUMERIC_COLS:
            if col in df_raw.columns:
                df_raw[col] = pd.to_numeric(df_raw[col], errors="coerce")

        # NEW: Module column, derived from batch_name (not au_batch_name --
        # au_batch_name is the generic AU program name and doesn't mention the
        # module; batch_name is the one with "DS SQL - February 2026" style text)
        df_raw["Module"] = df_raw["batch_name"].apply(extract_module)

        # Optional: filter to specific modules by batch name
        if MODULE_FILTER:
            module_pattern = "|".join(MODULE_FILTER)
            df_raw = df_raw[df_raw["batch_name"].str.contains(module_pattern, case=False, na=False)].copy()
            print(f"  ✓ {len(df_raw)} rows after filtering to batches matching: {MODULE_FILTER}")

        # Filter by approved students
        df_filtered = df_raw[df_raw["user_id"].isin(approved_ids)].copy()

        # Merge with Groomers
        df_merged = df_filtered.merge(
            df_groomers_clean[["user_id", "Enrolled Status", "Phase"]],
            on="user_id",
            how="left",
        )

        # Merge with Master Data (dynamically)
        if master_data_available:
            available_master_cols = ["user_id"]
            desired_cols = [
                'Persona', 'Age Bracket', 'Apti Bucket', 'CTC Bracket', "Learner's Brackets",
                       '12th Bucket', 'Placeability Bucket', 'Background', 'Financial Status Cleaned','Placeability Buckets',
                       'Employment Status', 'Batch', 'Batch Name','Grad CGPA',' Grad bin','Working ','Work ex bracket',
                'Age bracket','CTC bracket','Fin Bucket'
            ]
            for col in desired_cols:
                if col in df_master.columns:
                    available_master_cols.append(col)

            df_merged = df_merged.merge(
                df_master[available_master_cols].drop_duplicates(subset="user_id"),
                on="user_id",
                how="left",
            )

        return df_merged


    # ═══════════════════════════════════════════════════════════════════════════
    # STEP 1: Load Groomers Data (Once)
    # ═══════════════════════════════════════════════════════════════════════════
    print("📥 STEP 1: Load Groomers Data")

    workbook_groomers = gc.open("Groomers")
    worksheet_groomers = workbook_groomers.worksheet("Groomers")
    data_groomers = worksheet_groomers.get_all_values()

    df_groomers = pd.DataFrame(data_groomers[1:], columns=data_groomers[0])
    df_groomers = df_groomers.rename(columns={"UserID": "user_id"})
    df_groomers["user_id"] = df_groomers["user_id"].astype(str).str.strip()

    df_groomers_clean = df_groomers[
        (df_groomers["Enrolled Status"] != "Refund Requested")
        & (df_groomers["Phase"] != "Unavailable")
        & (df_groomers["Enrolled Status"] != "DPD/Foreclosed")
    ].copy()

    approved_ids = df_groomers_clean["user_id"].unique().tolist()
    print(f"✓ Approved Students: {len(approved_ids)}")

    # ═══════════════════════════════════════════════════════════════════════════
    # STEP 2: Load Master Data (Once)
    # ═══════════════════════════════════════════════════════════════════════════
    print("\n📥 STEP 2: Load Master Data")

    try:
        workbook_master = gc.open("DS Full program - All Intake 2026")
        worksheet_master = workbook_master.worksheet("Master Data 2023-2026")
        data_master = worksheet_master.get_all_values()

        df_master = pd.DataFrame(data_master[1:], columns=data_master[0])
        df_master = df_master.rename(columns={"UserID": "user_id"})
        df_master["user_id"] = df_master["user_id"].astype(str).str.strip()
        df_master = df_master[~df_master["Persona"].isin(["NF", "#N/A"]) & df_master["Persona"].notna()].copy()

        print(f"✓ Loaded {len(df_master)} rows from Master Data")
        master_data_available = True
    except Exception as e:
        print(f"⚠️  Could not load Master Data: {e}")
        master_data_available = False

    # ═══════════════════════════════════════════════════════════════════════════
    # STEP 3: Session Completion Rate (Lecture Level, from card 11635)
    # ═══════════════════════════════════════════════════════════════════════════
    print("\n" + "=" * 80)
    print("PROCESSING: SESSION COMPLETION RATE (LECTURE LEVEL)")
    print("=" * 80)

    df_session_completion = load_and_merge_session_completion_data(
        SESSION_COMPLETION_CARD_ID, "Session Completion Rate"
    )
    print(f"✓ Merged session completion data for {len(df_session_completion)} approved student-lecture rows")

    if len(df_session_completion) > 0:
        print(f"  Batches included: {sorted(df_session_completion['batch_name'].unique())}")
        print(f"  Modules found: {sorted(df_session_completion['Module'].unique())}")

    # ═══════════════════════════════════════════════════════════════════════════
    # STEP 4: Upload to Google Sheets (Student Level)
    # ═══════════════════════════════════════════════════════════════════════════
    print("\n" + "=" * 80)
    print("UPLOADING TO GOOGLE SHEETS")
    print("=" * 80)

    sheet = gc.open_by_key(OUTPUT_SHEET_ID)

    print(f"\n☁️  Uploading {STUDENT_LEVEL_WORKSHEET}...")
    try:
        worksheet = sheet.worksheet(STUDENT_LEVEL_WORKSHEET)
        worksheet.clear()
        set_with_dataframe(worksheet, df_session_completion, include_index=False, include_column_header=True)
        print(f"  ✓ {STUDENT_LEVEL_WORKSHEET}")
    except Exception:
        worksheet = sheet.add_worksheet(STUDENT_LEVEL_WORKSHEET, rows=5000, cols=150)
        set_with_dataframe(worksheet, df_session_completion, include_index=False, include_column_header=True)
        print(f"  ✓ Created {STUDENT_LEVEL_WORKSHEET}")

    # ===== COC - Avg Time Spent =====

    # ──────────────────────────────────────────────────────────────────────
    # Cell 65
    # ──────────────────────────────────────────────────────────────────────
    import pandas as pd
    import requests
    from gspread_dataframe import set_with_dataframe

    # =====================================================================
    # CONFIG
    # =====================================================================

    METABASE_HOST = "https://metabase-lierhfgoeiwhr.newtonschool.co"
    TIME_SPENT_CARD_ID = 11636  # "user-level-lecture-level-attendance-time-spent"

    OUTPUT_SHEET_ID = "1NJqW814fFvwJsrDkk5uzdg4WDhICTGs2eaMW8iv-uRk"  # <-- update if this should target a different sheet
    STUDENT_LEVEL_WORKSHEET = "Time-Spent-In-Session-StudentLevel"

    # Only keep batches whose name mentions one of these modules (case-insensitive,
    # substring match). Set to None to disable this filter and keep all batches.
    MODULE_FILTER = ["Spreadsheet", "SQL"]

    # Maps a substring found in batch_name -> display Module name (used for the
    # new "Module" column). Extend as more modules show up in your data.
    MODULE_PATTERNS = {
        "spreadsheet": "Spreadsheets",
        "sql": "SQL",
        "python": "Python",
        "power bi": "Power BI",
        "tableau": "Tableau",
        "machine learning": "Machine Learning",
    }

    # ID columns that can arrive comma-formatted (e.g. "3,380") -- need the
    # commas stripped before they can be treated as numbers.
    ID_COLS = ["course_id", "user_id", "lecture_id"]

    NUMERIC_COLS = [
        "class_number",
        "overall_attendance",
        "live_attended_flag",
        "live_attendance",
        "recorded_attendance",
        "live_60_per_watched_attendance",
        "recorded_40_per_watched_attendance",
        "overall_70_per_watched_attendance",
        "pct_watched",
        "watch_live_mins",
        "watch_recorded_mins",
        "present_gt_95pct_flag",
        "present_gt_50pct_flag",
        "time_spent_mins",
        "session_length_mins",
    ]


    def _clean_numeric(series: pd.Series) -> pd.Series:
        """Strips thousand-separator commas (e.g. '46,620' -> 46620) before converting to numeric."""
        return pd.to_numeric(
            series.astype(str).str.replace(",", "", regex=False).str.strip(),
            errors="coerce",
        )


    def _parse_datetime_flexible(series: pd.Series) -> pd.Series:
        parsed = pd.to_datetime(series, format="ISO8601", errors="coerce")
        still_missing = parsed.isna() & series.notna()
        if still_missing.any():
            parsed.loc[still_missing] = pd.to_datetime(
                series[still_missing], format="%B %d, %Y, %I:%M %p", errors="coerce"
            )
        return parsed


    def extract_module(batch_name) -> str:
        """Derives a clean Module name from batch_name (e.g. 'DS SQL - February 2026' -> 'SQL')."""
        if pd.isna(batch_name):
            return "Other"
        name_lower = str(batch_name).lower()
        for substring, display_name in MODULE_PATTERNS.items():
            if substring in name_lower:
                return display_name
        return "Other"


    def load_and_merge_time_spent_data(metabase_card_id, data_type_name):
        """Load time-spent-in-session lecture data + Groomers + Master Data."""

        print(f"📥 Loading {data_type_name} Data")

        res = requests.post(
            f"{METABASE_HOST}/api/card/{metabase_card_id}/query/json",
            headers={"Content-Type": "application/json", "X-Metabase-Session": token},
            timeout=3600,
        )
        res.raise_for_status()

        df_raw = pd.DataFrame(res.json())
        print(f"  ✓ Loaded {len(df_raw)} rows")

        # Clean comma-formatted ID columns (course_id, user_id, lecture_id can
        # come back as strings like "46,620" from this export)
        for col in ID_COLS:
            if col in df_raw.columns:
                df_raw[col] = _clean_numeric(df_raw[col])

        df_raw["user_id"] = df_raw["user_id"].astype("Int64").astype(str).str.strip()

        # au_batch_name, au_start_date columns present in this version of the raw data
        if "au_start_date" in df_raw.columns:
            df_raw["au_start_date"] = _parse_datetime_flexible(df_raw["au_start_date"])
        if "lecture_start_timestamp" in df_raw.columns:
            df_raw["lecture_start_timestamp"] = _parse_datetime_flexible(df_raw["lecture_start_timestamp"])

        for col in NUMERIC_COLS:
            if col in df_raw.columns:
                df_raw[col] = pd.to_numeric(df_raw[col], errors="coerce")

        # NEW: Module column, derived from batch_name (not au_batch_name --
        # au_batch_name is the generic AU program name and doesn't mention the
        # module; batch_name is the one with "DS SQL - February 2026" style text)
        df_raw["Module"] = df_raw["batch_name"].apply(extract_module)

        # Optional: filter to specific modules by batch name
        if MODULE_FILTER:
            module_pattern = "|".join(MODULE_FILTER)
            df_raw = df_raw[df_raw["batch_name"].str.contains(module_pattern, case=False, na=False)].copy()
            print(f"  ✓ {len(df_raw)} rows after filtering to batches matching: {MODULE_FILTER}")

        # Filter by approved students
        df_filtered = df_raw[df_raw["user_id"].isin(approved_ids)].copy()

        # Merge with Groomers
        df_merged = df_filtered.merge(
            df_groomers_clean[["user_id", "Enrolled Status", "Phase"]],
            on="user_id",
            how="left",
        )

        # Merge with Master Data (dynamically)
        if master_data_available:
            available_master_cols = ["user_id"]
            desired_cols = [
               'Persona', 'Age Bracket', 'Apti Bucket', 'CTC Bracket', "Learner's Brackets",
                       '12th Bucket', 'Placeability Bucket', 'Background', 'Financial Status Cleaned','Placeability Buckets',
                       'Employment Status', 'Batch', 'Batch Name','Grad CGPA',' Grad bin','Working ','Work ex bracket',
                'Age bracket','CTC bracket','Fin Bucket'
            ]
            for col in desired_cols:
                if col in df_master.columns:
                    available_master_cols.append(col)

            df_merged = df_merged.merge(
                df_master[available_master_cols].drop_duplicates(subset="user_id"),
                on="user_id",
                how="left",
            )

        return df_merged


    # ═══════════════════════════════════════════════════════════════════════════
    # STEP 1: Load Groomers Data (Once)
    # ═══════════════════════════════════════════════════════════════════════════
    print("📥 STEP 1: Load Groomers Data")

    workbook_groomers = gc.open("Groomers")
    worksheet_groomers = workbook_groomers.worksheet("Groomers")
    data_groomers = worksheet_groomers.get_all_values()

    df_groomers = pd.DataFrame(data_groomers[1:], columns=data_groomers[0])
    df_groomers = df_groomers.rename(columns={"UserID": "user_id"})
    df_groomers["user_id"] = df_groomers["user_id"].astype(str).str.strip()

    df_groomers_clean = df_groomers[
        (df_groomers["Enrolled Status"] != "Refund Requested")
        & (df_groomers["Phase"] != "Unavailable")
        & (df_groomers["Enrolled Status"] != "DPD/Foreclosed")
    ].copy()

    approved_ids = df_groomers_clean["user_id"].unique().tolist()
    print(f"✓ Approved Students: {len(approved_ids)}")

    # ═══════════════════════════════════════════════════════════════════════════
    # STEP 2: Load Master Data (Once)
    # ═══════════════════════════════════════════════════════════════════════════
    print("\n📥 STEP 2: Load Master Data")

    try:
        workbook_master = gc.open("DS Full program - All Intake 2026")
        worksheet_master = workbook_master.worksheet("Master Data 2023-2026")
        data_master = worksheet_master.get_all_values()

        df_master = pd.DataFrame(data_master[1:], columns=data_master[0])
        df_master = df_master.rename(columns={"User ID ": "user_id"})
        df_master["user_id"] = df_master["user_id"].astype(str).str.strip()
        df_master = df_master[~df_master["Persona"].isin(["NF", "#N/A"]) & df_master["Persona"].notna()].copy()

        print(f"✓ Loaded {len(df_master)} rows from Master Data")
        master_data_available = True
    except Exception as e:
        print(f"⚠️  Could not load Master Data: {e}")
        master_data_available = False

    # ═══════════════════════════════════════════════════════════════════════════
    # STEP 3: Time Spent in Session (Lecture Level, from card 11636)
    # ═══════════════════════════════════════════════════════════════════════════
    print("\n" + "=" * 80)
    print("PROCESSING: TIME SPENT IN SESSION (LECTURE LEVEL)")
    print("=" * 80)

    df_time_spent = load_and_merge_time_spent_data(TIME_SPENT_CARD_ID, "Time Spent in Session")
    print(f"✓ Merged time-spent data for {len(df_time_spent)} approved student-lecture rows")

    if len(df_time_spent) > 0:
        print(f"  Batches included: {sorted(df_time_spent['batch_name'].unique())}")
        print(f"  Modules found: {sorted(df_time_spent['Module'].unique())}")

    # ═══════════════════════════════════════════════════════════════════════════
    # STEP 4: Upload to Google Sheets (Student Level)
    # ═══════════════════════════════════════════════════════════════════════════
    print("\n" + "=" * 80)
    print("UPLOADING TO GOOGLE SHEETS")
    print("=" * 80)

    sheet = gc.open_by_key(OUTPUT_SHEET_ID)

    print(f"\n☁️  Uploading {STUDENT_LEVEL_WORKSHEET}...")
    try:
        worksheet = sheet.worksheet(STUDENT_LEVEL_WORKSHEET)
        worksheet.clear()
        set_with_dataframe(worksheet, df_time_spent, include_index=False, include_column_header=True)
        print(f"  ✓ {STUDENT_LEVEL_WORKSHEET}")
    except Exception:
        worksheet = sheet.add_worksheet(STUDENT_LEVEL_WORKSHEET, rows=5000, cols=150)
        set_with_dataframe(worksheet, df_time_spent, include_index=False, include_column_header=True)
        print(f"  ✓ Created {STUDENT_LEVEL_WORKSHEET}")

    # ===== WOW Avg Time Spent =====

    # ──────────────────────────────────────────────────────────────────────
    # Cell 67
    # ──────────────────────────────────────────────────────────────────────
    import pandas as pd
    import requests
    from gspread_dataframe import set_with_dataframe

    # =====================================================================
    # CONFIG
    # =====================================================================

    METABASE_HOST = "https://metabase-lierhfgoeiwhr.newtonschool.co"
    TIME_SPENT_CARD_ID = 11796  # "user-level-lecture-level-attendance-time-spent"

    OUTPUT_SHEET_ID = "1z72t5HaE3ombCHo6kbpM3bqhnuTEBNR6L8jncdQZ7Y8"  # <-- update if this should target a different sheet
    STUDENT_LEVEL_WORKSHEET = "Avg-Time-Spent-Class-Grouped"

    # Only keep batches whose name mentions one of these modules (case-insensitive,
    # substring match). Set to None to disable this filter and keep all batches.
    MODULE_FILTER = ["Spreadsheet", "SQL"]

    # Maps a substring found in batch_name -> display Module name (used for the
    # new "Module" column). Extend as more modules show up in your data.
    MODULE_PATTERNS = {
        "spreadsheet": "Spreadsheets",
        "sql": "SQL",
        "python": "Python",
        "power bi": "Power BI",
        "tableau": "Tableau",
        "machine learning": "Machine Learning",
    }

    # ID columns that can arrive comma-formatted (e.g. "3,380") -- need the
    # commas stripped before they can be treated as numbers.
    ID_COLS = ["course_id", "user_id"]

    NUMERIC_COLS = [
        "overall_attendance",
        "live_attended_flag",
        "live_attendance",
        "recorded_attendance",
        "live_60_per_watched_attendance",
        "recorded_40_per_watched_attendance",
        "overall_70_per_watched_attendance",
        "pct_watched",
        "watch_live_mins",
        "watch_recorded_mins",
        "time_spent_mins",
        "session_length_mins",
    ]


    def _clean_numeric(series: pd.Series) -> pd.Series:
        """Strips thousand-separator commas (e.g. '46,620' -> 46620) before converting to numeric."""
        return pd.to_numeric(
            series.astype(str).str.replace(",", "", regex=False).str.strip(),
            errors="coerce",
        )


    def _parse_datetime_flexible(series: pd.Series) -> pd.Series:
        parsed = pd.to_datetime(series, format="ISO8601", errors="coerce")
        still_missing = parsed.isna() & series.notna()
        if still_missing.any():
            parsed.loc[still_missing] = pd.to_datetime(
                series[still_missing], format="%B %d, %Y, %I:%M %p", errors="coerce"
            )
        return parsed


    def extract_module(batch_name) -> str:
        """Derives a clean Module name from batch_name (e.g. 'DS SQL - February 2026' -> 'SQL')."""
        if pd.isna(batch_name):
            return "Other"
        name_lower = str(batch_name).lower()
        for substring, display_name in MODULE_PATTERNS.items():
            if substring in name_lower:
                return display_name
        return "Other"


    def load_and_merge_time_spent_data(metabase_card_id, data_type_name):
        """Load time-spent-in-session lecture data + Groomers + Master Data."""

        print(f"📥 Loading {data_type_name} Data")

        res = requests.post(
            f"{METABASE_HOST}/api/card/{metabase_card_id}/query/json",
            headers={"Content-Type": "application/json", "X-Metabase-Session": token},
            timeout=3600,
        )
        res.raise_for_status()

        df_raw = pd.DataFrame(res.json())
        print(f"  ✓ Loaded {len(df_raw)} rows")

        # Clean comma-formatted ID columns (course_id, user_id, lecture_id can
        # come back as strings like "46,620" from this export)
        for col in ID_COLS:
            if col in df_raw.columns:
                df_raw[col] = _clean_numeric(df_raw[col])

        df_raw["user_id"] = df_raw["user_id"].astype("Int64").astype(str).str.strip()

        # au_batch_name, au_start_date columns present in this version of the raw data
        if "au_start_date" in df_raw.columns:
            df_raw["au_start_date"] = _parse_datetime_flexible(df_raw["au_start_date"])
        if "lecture_start_timestamp" in df_raw.columns:
            df_raw["lecture_start_timestamp"] = _parse_datetime_flexible(df_raw["lecture_start_timestamp"])

        for col in NUMERIC_COLS:
            if col in df_raw.columns:
                df_raw[col] = pd.to_numeric(df_raw[col], errors="coerce")

        # NEW: Module column, derived from batch_name (not au_batch_name --
        # au_batch_name is the generic AU program name and doesn't mention the
        # module; batch_name is the one with "DS SQL - February 2026" style text)
        df_raw["Module"] = df_raw["batch_name"].apply(extract_module)

        # Optional: filter to specific modules by batch name
        if MODULE_FILTER:
            module_pattern = "|".join(MODULE_FILTER)
            df_raw = df_raw[df_raw["batch_name"].str.contains(module_pattern, case=False, na=False)].copy()
            print(f"  ✓ {len(df_raw)} rows after filtering to batches matching: {MODULE_FILTER}")

        # Filter by approved students
        df_filtered = df_raw[df_raw["user_id"].isin(approved_ids)].copy()

        # Merge with Groomers
        df_merged = df_filtered.merge(
            df_groomers_clean[["user_id", "Enrolled Status", "Phase"]],
            on="user_id",
            how="left",
        )

        # Merge with Master Data (dynamically)
        if master_data_available:
            available_master_cols = ["user_id"]
            desired_cols = [
               'Persona', 'Age Bracket', 'Apti Bucket', 'CTC Bracket', "Learner's Brackets",
                       '12th Bucket', 'Placeability Bucket', 'Background', 'Financial Status Cleaned','Placeability Buckets',
                       'Employment Status', 'Batch', 'Batch Name','Grad CGPA',' Grad bin','Working ','Work ex bracket',
                'Age bracket','CTC bracket','Fin Bucket'
            ]
            for col in desired_cols:
                if col in df_master.columns:
                    available_master_cols.append(col)

            df_merged = df_merged.merge(
                df_master[available_master_cols].drop_duplicates(subset="user_id"),
                on="user_id",
                how="left",
            )

        return df_merged


    # ═══════════════════════════════════════════════════════════════════════════
    # STEP 1: Load Groomers Data (Once)
    # ═══════════════════════════════════════════════════════════════════════════
    print("📥 STEP 1: Load Groomers Data")

    workbook_groomers = gc.open("Groomers")
    worksheet_groomers = workbook_groomers.worksheet("Groomers")
    data_groomers = worksheet_groomers.get_all_values()

    df_groomers = pd.DataFrame(data_groomers[1:], columns=data_groomers[0])
    df_groomers = df_groomers.rename(columns={"UserID": "user_id"})
    df_groomers["user_id"] = df_groomers["user_id"].astype(str).str.strip()

    df_groomers_clean = df_groomers[
        (df_groomers["Enrolled Status"] != "Refund Requested")
        & (df_groomers["Phase"] != "Unavailable")
        & (df_groomers["Enrolled Status"] != "DPD/Foreclosed")
    ].copy()

    approved_ids = df_groomers_clean["user_id"].unique().tolist()
    print(f"✓ Approved Students: {len(approved_ids)}")

    # ═══════════════════════════════════════════════════════════════════════════
    # STEP 2: Load Master Data (Once)
    # ═══════════════════════════════════════════════════════════════════════════
    print("\n📥 STEP 2: Load Master Data")

    try:
        workbook_master = gc.open("DS Full program - All Intake 2026")
        worksheet_master = workbook_master.worksheet("Master Data 2023-2026")
        data_master = worksheet_master.get_all_values()

        df_master = pd.DataFrame(data_master[1:], columns=data_master[0])
        df_master = df_master.rename(columns={"UserID": "user_id"})
        df_master["user_id"] = df_master["user_id"].astype(str).str.strip()
        df_master = df_master[~df_master["Persona"].isin(["NF", "#N/A"]) & df_master["Persona"].notna()].copy()

        print(f"✓ Loaded {len(df_master)} rows from Master Data")
        master_data_available = True
    except Exception as e:
        print(f"⚠️  Could not load Master Data: {e}")
        master_data_available = False

    # ═══════════════════════════════════════════════════════════════════════════
    # STEP 3: Time Spent in Session (Lecture Level, from card 11636)
    # ═══════════════════════════════════════════════════════════════════════════
    print("\n" + "=" * 80)
    print("PROCESSING: TIME SPENT IN SESSION (LECTURE LEVEL)")
    print("=" * 80)

    df_time_spent = load_and_merge_time_spent_data(TIME_SPENT_CARD_ID, "Time Spent in Session")
    print(f"✓ Merged time-spent data for {len(df_time_spent)} approved student-lecture rows")

    if len(df_time_spent) > 0:
        print(f"  Batches included: {sorted(df_time_spent['batch_name'].unique())}")
        print(f"  Modules found: {sorted(df_time_spent['Module'].unique())}")

    # ═══════════════════════════════════════════════════════════════════════════
    # STEP 4: Upload to Google Sheets (Student Level)
    # ═══════════════════════════════════════════════════════════════════════════
    print("\n" + "=" * 80)
    print("UPLOADING TO GOOGLE SHEETS")
    print("=" * 80)

    sheet = gc.open_by_key(OUTPUT_SHEET_ID)

    print(f"\n☁️  Uploading {STUDENT_LEVEL_WORKSHEET}...")
    try:
        worksheet = sheet.worksheet(STUDENT_LEVEL_WORKSHEET)
        worksheet.clear()
        set_with_dataframe(worksheet, df_time_spent, include_index=False, include_column_header=True)
        print(f"  ✓ {STUDENT_LEVEL_WORKSHEET}")
    except Exception:
        worksheet = sheet.add_worksheet(STUDENT_LEVEL_WORKSHEET, rows=5000, cols=150)
        set_with_dataframe(worksheet, df_time_spent, include_index=False, include_column_header=True)
        print(f"  ✓ Created {STUDENT_LEVEL_WORKSHEET}")

    # ===== COC Retention Level =====

    # ──────────────────────────────────────────────────────────────────────
    # Cell 69
    # ──────────────────────────────────────────────────────────────────────
    import pandas as pd
    import requests
    from gspread_dataframe import set_with_dataframe

    # =====================================================================
    # CONFIG
    # =====================================================================

    METABASE_HOST = "https://metabase-lierhfgoeiwhr.newtonschool.co"
    RETENTION_CARD_ID = 11637  # "user-level-lecture-level-attendance-retention"

    OUTPUT_SHEET_ID = "1MrxwvmQP45Q2lhrEg6YYzJfI3wnzl-OLVqozkqMirDU"  # <-- update if this should target a different sheet
    STUDENT_LEVEL_WORKSHEET = "Retention-StudentLevel"

    # Only keep batches whose name mentions one of these modules (case-insensitive,
    # substring match). Set to None to disable this filter and keep all batches.
    MODULE_FILTER = ["Spreadsheet", "SQL"]

    # Maps a substring found in batch_name -> display Module name (used for the
    # new "Module" column). Extend as more modules show up in your data.
    MODULE_PATTERNS = {
        "spreadsheet": "Spreadsheets",
        "sql": "SQL",
        "python": "Python",
        "power bi": "Power BI",
        "tableau": "Tableau",
        "machine learning": "Machine Learning",
    }

    # ID columns that can arrive comma-formatted (e.g. "3,380") -- need the
    # commas stripped before they can be treated as numbers. Checked with
    # "if present" below, since not every card includes every one of these.
    ID_COLS = ["course_id", "user_id", "lecture_id"]

    NUMERIC_COLS = [
        "class_number",
        "retained_flag",
    ]


    def _clean_numeric(series: pd.Series) -> pd.Series:
        """Strips thousand-separator commas (e.g. '46,620' -> 46620) before converting to numeric."""
        return pd.to_numeric(
            series.astype(str).str.replace(",", "", regex=False).str.strip(),
            errors="coerce",
        )


    def _parse_datetime_flexible(series: pd.Series) -> pd.Series:
        parsed = pd.to_datetime(series, format="ISO8601", errors="coerce")
        still_missing = parsed.isna() & series.notna()
        if still_missing.any():
            parsed.loc[still_missing] = pd.to_datetime(
                series[still_missing], format="%B %d, %Y, %I:%M %p", errors="coerce"
            )
        return parsed


    def extract_module(batch_name) -> str:
        """Derives a clean Module name from batch_name (e.g. 'DS SQL - February 2026' -> 'SQL')."""
        if pd.isna(batch_name):
            return "Other"
        name_lower = str(batch_name).lower()
        for substring, display_name in MODULE_PATTERNS.items():
            if substring in name_lower:
                return display_name
        return "Other"


    def load_and_merge_retention_data(metabase_card_id, data_type_name):
        """Load student-level, lecture-level retention data + Groomers + Master Data."""

        print(f"📥 Loading {data_type_name} Data")

        res = requests.post(
            f"{METABASE_HOST}/api/card/{metabase_card_id}/query/json",
            headers={"Content-Type": "application/json", "X-Metabase-Session": token},
            timeout=3600,
        )
        res.raise_for_status()

        df_raw = pd.DataFrame(res.json())
        print(f"  ✓ Loaded {len(df_raw)} rows")

        # Clean comma-formatted ID columns (course_id, user_id, lecture_id can
        # come back as strings like "46,620" from this export)
        for col in ID_COLS:
            if col in df_raw.columns:
                df_raw[col] = _clean_numeric(df_raw[col])

        df_raw["user_id"] = df_raw["user_id"].astype("Int64").astype(str).str.strip()

        # au_batch_name, au_start_date columns, if present in this card's output
        if "au_start_date" in df_raw.columns:
            df_raw["au_start_date"] = _parse_datetime_flexible(df_raw["au_start_date"])
        if "lecture_start_timestamp" in df_raw.columns:
            df_raw["lecture_start_timestamp"] = _parse_datetime_flexible(df_raw["lecture_start_timestamp"])

        for col in NUMERIC_COLS:
            if col in df_raw.columns:
                df_raw[col] = pd.to_numeric(df_raw[col], errors="coerce")

        # NEW: Module column, derived from batch_name (not au_batch_name --
        # au_batch_name is the generic AU program name and doesn't mention the
        # module; batch_name is the one with "DS SQL - February 2026" style text)
        df_raw["Module"] = df_raw["batch_name"].apply(extract_module)

        # Optional: filter to specific modules by batch name
        if MODULE_FILTER:
            module_pattern = "|".join(MODULE_FILTER)
            df_raw = df_raw[df_raw["batch_name"].str.contains(module_pattern, case=False, na=False)].copy()
            print(f"  ✓ {len(df_raw)} rows after filtering to batches matching: {MODULE_FILTER}")

        # Filter by approved students
        df_filtered = df_raw[df_raw["user_id"].isin(approved_ids)].copy()

        # Merge with Groomers
        df_merged = df_filtered.merge(
            df_groomers_clean[["user_id", "Enrolled Status", "Phase"]],
            on="user_id",
            how="left",
        )

        # Merge with Master Data (dynamically)
        if master_data_available:
            available_master_cols = ["user_id"]
            desired_cols = [
               'Persona', 'Age Bracket', 'Apti Bucket', 'CTC Bracket', "Learner's Brackets",
                       '12th Bucket', 'Placeability Bucket', 'Background', 'Financial Status Cleaned','Placeability Buckets',
                       'Employment Status', 'Batch', 'Batch Name','Grad CGPA',' Grad bin','Working ','Work ex bracket',
                'Age bracket','CTC bracket','Fin Bucket'
            ]
            for col in desired_cols:
                if col in df_master.columns:
                    available_master_cols.append(col)

            df_merged = df_merged.merge(
                df_master[available_master_cols].drop_duplicates(subset="user_id"),
                on="user_id",
                how="left",
            )

        return df_merged


    # ═══════════════════════════════════════════════════════════════════════════
    # STEP 1: Load Groomers Data (Once)
    # ═══════════════════════════════════════════════════════════════════════════
    print("📥 STEP 1: Load Groomers Data")

    workbook_groomers = gc.open("Groomers")
    worksheet_groomers = workbook_groomers.worksheet("Groomers")
    data_groomers = worksheet_groomers.get_all_values()

    df_groomers = pd.DataFrame(data_groomers[1:], columns=data_groomers[0])
    df_groomers = df_groomers.rename(columns={"UserID": "user_id"})
    df_groomers["user_id"] = df_groomers["user_id"].astype(str).str.strip()

    df_groomers_clean = df_groomers[
        (df_groomers["Enrolled Status"] != "Refund Requested")
        & (df_groomers["Phase"] != "Unavailable")
        & (df_groomers["Enrolled Status"] != "DPD/Foreclosed")
    ].copy()

    approved_ids = df_groomers_clean["user_id"].unique().tolist()
    print(f"✓ Approved Students: {len(approved_ids)}")

    # ═══════════════════════════════════════════════════════════════════════════
    # STEP 2: Load Master Data (Once)
    # ═══════════════════════════════════════════════════════════════════════════
    print("\n📥 STEP 2: Load Master Data")

    try:
        workbook_master = gc.open("DS Full program - All Intake 2026")
        worksheet_master = workbook_master.worksheet("Master Data 2023-2026")
        data_master = worksheet_master.get_all_values()

        df_master = pd.DataFrame(data_master[1:], columns=data_master[0])
        df_master = df_master.rename(columns={"User ID ": "user_id"})
        df_master["user_id"] = df_master["user_id"].astype(str).str.strip()
        df_master = df_master[~df_master["Persona"].isin(["NF", "#N/A"]) & df_master["Persona"].notna()].copy()

        print(f"✓ Loaded {len(df_master)} rows from Master Data")
        master_data_available = True
    except Exception as e:
        print(f"⚠️  Could not load Master Data: {e}")
        master_data_available = False

    # ═══════════════════════════════════════════════════════════════════════════
    # STEP 3: Cumulative Cohort Retention (Student Level, Lecture Level, card 11637)
    # ═══════════════════════════════════════════════════════════════════════════
    print("\n" + "=" * 80)
    print("PROCESSING: CUMULATIVE COHORT RETENTION (STUDENT LEVEL, LECTURE LEVEL)")
    print("=" * 80)

    df_retention = load_and_merge_retention_data(RETENTION_CARD_ID, "Cumulative Cohort Retention")
    print(f"✓ Merged retention data for {len(df_retention)} approved student-lecture rows")

    if len(df_retention) > 0:
        print(f"  Batches included: {sorted(df_retention['batch_name'].unique())}")
        print(f"  Modules found: {sorted(df_retention['Module'].unique())}")

    # ═══════════════════════════════════════════════════════════════════════════
    # STEP 4: Upload to Google Sheets (Student Level)
    # ═══════════════════════════════════════════════════════════════════════════
    print("\n" + "=" * 80)
    print("UPLOADING TO GOOGLE SHEETS")
    print("=" * 80)

    sheet = gc.open_by_key(OUTPUT_SHEET_ID)

    print(f"\n☁️  Uploading {STUDENT_LEVEL_WORKSHEET}...")
    try:
        worksheet = sheet.worksheet(STUDENT_LEVEL_WORKSHEET)
        worksheet.clear()
        set_with_dataframe(worksheet, df_retention, include_index=False, include_column_header=True)
        print(f"  ✓ {STUDENT_LEVEL_WORKSHEET}")
    except Exception:
        worksheet = sheet.add_worksheet(STUDENT_LEVEL_WORKSHEET, rows=5000, cols=150)
        set_with_dataframe(worksheet, df_retention, include_index=False, include_column_header=True)
        print(f"  ✓ Created {STUDENT_LEVEL_WORKSHEET}")

    # ===== WoW Retention Level =====

    # ──────────────────────────────────────────────────────────────────────
    # Cell 71
    # ──────────────────────────────────────────────────────────────────────
    import pandas as pd
    import requests
    from gspread_dataframe import set_with_dataframe

    # =====================================================================
    # CONFIG
    # =====================================================================

    METABASE_HOST = "https://metabase-lierhfgoeiwhr.newtonschool.co"
    RETENTION_CARD_ID = 11799  # "user-level-lecture-level-attendance-retention"

    OUTPUT_SHEET_ID = "1z72t5HaE3ombCHo6kbpM3bqhnuTEBNR6L8jncdQZ7Y8"  # <-- update if this should target a different sheet
    STUDENT_LEVEL_WORKSHEET = "Retention-StudentLevel-Class-Grouped"

    # Only keep batches whose name mentions one of these modules (case-insensitive,
    # substring match). Set to None to disable this filter and keep all batches.
    MODULE_FILTER = ["Spreadsheet", "SQL"]

    # Maps a substring found in batch_name -> display Module name (used for the
    # new "Module" column). Extend as more modules show up in your data.
    MODULE_PATTERNS = {
        "spreadsheet": "Spreadsheets",
        "sql": "SQL",
        "python": "Python",
        "power bi": "Power BI",
        "tableau": "Tableau",
        "machine learning": "Machine Learning",
    }

    # ID columns that can arrive comma-formatted (e.g. "3,380") -- need the
    # commas stripped before they can be treated as numbers. Checked with
    # "if present" below, since not every card includes every one of these.
    ID_COLS = ["course_id", "user_id"]

    NUMERIC_COLS = [
        "retained_flag",
        "classes_in_bucket",
        "classes_retained"

    ]


    def _clean_numeric(series: pd.Series) -> pd.Series:
        """Strips thousand-separator commas (e.g. '46,620' -> 46620) before converting to numeric."""
        return pd.to_numeric(
            series.astype(str).str.replace(",", "", regex=False).str.strip(),
            errors="coerce",
        )


    def _parse_datetime_flexible(series: pd.Series) -> pd.Series:
        parsed = pd.to_datetime(series, format="ISO8601", errors="coerce")
        still_missing = parsed.isna() & series.notna()
        if still_missing.any():
            parsed.loc[still_missing] = pd.to_datetime(
                series[still_missing], format="%B %d, %Y, %I:%M %p", errors="coerce"
            )
        return parsed


    def extract_module(batch_name) -> str:
        """Derives a clean Module name from batch_name (e.g. 'DS SQL - February 2026' -> 'SQL')."""
        if pd.isna(batch_name):
            return "Other"
        name_lower = str(batch_name).lower()
        for substring, display_name in MODULE_PATTERNS.items():
            if substring in name_lower:
                return display_name
        return "Other"


    def load_and_merge_retention_data(metabase_card_id, data_type_name):
        """Load student-level, lecture-level retention data + Groomers + Master Data."""

        print(f"📥 Loading {data_type_name} Data")

        res = requests.post(
            f"{METABASE_HOST}/api/card/{metabase_card_id}/query/json",
            headers={"Content-Type": "application/json", "X-Metabase-Session": token},
            timeout=3600,
        )
        res.raise_for_status()

        df_raw = pd.DataFrame(res.json())
        print(f"  ✓ Loaded {len(df_raw)} rows")

        # Clean comma-formatted ID columns (course_id, user_id, lecture_id can
        # come back as strings like "46,620" from this export)
        for col in ID_COLS:
            if col in df_raw.columns:
                df_raw[col] = _clean_numeric(df_raw[col])

        df_raw["user_id"] = df_raw["user_id"].astype("Int64").astype(str).str.strip()

        # au_batch_name, au_start_date columns, if present in this card's output
        if "au_start_date" in df_raw.columns:
            df_raw["au_start_date"] = _parse_datetime_flexible(df_raw["au_start_date"])
        if "lecture_start_timestamp" in df_raw.columns:
            df_raw["lecture_start_timestamp"] = _parse_datetime_flexible(df_raw["lecture_start_timestamp"])

        for col in NUMERIC_COLS:
            if col in df_raw.columns:
                df_raw[col] = pd.to_numeric(df_raw[col], errors="coerce")

        # NEW: Module column, derived from batch_name (not au_batch_name --
        # au_batch_name is the generic AU program name and doesn't mention the
        # module; batch_name is the one with "DS SQL - February 2026" style text)
        df_raw["Module"] = df_raw["batch_name"].apply(extract_module)

        # Optional: filter to specific modules by batch name
        if MODULE_FILTER:
            module_pattern = "|".join(MODULE_FILTER)
            df_raw = df_raw[df_raw["batch_name"].str.contains(module_pattern, case=False, na=False)].copy()
            print(f"  ✓ {len(df_raw)} rows after filtering to batches matching: {MODULE_FILTER}")

        # Filter by approved students
        df_filtered = df_raw[df_raw["user_id"].isin(approved_ids)].copy()

        # Merge with Groomers
        df_merged = df_filtered.merge(
            df_groomers_clean[["user_id", "Enrolled Status", "Phase"]],
            on="user_id",
            how="left",
        )

        # Merge with Master Data (dynamically)
        if master_data_available:
            available_master_cols = ["user_id"]
            desired_cols = [
               'Persona', 'Age Bracket', 'Apti Bucket', 'CTC Bracket', "Learner's Brackets",
                       '12th Bucket', 'Placeability Bucket', 'Background', 'Financial Status Cleaned','Placeability Buckets',
                       'Employment Status', 'Batch', 'Batch Name','Grad CGPA',' Grad bin','Working ','Work ex bracket',
                'Age bracket','CTC bracket','Fin Bucket'
            ]
            for col in desired_cols:
                if col in df_master.columns:
                    available_master_cols.append(col)

            df_merged = df_merged.merge(
                df_master[available_master_cols].drop_duplicates(subset="user_id"),
                on="user_id",
                how="left",
            )

        return df_merged


    # ═══════════════════════════════════════════════════════════════════════════
    # STEP 1: Load Groomers Data (Once)
    # ═══════════════════════════════════════════════════════════════════════════
    print("📥 STEP 1: Load Groomers Data")

    workbook_groomers = gc.open("Groomers")
    worksheet_groomers = workbook_groomers.worksheet("Groomers")
    data_groomers = worksheet_groomers.get_all_values()

    df_groomers = pd.DataFrame(data_groomers[1:], columns=data_groomers[0])
    df_groomers = df_groomers.rename(columns={"UserID": "user_id"})
    df_groomers["user_id"] = df_groomers["user_id"].astype(str).str.strip()

    df_groomers_clean = df_groomers[
        (df_groomers["Enrolled Status"] != "Refund Requested")
        & (df_groomers["Phase"] != "Unavailable")
        & (df_groomers["Enrolled Status"] != "DPD/Foreclosed")
    ].copy()

    approved_ids = df_groomers_clean["user_id"].unique().tolist()
    print(f"✓ Approved Students: {len(approved_ids)}")

    # ═══════════════════════════════════════════════════════════════════════════
    # STEP 2: Load Master Data (Once)
    # ═══════════════════════════════════════════════════════════════════════════
    print("\n📥 STEP 2: Load Master Data")

    try:
        workbook_master = gc.open("DS Full program - All Intake 2026")
        worksheet_master = workbook_master.worksheet("Master Data 2023-2026")
        data_master = worksheet_master.get_all_values()

        df_master = pd.DataFrame(data_master[1:], columns=data_master[0])
        df_master = df_master.rename(columns={"User ID ": "user_id"})
        df_master["user_id"] = df_master["user_id"].astype(str).str.strip()
        df_master = df_master[~df_master["Persona"].isin(["NF", "#N/A"]) & df_master["Persona"].notna()].copy()

        print(f"✓ Loaded {len(df_master)} rows from Master Data")
        master_data_available = True
    except Exception as e:
        print(f"⚠️  Could not load Master Data: {e}")
        master_data_available = False

    # ═══════════════════════════════════════════════════════════════════════════
    # STEP 3: Cumulative Cohort Retention (Student Level, Lecture Level, card 11637)
    # ═══════════════════════════════════════════════════════════════════════════
    print("\n" + "=" * 80)
    print("PROCESSING: CUMULATIVE COHORT RETENTION (STUDENT LEVEL, LECTURE LEVEL)")
    print("=" * 80)

    df_retention = load_and_merge_retention_data(RETENTION_CARD_ID, "Cumulative Cohort Retention")
    print(f"✓ Merged retention data for {len(df_retention)} approved student-lecture rows")

    if len(df_retention) > 0:
        print(f"  Batches included: {sorted(df_retention['batch_name'].unique())}")
        print(f"  Modules found: {sorted(df_retention['Module'].unique())}")

    # ═══════════════════════════════════════════════════════════════════════════
    # STEP 4: Upload to Google Sheets (Student Level)
    # ═══════════════════════════════════════════════════════════════════════════
    print("\n" + "=" * 80)
    print("UPLOADING TO GOOGLE SHEETS")
    print("=" * 80)

    sheet = gc.open_by_key(OUTPUT_SHEET_ID)

    print(f"\n☁️  Uploading {STUDENT_LEVEL_WORKSHEET}...")
    try:
        worksheet = sheet.worksheet(STUDENT_LEVEL_WORKSHEET)
        worksheet.clear()
        set_with_dataframe(worksheet, df_retention, include_index=False, include_column_header=True)
        print(f"  ✓ {STUDENT_LEVEL_WORKSHEET}")
    except Exception:
        worksheet = sheet.add_worksheet(STUDENT_LEVEL_WORKSHEET, rows=5000, cols=150)
        set_with_dataframe(worksheet, df_retention, include_index=False, include_column_header=True)
        print(f"  ✓ Created {STUDENT_LEVEL_WORKSHEET}")

    # ===== COC Lecture Rating =====

    # ──────────────────────────────────────────────────────────────────────
    # Cell 73
    # ──────────────────────────────────────────────────────────────────────


    import pandas as pd
    import requests
    from gspread_dataframe import set_with_dataframe

    # =====================================================================
    # CONFIG
    # =====================================================================

    METABASE_HOST = "https://metabase-lierhfgoeiwhr.newtonschool.co"
    RATING_CARD_ID = 11642  # "instructor-rating-student-level-lecture-level"

    OUTPUT_SHEET_ID = "1MrxwvmQP45Q2lhrEg6YYzJfI3wnzl-OLVqozkqMirDU"  # <-- update if this should target a different sheet
    STUDENT_LEVEL_WORKSHEET = "Lecture-Rating-StudentLevel"

    # Only keep batches whose name mentions one of these modules (case-insensitive,
    # substring match). Set to None to disable this filter and keep all batches.
    MODULE_FILTER = ["Spreadsheet", "SQL"]

    # Maps a substring found in batch_name -> display Module name (used for the
    # "Module" column). Extend as needed.
    MODULE_PATTERNS = {
        "spreadsheet": "Spreadsheets",
        "sql": "SQL",
        "python": "Python",
        "power bi": "Power BI",
        "tableau": "Tableau",
        "machine learning": "Machine Learning",
    }

    # ID columns that can arrive comma-formatted (e.g. "3,380") -- need the
    # commas stripped before they can be treated as numbers.
    ID_COLS = ["course_id", "user_id", "lecture_id"]

    NUMERIC_COLS = ["course_id", "lecture_id", "rating_out_of_5"]


    def _clean_numeric(series: pd.Series) -> pd.Series:
        """Strips thousand-separator commas (e.g. '46,620' -> 46620) before converting to numeric."""
        return pd.to_numeric(
            series.astype(str).str.replace(",", "", regex=False).str.strip(),
            errors="coerce",
        )


    def _parse_datetime_flexible(series: pd.Series) -> pd.Series:
        parsed = pd.to_datetime(series, format="ISO8601", errors="coerce")
        still_missing = parsed.isna() & series.notna()
        if still_missing.any():
            parsed.loc[still_missing] = pd.to_datetime(
                series[still_missing], format="%B %d, %Y, %I:%M %p", errors="coerce"
            )
        return parsed


    def extract_module(batch_name) -> str:
        """Derives a clean Module name from batch_name (e.g. 'DS SQL - February 2026' -> 'SQL')."""
        if pd.isna(batch_name):
            return "Other"
        name_lower = str(batch_name).lower()
        for substring, display_name in MODULE_PATTERNS.items():
            if substring in name_lower:
                return display_name
        return "Other"


    def add_class_number(df: pd.DataFrame) -> pd.DataFrame:
        """Ranks each batch's DISTINCT lectures 1, 2, 3... by lecture_start_timestamp,
        then maps that ordinal back onto every student-level row for that lecture."""
        distinct_lectures = (
            df[["batch_name", "lecture_id", "lecture_start_timestamp"]]
            .drop_duplicates(subset=["batch_name", "lecture_id"])
            .sort_values(["batch_name", "lecture_start_timestamp", "lecture_id"])
        )
        distinct_lectures["class_number"] = distinct_lectures.groupby("batch_name").cumcount() + 1

        return df.merge(
            distinct_lectures[["batch_name", "lecture_id", "class_number"]],
            on=["batch_name", "lecture_id"],
            how="left",
        )


    def load_and_merge_rating_data(metabase_card_id, data_type_name):
        """Load student-level lecture rating data + Groomers + Master Data."""

        print(f"📥 Loading {data_type_name} Data")

        res = requests.post(
            f"{METABASE_HOST}/api/card/{metabase_card_id}/query/json",
            headers={"Content-Type": "application/json", "X-Metabase-Session": token},
            timeout=3600,
        )
        res.raise_for_status()

        df_raw = pd.DataFrame(res.json())
        print(f"  ✓ Loaded {len(df_raw)} rows")

        # Clean comma-formatted ID columns (course_id, user_id, lecture_id can
        # come back as strings like "46,620" from this export)
        for col in ID_COLS:
            if col in df_raw.columns:
                df_raw[col] = _clean_numeric(df_raw[col])

        df_raw["user_id"] = df_raw["user_id"].astype("Int64").astype(str).str.strip()

        for col in ["lecture_start_timestamp", "lecture_date", "form_fill_date", "au_start_date"]:
            if col in df_raw.columns:
                df_raw[col] = _parse_datetime_flexible(df_raw[col])

        for col in NUMERIC_COLS:
            if col in df_raw.columns:
                df_raw[col] = pd.to_numeric(df_raw[col], errors="coerce")

        # NEW: Module column, derived from batch_name (not au_batch_name --
        # au_batch_name is the generic AU program name and doesn't mention the
        # module; batch_name is the one with "DS SQL - February 2026" style text)
        df_raw["Module"] = df_raw["batch_name"].apply(extract_module)

        if MODULE_FILTER:
            pattern = "|".join(MODULE_FILTER)
            df_raw = df_raw[df_raw["batch_name"].str.contains(pattern, case=False, na=False)].copy()
            print(f"  ✓ {len(df_raw)} rows after filtering to batches matching: {MODULE_FILTER}")

        # Filter by approved students
        df_filtered = df_raw[df_raw["user_id"].isin(approved_ids)].copy()

        # Merge with Groomers
        df_merged = df_filtered.merge(
            df_groomers_clean[["user_id", "Enrolled Status", "Phase"]],
            on="user_id",
            how="left",
        )

        # Merge with Master Data (dynamically)
        if master_data_available:
            available_master_cols = ["user_id"]
            desired_cols = [
                'Persona', 'Age Bracket', 'Apti Bucket', 'CTC Bracket', "Learner's Brackets",
                       '12th Bucket', 'Placeability Bucket', 'Background', 'Financial Status Cleaned','Placeability Buckets',
                       'Employment Status', 'Batch', 'Batch Name','Grad CGPA',' Grad bin','Working ','Work ex bracket',
                'Age bracket','CTC bracket','Fin Bucket'
            ]
            for col in desired_cols:
                if col in df_master.columns:
                    available_master_cols.append(col)

            df_merged = df_merged.merge(
                df_master[available_master_cols].drop_duplicates(subset="user_id"),
                on="user_id",
                how="left",
            )

        return df_merged


    # ═══════════════════════════════════════════════════════════════════════════
    # STEP 1: Load Groomers Data (Once)
    # ═══════════════════════════════════════════════════════════════════════════
    print("📥 STEP 1: Load Groomers Data")

    workbook_groomers = gc.open("Groomers")
    worksheet_groomers = workbook_groomers.worksheet("Groomers")
    data_groomers = worksheet_groomers.get_all_values()

    df_groomers = pd.DataFrame(data_groomers[1:], columns=data_groomers[0])
    df_groomers = df_groomers.rename(columns={"UserID": "user_id"})
    df_groomers["user_id"] = df_groomers["user_id"].astype(str).str.strip()

    df_groomers_clean = df_groomers[
        (df_groomers["Enrolled Status"] != "Refund Requested")
        & (df_groomers["Phase"] != "Unavailable")
        & (df_groomers["Enrolled Status"] != "DPD/Foreclosed")
    ].copy()

    approved_ids = df_groomers_clean["user_id"].unique().tolist()
    print(f"✓ Approved Students: {len(approved_ids)}")

    # ═══════════════════════════════════════════════════════════════════════════
    # STEP 2: Load Master Data (Once)
    # ═══════════════════════════════════════════════════════════════════════════
    print("\n📥 STEP 2: Load Master Data")

    try:
        workbook_master = gc.open("DS Full program - All Intake 2026")
        worksheet_master = workbook_master.worksheet("Master Data 2023-2026")
        data_master = worksheet_master.get_all_values()

        df_master = pd.DataFrame(data_master[1:], columns=data_master[0])
        df_master = df_master.rename(columns={"UserID": "user_id"})
        df_master["user_id"] = df_master["user_id"].astype(str).str.strip()
        df_master = df_master[~df_master["Persona"].isin(["NF", "#N/A"]) & df_master["Persona"].notna()].copy()

        print(f"✓ Loaded {len(df_master)} rows from Master Data")
        master_data_available = True
    except Exception as e:
        print(f"⚠️  Could not load Master Data: {e}")
        master_data_available = False

    # ═══════════════════════════════════════════════════════════════════════════
    # STEP 3: Lecture Rating (Student Level, from card 11642) + class_number
    # ═══════════════════════════════════════════════════════════════════════════
    print("\n" + "=" * 80)
    print("PROCESSING: LECTURE RATING (STUDENT LEVEL)")
    print("=" * 80)

    df_rating = load_and_merge_rating_data(RATING_CARD_ID, "Lecture Rating")
    df_rating = add_class_number(df_rating)
    print(f"✓ Merged rating data + class_number for {len(df_rating)} approved student-lecture rows")

    if len(df_rating) > 0:
        print(f"  Batches included: {sorted(df_rating['batch_name'].unique())}")
        print(f"  Modules found: {sorted(df_rating['Module'].unique())}")

    # ═══════════════════════════════════════════════════════════════════════════
    # STEP 4: Upload to Google Sheets (Student Level only)
    # ═══════════════════════════════════════════════════════════════════════════
    print("\n" + "=" * 80)
    print("UPLOADING TO GOOGLE SHEETS")
    print("=" * 80)

    sheet = gc.open_by_key(OUTPUT_SHEET_ID)

    print(f"\n☁️  Uploading {STUDENT_LEVEL_WORKSHEET}...")
    try:
        worksheet = sheet.worksheet(STUDENT_LEVEL_WORKSHEET)
        worksheet.clear()
        set_with_dataframe(worksheet, df_rating, include_index=False, include_column_header=True)
        print(f"  ✓ {STUDENT_LEVEL_WORKSHEET}")
    except Exception:
        worksheet = sheet.add_worksheet(STUDENT_LEVEL_WORKSHEET, rows=5000, cols=150)
        set_with_dataframe(worksheet, df_rating, include_index=False, include_column_header=True)
        print(f"  ✓ Created {STUDENT_LEVEL_WORKSHEET}")

    # ===== WOW Lecture Rating =====

    # ──────────────────────────────────────────────────────────────────────
    # Cell 75
    # ──────────────────────────────────────────────────────────────────────


    import pandas as pd
    import requests
    from gspread_dataframe import set_with_dataframe

    # =====================================================================
    # CONFIG
    # =====================================================================

    METABASE_HOST = "https://metabase-lierhfgoeiwhr.newtonschool.co"
    RATING_CARD_ID = 11802  # "instructor-rating-student-level-lecture-level"

    OUTPUT_SHEET_ID = "1z72t5HaE3ombCHo6kbpM3bqhnuTEBNR6L8jncdQZ7Y8"  # <-- update if this should target a different sheet
    STUDENT_LEVEL_WORKSHEET = "Lecture-Rating-StudentLevel-Class-Grouped"

    # Only keep batches whose name mentions one of these modules (case-insensitive,
    # substring match). Set to None to disable this filter and keep all batches.
    MODULE_FILTER = ["Spreadsheet", "SQL"]

    # Maps a substring found in batch_name -> display Module name (used for the
    # "Module" column). Extend as needed.
    MODULE_PATTERNS = {
        "spreadsheet": "Spreadsheets",
        "sql": "SQL",
        "python": "Python",
        "power bi": "Power BI",
        "tableau": "Tableau",
        "machine learning": "Machine Learning",
    }

    # ID columns that can arrive comma-formatted (e.g. "3,380") -- need the
    # commas stripped before they can be treated as numbers.
    ID_COLS = ["course_id", "user_id"]

    NUMERIC_COLS = ["course_id", "sum_rating_out_of_5","lectures_rated"]


    def _clean_numeric(series: pd.Series) -> pd.Series:
        """Strips thousand-separator commas (e.g. '46,620' -> 46620) before converting to numeric."""
        return pd.to_numeric(
            series.astype(str).str.replace(",", "", regex=False).str.strip(),
            errors="coerce",
        )


    def _parse_datetime_flexible(series: pd.Series) -> pd.Series:
        parsed = pd.to_datetime(series, format="ISO8601", errors="coerce")
        still_missing = parsed.isna() & series.notna()
        if still_missing.any():
            parsed.loc[still_missing] = pd.to_datetime(
                series[still_missing], format="%B %d, %Y, %I:%M %p", errors="coerce"
            )
        return parsed


    def extract_module(batch_name) -> str:
        """Derives a clean Module name from batch_name (e.g. 'DS SQL - February 2026' -> 'SQL')."""
        if pd.isna(batch_name):
            return "Other"
        name_lower = str(batch_name).lower()
        for substring, display_name in MODULE_PATTERNS.items():
            if substring in name_lower:
                return display_name
        return "Other"


    def add_class_number(df: pd.DataFrame) -> pd.DataFrame:
        """Ranks each batch's DISTINCT lectures 1, 2, 3... by lecture_start_timestamp,
        then maps that ordinal back onto every student-level row for that lecture."""
        distinct_lectures = (
            df[["batch_name"]]
            .drop_duplicates(subset=["batch_name"])
            .sort_values(["batch_name"])
        )
        distinct_lectures["class_number"] = distinct_lectures.groupby("batch_name").cumcount() + 1

        return df.merge(
            distinct_lectures[["batch_name", "class_number"]],
            on=["batch_name"],
            how="left",
        )


    def load_and_merge_rating_data(metabase_card_id, data_type_name):
        """Load student-level lecture rating data + Groomers + Master Data."""

        print(f"📥 Loading {data_type_name} Data")

        res = requests.post(
            f"{METABASE_HOST}/api/card/{metabase_card_id}/query/json",
            headers={"Content-Type": "application/json", "X-Metabase-Session": token},
            timeout=3600,
        )
        res.raise_for_status()

        df_raw = pd.DataFrame(res.json())
        print(f"  ✓ Loaded {len(df_raw)} rows")

        # Clean comma-formatted ID columns (course_id, user_id, lecture_id can
        # come back as strings like "46,620" from this export)
        for col in ID_COLS:
            if col in df_raw.columns:
                df_raw[col] = _clean_numeric(df_raw[col])

        df_raw["user_id"] = df_raw["user_id"].astype("Int64").astype(str).str.strip()

        for col in ["lecture_start_timestamp", "lecture_date", "form_fill_date", "au_start_date"]:
            if col in df_raw.columns:
                df_raw[col] = _parse_datetime_flexible(df_raw[col])

        for col in NUMERIC_COLS:
            if col in df_raw.columns:
                df_raw[col] = pd.to_numeric(df_raw[col], errors="coerce")

        # NEW: Module column, derived from batch_name (not au_batch_name --
        # au_batch_name is the generic AU program name and doesn't mention the
        # module; batch_name is the one with "DS SQL - February 2026" style text)
        df_raw["Module"] = df_raw["batch_name"].apply(extract_module)

        if MODULE_FILTER:
            pattern = "|".join(MODULE_FILTER)
            df_raw = df_raw[df_raw["batch_name"].str.contains(pattern, case=False, na=False)].copy()
            print(f"  ✓ {len(df_raw)} rows after filtering to batches matching: {MODULE_FILTER}")

        # Filter by approved students
        df_filtered = df_raw[df_raw["user_id"].isin(approved_ids)].copy()

        # Merge with Groomers
        df_merged = df_filtered.merge(
            df_groomers_clean[["user_id", "Enrolled Status", "Phase"]],
            on="user_id",
            how="left",
        )

        # Merge with Master Data (dynamically)
        if master_data_available:
            available_master_cols = ["user_id"]
            desired_cols = [
                'Persona', 'Age Bracket', 'Apti Bucket', 'CTC Bracket', "Learner's Brackets",
                       '12th Bucket', 'Placeability Bucket', 'Background', 'Financial Status Cleaned','Placeability Buckets',
                       'Employment Status', 'Batch', 'Batch Name','Grad CGPA',' Grad bin','Working ','Work ex bracket',
                'Age bracket','CTC bracket','Fin Bucket'
            ]
            for col in desired_cols:
                if col in df_master.columns:
                    available_master_cols.append(col)

            df_merged = df_merged.merge(
                df_master[available_master_cols].drop_duplicates(subset="user_id"),
                on="user_id",
                how="left",
            )

        return df_merged


    # ═══════════════════════════════════════════════════════════════════════════
    # STEP 1: Load Groomers Data (Once)
    # ═══════════════════════════════════════════════════════════════════════════
    print("📥 STEP 1: Load Groomers Data")

    workbook_groomers = gc.open("Groomers")
    worksheet_groomers = workbook_groomers.worksheet("Groomers")
    data_groomers = worksheet_groomers.get_all_values()

    df_groomers = pd.DataFrame(data_groomers[1:], columns=data_groomers[0])
    df_groomers = df_groomers.rename(columns={"UserID": "user_id"})
    df_groomers["user_id"] = df_groomers["user_id"].astype(str).str.strip()

    df_groomers_clean = df_groomers[
        (df_groomers["Enrolled Status"] != "Refund Requested")
        & (df_groomers["Phase"] != "Unavailable")
        & (df_groomers["Enrolled Status"] != "DPD/Foreclosed")
    ].copy()

    approved_ids = df_groomers_clean["user_id"].unique().tolist()
    print(f"✓ Approved Students: {len(approved_ids)}")

    # ═══════════════════════════════════════════════════════════════════════════
    # STEP 2: Load Master Data (Once)
    # ═══════════════════════════════════════════════════════════════════════════
    print("\n📥 STEP 2: Load Master Data")

    try:
        workbook_master = gc.open("DS Full program - All Intake 2026")
        worksheet_master = workbook_master.worksheet("Master Data 2023-2026")
        data_master = worksheet_master.get_all_values()

        df_master = pd.DataFrame(data_master[1:], columns=data_master[0])
        df_master = df_master.rename(columns={"UserID": "user_id"})
        df_master["user_id"] = df_master["user_id"].astype(str).str.strip()
        df_master = df_master[~df_master["Persona"].isin(["NF", "#N/A"]) & df_master["Persona"].notna()].copy()

        print(f"✓ Loaded {len(df_master)} rows from Master Data")
        master_data_available = True
    except Exception as e:
        print(f"⚠️  Could not load Master Data: {e}")
        master_data_available = False

    # ═══════════════════════════════════════════════════════════════════════════
    # STEP 3: Lecture Rating (Student Level, from card 11642) + class_number
    # ═══════════════════════════════════════════════════════════════════════════
    print("\n" + "=" * 80)
    print("PROCESSING: LECTURE RATING (STUDENT LEVEL)")
    print("=" * 80)

    df_rating = load_and_merge_rating_data(RATING_CARD_ID, "Lecture Rating")
    df_rating = add_class_number(df_rating)
    print(f"✓ Merged rating data + class_number for {len(df_rating)} approved student-lecture rows")

    if len(df_rating) > 0:
        print(f"  Batches included: {sorted(df_rating['batch_name'].unique())}")
        print(f"  Modules found: {sorted(df_rating['Module'].unique())}")

    # ═══════════════════════════════════════════════════════════════════════════
    # STEP 4: Upload to Google Sheets (Student Level only)
    # ═══════════════════════════════════════════════════════════════════════════
    print("\n" + "=" * 80)
    print("UPLOADING TO GOOGLE SHEETS")
    print("=" * 80)

    sheet = gc.open_by_key(OUTPUT_SHEET_ID)

    print(f"\n☁️  Uploading {STUDENT_LEVEL_WORKSHEET}...")
    try:
        worksheet = sheet.worksheet(STUDENT_LEVEL_WORKSHEET)
        worksheet.clear()
        set_with_dataframe(worksheet, df_rating, include_index=False, include_column_header=True)
        print(f"  ✓ {STUDENT_LEVEL_WORKSHEET}")
    except Exception:
        worksheet = sheet.add_worksheet(STUDENT_LEVEL_WORKSHEET, rows=5000, cols=150)
        set_with_dataframe(worksheet, df_rating, include_index=False, include_column_header=True)
        print(f"  ✓ Created {STUDENT_LEVEL_WORKSHEET}")

except Exception as e:
    print(f"❌ Pipeline failed: {e}")
    traceback.print_exc()
    sys.exit(1)

mins, secs = divmod(time.time() - start_time, 60)
print(f"\n🎯 Attendance X Persona Views Pipeline completed successfully in {int(mins)}m {int(secs)}s")
sys.exit(0)
# This is a sample Python script.

# Press ⌃R to execute it or replace it with your code.
# Press Double ⇧ to search everywhere for classes, files, tool windows, actions, and settings.


def print_hi(name):
    # Use a breakpoint in the code line below to debug your script.
    print(f'Hi, {name}')  # Press ⌘F8 to toggle the breakpoint.


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    print_hi('PyCharm')

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
