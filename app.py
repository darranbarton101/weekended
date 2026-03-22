"""
Travel Deal Scanner — Streamlit Dashboard
Run with:  streamlit run app.py
"""

from __future__ import annotations

import json
import logging
import os
import random
import re
from datetime import datetime
from pathlib import Path

import pandas as pd
import streamlit as st
from dotenv import load_dotenv

load_dotenv()

# Bridge Streamlit Cloud secrets to env vars (for deployment)
for _key in ("SERPAPI_KEY", "SUPABASE_URL", "SUPABASE_KEY"):
    if _key not in os.environ:
        try:
            os.environ[_key] = st.secrets[_key]
        except (KeyError, FileNotFoundError):
            pass

# enrich_flight_times removed — simplified UI
from travel_scanner.deal_store import get_connection, load_deals, mark_notified, clear_all_deals
from travel_scanner.models import DAY_NAMES, DAY_SHORT, ScanParams
from travel_scanner.scanner import load_config, run_scan_streaming


_SCAN_PHRASES = [
    "Checking the departure boards",
    "Negotiating with airlines",
    "Scanning the skies",
    "Finding hidden gems",
    "Asking the pilot nicely",
    "Rummaging through flight manifests",
    "Comparing boarding passes",
    "Haggling at the check-in desk",
    "Consulting the travel gods",
    "Decoding airport codes",
    "Consulting the departure oracle",
    "Searching under seat cushions for deals",
    "Checking behind the duty free",
    "Speed-reading timetables",
    "Triangulating cheap weekends",
    "Whispering to the booking engine",
    "Befriending a gate agent",
    "Calculating optimal layovers",
    "Inspecting the fine print",
    "Charming the fare algorithm",
    "Scouring the last-minute board",
    "Untangling codeshare agreements",
    "Peeking at the cockpit schedule",
    "Bribing the price oracle",
    "Running through the terminal",
    "Flipping through every calendar",
    "Crunching the numbers",
    "Cross-referencing sun forecasts",
    "Checking passport validity",
    "Loading the overhead bins",
]


def _random_scan_phrase() -> str:
    return random.choice(_SCAN_PHRASES)


# ── In-memory log capture ────────────────────────────────────────────────────

class _LogCapture(logging.Handler):
    def __init__(self):
        super().__init__()
        self.records: list[logging.LogRecord] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.records.append(record)


def _attach_log_capture() -> _LogCapture:
    handler = _LogCapture()
    handler.setLevel(logging.DEBUG)
    logging.getLogger("travel_scanner").addHandler(handler)
    return handler


def _detach_log_capture(handler: _LogCapture) -> None:
    logging.getLogger("travel_scanner").removeHandler(handler)


# ── Page config ──────────────────────────────────────────────────────────────

st.set_page_config(page_title="Sky Spanner", page_icon="✈", layout="wide")

# ── Favourites persistence ───────────────────────────────────────────────────

_FAV_PATH = Path("data/favourites.json")


def _load_favourites() -> dict:
    if _FAV_PATH.exists():
        try:
            return json.loads(_FAV_PATH.read_text())
        except Exception:
            pass
    return {"fav_flights": []}


def _save_favourites() -> None:
    _FAV_PATH.parent.mkdir(parents=True, exist_ok=True)
    data = {
        "fav_flights": list(st.session_state.get("fav_flights", set())),
    }
    _FAV_PATH.write_text(json.dumps(data))


if "fav_flights" not in st.session_state:
    _saved = _load_favourites()
    st.session_state["fav_flights"] = set(_saved.get("fav_flights", []))


# ── Persistent search settings ───────────────────────────────────────────────

_SEARCH_PREFS_PATH = Path("data/search_prefs.json")


def _load_search_prefs() -> dict:
    if _SEARCH_PREFS_PATH.exists():
        try:
            return json.loads(_SEARCH_PREFS_PATH.read_text())
        except Exception:
            pass
    return {}


def _save_search_prefs(prefs: dict) -> None:
    _SEARCH_PREFS_PATH.parent.mkdir(parents=True, exist_ok=True)
    _SEARCH_PREFS_PATH.write_text(json.dumps(prefs))


_saved_prefs = _load_search_prefs()


# ── CSS — Virgil Abloh archive aesthetic ─────────────────────────────────────
# Bold blue, white condensed type, monospace metadata, dashed separators

_BLUE = "#0035FF"
_BLUE_DARK = "#002AD4"
_BLUE_LIGHT = "#1A4FFF"
_BLUE_PANEL = "#0029CC"

st.markdown(f"""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800;900&family=JetBrains+Mono:wght@400;500;600;700&display=swap');

    /* ── Base ── */
    .stApp, .stApp > header {{
        background-color: {_BLUE} !important;
    }}
    .stApp {{
        color: #ffffff;
        font-family: 'Inter', -apple-system, sans-serif;
    }}

    /* Hide sidebar */
    [data-testid="stSidebarCollapsedControl"] {{ display: none !important; }}
    section[data-testid="stSidebar"] {{ display: none !important; }}

    /* Main container */
    .main .block-container {{
        padding-top: 0.8rem;
        padding-left: 2rem;
        padding-right: 2rem;
        max-width: 1400px;
    }}

    /* ── Typography ── */
    h1 {{
        color: #ffffff !important;
        font-size: 3rem !important;
        font-weight: 900 !important;
        letter-spacing: -0.02em !important;
        text-transform: uppercase;
        line-height: 1 !important;
        margin-bottom: 0 !important;
    }}
    h2 {{
        color: #ffffff !important;
        font-weight: 800 !important;
        font-size: 1.8rem !important;
        letter-spacing: -0.01em;
        text-transform: uppercase;
    }}
    h3 {{
        color: #ffffff !important;
        font-weight: 700 !important;
        font-size: 1.3rem !important;
        text-transform: uppercase;
        letter-spacing: 0.02em;
    }}

    /* Labels & text */
    label, .stCaption, [data-testid="stWidgetLabel"] p,
    [data-testid="stMarkdownContainer"] p {{
        color: rgba(255,255,255,0.7) !important;
        font-family: 'JetBrains Mono', monospace !important;
        font-size: 0.72rem !important;
    }}

    /* ── Slider ── */
    [data-testid="stSlider"] [data-testid="stThumbValue"],
    [data-testid="stSlider"] [data-testid="stTickBarMin"],
    [data-testid="stSlider"] [data-testid="stTickBarMax"] {{
        color: #ffffff !important;
        font-family: 'JetBrains Mono', monospace !important;
    }}
    .stSlider > div > div > div > div {{
        background-color: #ffffff !important;
    }}
    [data-testid="stSlider"] > div > div {{
        background-color: rgba(255,255,255,0.2) !important;
    }}

    /* ── Radio ── */
    .stRadio label span {{ color: rgba(255,255,255,0.5) !important; font-family: 'JetBrains Mono', monospace !important; font-size: 0.75rem !important; }}
    .stRadio label[data-checked="true"] span {{ color: #fff !important; font-weight: 600; }}

    /* ── Checkbox ── */
    .stCheckbox label span {{ color: rgba(255,255,255,0.7) !important; }}

    /* ── Dataframe ── */
    .stDataFrame {{
        border-radius: 0px;
        overflow: hidden;
        border: 2px solid #fff !important;
    }}
    [data-testid="stDataFrame"] table {{ background-color: {_BLUE_DARK} !important; }}
    [data-testid="stDataFrame"] th {{
        background-color: {_BLUE_PANEL} !important;
        color: rgba(255,255,255,0.7) !important;
        font-family: 'JetBrains Mono', monospace !important;
        font-size: 0.68rem !important;
        text-transform: uppercase;
        letter-spacing: 0.12em;
        font-weight: 600;
        border-bottom: 2px solid #fff !important;
    }}
    [data-testid="stDataFrame"] td {{
        color: #fff !important;
        font-family: 'JetBrains Mono', monospace !important;
        font-size: 0.78rem !important;
        border-bottom: 1px dashed rgba(255,255,255,0.15) !important;
    }}
    [data-testid="stDataFrame"] tr:hover td {{ background-color: {_BLUE_LIGHT} !important; }}

    /* ── Buttons ── */
    .stButton > button[kind="primary"] {{
        background-color: #000000;
        color: #ffffff;
        border: 2px solid #000000;
        border-radius: 0px;
        font-weight: 700;
        font-size: 0.82rem;
        font-family: 'JetBrains Mono', monospace;
        padding: 0.7rem 2rem;
        letter-spacing: 0.12em;
        text-transform: uppercase;
        transition: all 0.1s ease;
    }}
    .stButton > button[kind="primary"]:hover,
    .stButton > button[kind="primary"]:focus,
    .stButton > button[kind="primary"]:active {{
        background-color: #ffffff !important;
        color: #000000 !important;
        border-color: #000000 !important;
    }}
    .stButton > button[kind="primary"]:hover p,
    .stButton > button[kind="primary"]:focus p,
    .stButton > button[kind="primary"]:active p {{
        color: #000000 !important;
    }}
    .stButton > button:not([kind="primary"]) {{
        background-color: transparent !important;
        color: rgba(255,255,255,0.7) !important;
        border: 2px solid rgba(255,255,255,0.4) !important;
        border-radius: 0px !important;
        font-size: 0.72rem;
        font-family: 'JetBrains Mono', monospace;
        letter-spacing: 0.08em;
        text-transform: uppercase;
    }}
    .stButton > button:not([kind="primary"]) p {{
        color: rgba(255,255,255,0.7) !important;
    }}
    .stButton > button:not([kind="primary"]):hover,
    .stButton > button:not([kind="primary"]):focus {{
        color: #000000 !important;
        border-color: #fff;
        background-color: rgba(255,255,255,0.85);
    }}
    .stButton > button:not([kind="primary"]):hover p,
    .stButton > button:not([kind="primary"]):focus p {{
        color: #000000 !important;
    }}

    /* ── Selectbox / multiselect ── */
    .stSelectbox > div > div,
    .stMultiSelect > div > div {{
        background-color: rgba(0,0,0,0.2) !important;
        border-color: rgba(255,255,255,0.3) !important;
        border-radius: 0px !important;
        color: #ffffff !important;
        font-family: 'JetBrains Mono', monospace !important;
    }}
    .stMultiSelect span[data-baseweb="tag"] {{
        background-color: #fff !important;
        color: {_BLUE} !important;
        border-radius: 0px;
        font-weight: 600;
        font-family: 'JetBrains Mono', monospace;
    }}

    /* ── Progress bar ── */
    .stProgress > div > div {{ background-color: #ffffff !important; }}
    .stProgress {{ background-color: rgba(255,255,255,0.15); border-radius: 0px; }}

    /* ── Expander ── */
    .streamlit-expanderHeader {{
        background-color: rgba(0,0,0,0.12) !important;
        border-radius: 0px !important;
        color: rgba(255,255,255,0.7) !important;
        border: 2px solid rgba(255,255,255,0.2) !important;
        font-family: 'JetBrains Mono', monospace !important;
        font-weight: 600 !important;
        letter-spacing: 0.1em;
        text-transform: uppercase;
        font-size: 0.78rem !important;
    }}
    .streamlit-expanderHeader:hover {{
        background-color: rgba(0,0,0,0.2) !important;
        border-color: rgba(255,255,255,0.4) !important;
        color: #fff !important;
    }}
    .streamlit-expanderContent {{
        background-color: rgba(0,0,0,0.1) !important;
        border: 2px solid rgba(255,255,255,0.2) !important;
        border-top: none !important;
    }}
    /* Expander toggle icon */
    .streamlit-expanderHeader svg {{
        fill: rgba(255,255,255,0.5) !important;
    }}
    .streamlit-expanderHeader:hover svg {{
        fill: #fff !important;
    }}
    /* Override Streamlit's white expander summary */
    details > summary {{
        background-color: rgba(0,0,0,0.12) !important;
        color: rgba(255,255,255,0.7) !important;
        border: 2px solid rgba(255,255,255,0.2) !important;
        border-radius: 0px !important;
    }}
    details > summary:hover {{
        background-color: rgba(0,0,0,0.2) !important;
        border-color: rgba(255,255,255,0.4) !important;
        color: #fff !important;
    }}
    details[open] > summary {{
        border-bottom: none !important;
    }}
    /* Target the stExpander wrapper */
    [data-testid="stExpander"] {{
        border: none !important;
        border-radius: 0px !important;
    }}
    [data-testid="stExpander"] > details {{
        border: none !important;
        border-radius: 0px !important;
        background: transparent !important;
    }}
    [data-testid="stExpander"] > details > summary {{
        background-color: rgba(0,0,0,0.12) !important;
        color: rgba(255,255,255,0.7) !important;
        border: 2px solid rgba(255,255,255,0.2) !important;
        border-radius: 0px !important;
        font-family: 'JetBrains Mono', monospace !important;
        font-weight: 600 !important;
        letter-spacing: 0.1em;
        text-transform: uppercase;
        font-size: 0.78rem !important;
    }}
    [data-testid="stExpander"] > details > summary:hover {{
        background-color: rgba(0,0,0,0.2) !important;
        border-color: rgba(255,255,255,0.4) !important;
        color: #fff !important;
    }}
    [data-testid="stExpander"] > details > div {{
        background-color: rgba(0,0,0,0.1) !important;
        border: 2px solid rgba(255,255,255,0.2) !important;
        border-top: none !important;
        border-radius: 0px !important;
    }}

    /* ── Info / alerts ── */
    .stAlert {{ border-radius: 0px; }}
    .stInfo {{
        background-color: rgba(0,0,0,0.15) !important;
        border: 2px solid rgba(255,255,255,0.3) !important;
        color: #fff !important;
    }}

    /* ── Tabs ── */
    .stTabs [data-baseweb="tab-list"] {{
        gap: 0;
        border-bottom: 2px solid rgba(255,255,255,0.3);
    }}
    .stTabs [data-baseweb="tab"] {{
        color: rgba(255,255,255,0.4) !important;
        font-family: 'JetBrains Mono', monospace;
        font-size: 0.72rem;
        letter-spacing: 0.12em;
        text-transform: uppercase;
        font-weight: 600;
        padding: 0.7rem 2rem;
        border-bottom: 3px solid transparent;
    }}
    .stTabs [aria-selected="true"] {{
        color: #fff !important;
        border-bottom: 3px solid #fff !important;
        background: transparent !important;
    }}

    /* ── Divider ── */
    hr {{ border-color: rgba(255,255,255,0.2) !important; border-style: dashed !important; }}

    /* ── Caption ── */
    .stCaption {{ color: rgba(255,255,255,0.5) !important; font-family: 'JetBrains Mono', monospace !important; }}

    /* ── Links ── */
    a {{ color: #fff !important; }}
    a:hover {{ color: rgba(255,255,255,0.7) !important; }}

    /* ── Day-picker containers ── */
    .day-box [data-testid="stVerticalBlockBorderWrapper"] {{
        background: rgba(0,0,0,0.2) !important;
        border: 2px solid rgba(255,255,255,0.25) !important;
        border-radius: 0px !important;
    }}
    .day-box [data-testid="stVerticalBlockBorderWrapper"] > div {{
        background: rgba(0,0,0,0.2) !important;
    }}

    /* ── Compact search ── */
    .search-panel .stCheckbox {{ margin-bottom: 0 !important; margin-top: 0 !important; }}
    .search-panel .stSlider {{ margin-bottom: 0 !important; padding-top: 0 !important; }}
    .search-panel [data-testid="stHorizontalBlock"] {{
        align-items: center !important;
        gap: 0.15rem !important;
        margin-bottom: -0.1rem !important;
    }}

    /* ── Destination cards ── */
    .dest-card {{
        background: rgba(0,0,0,0.15);
        border: 2px solid rgba(255,255,255,0.25);
        padding: 20px 22px 16px;
        margin-bottom: 6px;
        min-height: 160px;
        transition: all 0.15s ease;
        position: relative;
    }}
    .dest-card:hover {{
        border-color: #fff;
        background: rgba(0,0,0,0.25);
    }}
    .dest-card.starred {{
        border-left: 4px solid #fff;
    }}

    /* ── Header bar ── */
    .header-bar {{
        display: flex;
        align-items: center;
        justify-content: space-between;
        padding: 8px 0;
        border-bottom: 2px dashed rgba(255,255,255,0.25);
        margin-bottom: 10px;
    }}
    .header-bar span {{
        color: rgba(255,255,255,0.5);
        font-family: 'JetBrains Mono', monospace;
        font-size: 0.68rem;
        letter-spacing: 0.12em;
        text-transform: uppercase;
    }}

    /* ── Metric grid ── */
    .metric-row {{
        display: flex;
        gap: 2rem;
        padding: 6px 0;
        border-bottom: 1px dashed rgba(255,255,255,0.15);
    }}
    .metric-row .metric-label {{
        color: rgba(255,255,255,0.4);
        font-family: 'JetBrains Mono', monospace;
        font-size: 0.65rem;
        text-transform: uppercase;
        letter-spacing: 0.1em;
    }}
    .metric-row .metric-value {{
        color: #fff;
        font-family: 'JetBrains Mono', monospace;
        font-size: 0.85rem;
        font-weight: 700;
    }}

    /* ── Airline tags ── */
    .airline-tag {{
        display: inline-block;
        border: 1px solid rgba(255,255,255,0.4);
        color: rgba(255,255,255,0.8);
        padding: 2px 8px;
        font-family: 'JetBrains Mono', monospace;
        font-size: 0.6rem;
        letter-spacing: 0.08em;
        text-transform: uppercase;
        margin-right: 4px;
        margin-bottom: 4px;
    }}
    .airline-tag.primary {{
        border-color: rgba(255,255,255,0.5);
        color: rgba(255,255,255,0.9);
    }}

    /* ── Popover button ── */
    [data-testid="stPopoverButton"] > button,
    [data-testid="stPopoverButton"] > button[kind="secondary"] {{
        background-color: transparent !important;
        color: rgba(255,255,255,0.7) !important;
        border: 2px solid rgba(255,255,255,0.4) !important;
        border-radius: 0px !important;
        font-family: 'JetBrains Mono', monospace !important;
        font-size: 0.72rem !important;
        letter-spacing: 0.08em !important;
        text-transform: uppercase !important;
    }}
    [data-testid="stPopoverButton"] > button p,
    [data-testid="stPopoverButton"] > button[kind="secondary"] p {{
        color: rgba(255,255,255,0.7) !important;
    }}
    [data-testid="stPopoverButton"] > button:hover,
    [data-testid="stPopoverButton"] > button[kind="secondary"]:hover {{
        color: #000000 !important;
        border-color: #fff !important;
        background-color: rgba(255,255,255,0.85) !important;
    }}
    [data-testid="stPopoverButton"] > button:hover p,
    [data-testid="stPopoverButton"] > button[kind="secondary"]:hover p {{
        color: #000000 !important;
    }}

    /* ── Secondary buttons (Streamlit default white) ── */
    button[kind="secondary"] {{
        background-color: transparent !important;
        color: rgba(255,255,255,0.7) !important;
        border: 2px solid rgba(255,255,255,0.4) !important;
        border-radius: 0px !important;
    }}
    button[kind="secondary"] p {{
        color: rgba(255,255,255,0.7) !important;
    }}
    button[kind="secondary"]:hover {{
        color: #000000 !important;
        background-color: rgba(255,255,255,0.85) !important;
        border-color: #fff !important;
    }}
    button[kind="secondary"]:hover p {{
        color: #000000 !important;
    }}

    /* ── Scanning animation ── */
    @keyframes scanning-dots {{
        0% {{ content: '.'; }}
        33% {{ content: '..'; }}
        66% {{ content: '...'; }}
    }}
    .scan-status {{
        font-family: 'JetBrains Mono', monospace;
        font-size: 0.85rem;
        color: #fff;
        font-weight: 700;
        letter-spacing: 0.1em;
        text-transform: uppercase;
    }}
    .scan-status::after {{
        content: '...';
        animation: scanning-dots 1.5s infinite;
    }}

    /* Dotted grid decoration */
    .dot-separator {{
        border-top: 2px dotted rgba(255,255,255,0.2);
        margin: 6px 0;
    }}

</style>
""", unsafe_allow_html=True)

# ── Load config ──────────────────────────────────────────────────────────────

@st.cache_resource
def get_config() -> dict:
    return load_config()

config = get_config()
dp = config.get("destination_preferences", {})
db_path = config.get("database", {}).get("path", "data/deals.db")

# ── Airport options ──────────────────────────────────────────────────────────

UK_IRISH_AIRPORTS = {
    "ABZ": "ABZ — Aberdeen", "BFS": "BFS — Belfast Intl",
    "BHD": "BHD — Belfast City", "BHX": "BHX — Birmingham",
    "BLK": "BLK — Blackpool", "BOH": "BOH — Bournemouth",
    "BRS": "BRS — Bristol", "CWL": "CWL — Cardiff",
    "DSA": "DSA — Doncaster Sheffield", "DUB": "DUB — Dublin",
    "EDI": "EDI — Edinburgh", "EMA": "EMA — East Midlands",
    "EXT": "EXT — Exeter", "GLA": "GLA — Glasgow Intl",
    "HUY": "HUY — Humberside", "INV": "INV — Inverness",
    "KIR": "KIR — Kerry", "LBA": "LBA — Leeds Bradford",
    "LCY": "LCY — London City", "LGW": "LGW — London Gatwick",
    "LHR": "LHR — London Heathrow", "LTN": "LTN — London Luton",
    "MAN": "MAN — Manchester", "MME": "MME — Teesside",
    "NCL": "NCL — Newcastle", "NOC": "NOC — Ireland West (Knock)",
    "NWI": "NWI — Norwich", "ORK": "ORK — Cork",
    "PIK": "PIK — Glasgow Prestwick", "SNN": "SNN — Shannon",
    "SOU": "SOU — Southampton", "STN": "STN — London Stansted",
    "SWS": "SWS — Swansea",
}

INTERNATIONAL_AIRPORTS = {
    "AMS": "AMS — Amsterdam", "ARN": "ARN — Stockholm Arlanda",
    "ATH": "ATH — Athens", "BCN": "BCN — Barcelona",
    "BER": "BER — Berlin", "BGY": "BGY — Milan Bergamo",
    "BRU": "BRU — Brussels", "BUD": "BUD — Budapest",
    "CDG": "CDG — Paris CDG", "CPH": "CPH — Copenhagen",
    "DUS": "DUS — Dusseldorf", "FCO": "FCO — Rome Fiumicino",
    "FRA": "FRA — Frankfurt", "GVA": "GVA — Geneva",
    "HAM": "HAM — Hamburg", "HEL": "HEL — Helsinki",
    "LIS": "LIS — Lisbon", "MAD": "MAD — Madrid",
    "MRS": "MRS — Marseille", "MUC": "MUC — Munich",
    "MXP": "MXP — Milan Malpensa", "NAP": "NAP — Naples",
    "OPO": "OPO — Porto", "ORY": "ORY — Paris Orly",
    "OSL": "OSL — Oslo", "PRG": "PRG — Prague",
    "PSA": "PSA — Pisa", "TLS": "TLS — Toulouse",
    "VIE": "VIE — Vienna", "WAW": "WAW — Warsaw",
    "ZAG": "ZAG — Zagreb", "ZRH": "ZRH — Zurich",
    "KEF": "KEF — Reykjavik Keflavik", "AEY": "AEY — Akureyri",
    "JFK": "JFK — New York JFK", "EWR": "EWR — New York Newark",
    "LGA": "LGA — New York LaGuardia", "BOS": "BOS — Boston",
    "IAD": "IAD — Washington Dulles", "ORD": "ORD — Chicago O'Hare",
    "LAX": "LAX — Los Angeles", "SFO": "SFO — San Francisco",
    "MIA": "MIA — Miami", "ATL": "ATL — Atlanta",
    "YYZ": "YYZ — Toronto Pearson", "YUL": "YUL — Montreal",
    "YVR": "YVR — Vancouver",
}

AIRPORT_OPTIONS = {**UK_IRISH_AIRPORTS, **INTERNATIONAL_AIRPORTS}

# Airport code → friendly name mapping
_airport_names = {code: label.split(" — ")[1] if " — " in label else code
                  for code, label in AIRPORT_OPTIONS.items()}

# ── Day row helper ───────────────────────────────────────────────────────────

def _day_row(wd: int, enabled_default: bool, time_default: tuple[int, int],
             key_prefix: str) -> tuple[bool, tuple[str, str]]:
    c1, c2 = st.columns([1, 5])
    enabled = c1.checkbox(
        "on", value=enabled_default,
        key=f"{key_prefix}_{wd}", label_visibility="collapsed",
    )
    _col = "#ffffff" if enabled else "rgba(255,255,255,0.3)"
    _wt = "600" if enabled else "400"
    c2.markdown(
        f"<p style='margin:0;padding:7px 0 0;font-size:0.78rem;"
        f"font-weight:{_wt};color:{_col};font-family:JetBrains Mono,monospace;"
        f"letter-spacing:0.08em;text-transform:uppercase'>"
        f"{DAY_NAMES[wd][:3]}</p>",
        unsafe_allow_html=True,
    )
    if enabled:
        return True, ("00:00", "23:59")
    else:
        return False, ("00:00", "23:59")


# ── Header ───────────────────────────────────────────────────────────────────

_title_col, _home_col = st.columns([6, 1])
with _title_col:
    st.title("Sky Spanner")
with _home_col:
    if st.session_state.get("selected_dest"):
        if st.button("← Home", key="home_btn"):
            st.session_state["selected_dest"] = None
            st.rerun()

_last_scan_str = ""
try:
    _sb = get_connection(db_path)
    _last_row = _sb.table("deals").select("last_seen").order("last_seen", desc=True).limit(1).execute()
    if _last_row.data:
        _last_dt = datetime.fromisoformat(_last_row.data[0]["last_seen"])
        _last_scan_str = _last_dt.strftime("%d/%m/%y  %H:%M")
except Exception:
    pass

_now_str = datetime.now().strftime("%d/%m/%y")

st.markdown(
    f"""<div class="header-bar">
        <span>Last scan ———— {_last_scan_str if _last_scan_str else 'N/A'}</span>
        <span>{_now_str} ———— Sky Spanner ———— V.015</span>
    </div>""",
    unsafe_allow_html=True,
)

# ── Search panel ─────────────────────────────────────────────────────────────

# Manual toggle — not st.expander, so we can force-close it
if "search_open" not in st.session_state:
    st.session_state["search_open"] = False
_is_searching = st.session_state.get("_run_search", False)

# Toggle button
_toggle_label = "▼ [ Search ]" if st.session_state["search_open"] else "► [ Search ]"
if st.button(_toggle_label, key="search_toggle", use_container_width=True):
    st.session_state["search_open"] = not st.session_state["search_open"]
    st.rerun()

# Widgets always render (hidden or not) so state persists via keys
# Use a container we can show/hide
_show_search = st.session_state["search_open"] and not _is_searching

if _show_search:
    st.markdown(
        "<div style='background:rgba(0,0,0,0.1);border:2px solid rgba(255,255,255,0.2);"
        "padding:16px 20px 10px;margin-bottom:8px'>",
        unsafe_allow_html=True,
    )

    r1c1, r1c2, r1c3, r1c4 = st.columns([3, 1.5, 1.5, 1.5])

    with r1c1:
        st.caption("AIRPORTS (UP TO 3)")
        _pref_airports = _saved_prefs.get("airports", ["GLA", "EDI"])
        selected_airports = st.multiselect(
            "airports",
            options=list(AIRPORT_OPTIONS.keys()),
            default=_pref_airports,
            format_func=lambda x: AIRPORT_OPTIONS.get(x, x),
            label_visibility="collapsed",
            max_selections=3,
        )
        origins = selected_airports or ["GLA"]

    with r1c2:
        st.caption("MONTHS OUT")
        _pref_months = _saved_prefs.get("month_range", [1, 6])
        month_range = st.slider("Months", 1, 12, tuple(_pref_months), label_visibility="collapsed")

    with r1c3:
        st.caption("MAX PRICE")
        _pref_price = _saved_prefs.get("max_price", int(dp.get("max_price_gbp", 300)))
        max_price = st.slider(
            "Max price", min_value=20, max_value=600,
            value=_pref_price, step=10,
            format="£%d", label_visibility="collapsed",
        )

    with r1c4:
        st.caption("STOPS")
        _stops_opts = ["Direct", "1 stop", "Any"]
        _pref_stops = _saved_prefs.get("stops", "Any")
        _stops_idx = _stops_opts.index(_pref_stops) if _pref_stops in _stops_opts else 2
        stops_label = st.radio(
            "Stops", _stops_opts,
            index=_stops_idx, horizontal=True, label_visibility="collapsed",
        )
        max_stopovers = {"Direct": 0, "1 stop": 1, "Any": 2}[stops_label]

    # Day pickers
    r2c1, r2c2 = st.columns(2)

    with r2c1:
        st.markdown('<div class="day-box">', unsafe_allow_html=True)
        with st.container(border=True):
            st.markdown(
                "<p style='margin:0 0 4px;color:rgba(255,255,255,0.5);font-size:0.68rem;"
                "font-weight:700;text-transform:uppercase;letter-spacing:0.15em;"
                "font-family:JetBrains Mono,monospace'>Depart on</p>",
                unsafe_allow_html=True,
            )
            _dep_defaults = {3: (17, 23), 4: (0, 11)}
            _pref_dep = _saved_prefs.get("dep_days", {})
            departure_days: dict[int, tuple[str, str]] = {}
            for wd in range(7):
                _wd_str = str(wd)
                if _pref_dep and _wd_str in _pref_dep:
                    _en_def = True
                    _t_def = (int(_pref_dep[_wd_str][0].split(":")[0]), int(_pref_dep[_wd_str][1].split(":")[0]))
                elif _pref_dep:
                    _en_def = False
                    _t_def = _dep_defaults.get(wd, (6, 22))
                else:
                    _en_def = wd in (3, 4)
                    _t_def = _dep_defaults.get(wd, (6, 22))
                _on, _times = _day_row(
                    wd, enabled_default=_en_def,
                    time_default=_t_def,
                    key_prefix="dep",
                )
                if _on:
                    departure_days[wd] = _times
            if not departure_days:
                departure_days[3] = ("17:00", "23:59")
        st.markdown('</div>', unsafe_allow_html=True)

    with r2c2:
        st.markdown('<div class="day-box">', unsafe_allow_html=True)
        with st.container(border=True):
            st.markdown(
                "<p style='margin:0 0 4px;color:rgba(255,255,255,0.5);font-size:0.68rem;"
                "font-weight:700;text-transform:uppercase;letter-spacing:0.15em;"
                "font-family:JetBrains Mono,monospace'>Return on</p>",
                unsafe_allow_html=True,
            )
            _ret_defaults = {6: (17, 23), 0: (0, 11)}
            _pref_ret = _saved_prefs.get("ret_days", {})
            return_days: dict[int, tuple[str, str]] = {}
            for wd in range(7):
                _wd_str = str(wd)
                if _pref_ret and _wd_str in _pref_ret:
                    _en_def = True
                    _t_def = (int(_pref_ret[_wd_str][0].split(":")[0]), int(_pref_ret[_wd_str][1].split(":")[0]))
                elif _pref_ret:
                    _en_def = False
                    _t_def = _ret_defaults.get(wd, (14, 23))
                else:
                    _en_def = wd == 6
                    _t_def = _ret_defaults.get(wd, (14, 23))
                _on, _times = _day_row(
                    wd, enabled_default=_en_def,
                    time_default=_t_def,
                    key_prefix="ret",
                )
                if _on:
                    return_days[wd] = _times
            if not return_days:
                return_days[6] = ("17:00", "23:59")
        st.markdown('</div>', unsafe_allow_html=True)

    # Row 3
    r3c1, r3c2 = st.columns([3, 1.5])

    serpapi_key = os.environ.get("SERPAPI_KEY", "")

    with r3c1:
        _serp_status = "CONNECTED" if serpapi_key else "KEY NOT SET"
        _status_col = "rgba(0,255,100,0.6)" if serpapi_key else "rgba(255,100,100,0.6)"
        st.markdown(
            f"<span style='font-family:JetBrains Mono,monospace;font-size:0.68rem;"
            f"color:{_status_col};letter-spacing:0.08em'>● SERPAPI {_serp_status}</span>",
            unsafe_allow_html=True,
        )
        use_serpapi_ui = bool(serpapi_key)
        use_ryanair_ui = False
        force_refresh_ui = st.checkbox("Force refresh (bypass cache)", value=False, key="force_refresh")

    with r3c2:
        st.write("")
        run_search = st.button("Search", type="primary", use_container_width=True)

    st.markdown("</div>", unsafe_allow_html=True)

else:
    # Search panel hidden — use session state, then saved prefs, then defaults
    run_search = False
    origins = st.session_state.get("_last_origins", _saved_prefs.get("airports", ["GLA", "EDI"]))
    month_range = st.session_state.get("_last_month_range", tuple(_saved_prefs.get("month_range", [1, 6])))
    max_price = st.session_state.get("_last_max_price", _saved_prefs.get("max_price", int(dp.get("max_price_gbp", 300))))
    _stops_map = {"Direct": 0, "1 stop": 1, "Any": 2}
    max_stopovers = st.session_state.get("_last_max_stopovers", _stops_map.get(_saved_prefs.get("stops", "Any"), 2))
    _pref_dep_raw = _saved_prefs.get("dep_days", {})
    _dep_default = {int(k): tuple(v) for k, v in _pref_dep_raw.items()} if _pref_dep_raw else {3: ("17:00", "23:59"), 4: ("00:00", "11:59")}
    departure_days = st.session_state.get("_last_dep_days", _dep_default)
    _pref_ret_raw = _saved_prefs.get("ret_days", {})
    _ret_default = {int(k): tuple(v) for k, v in _pref_ret_raw.items()} if _pref_ret_raw else {6: ("17:00", "23:59")}
    return_days = st.session_state.get("_last_ret_days", _ret_default)
    serpapi_key = os.environ.get("SERPAPI_KEY", "")
    use_serpapi_ui = bool(serpapi_key)
    use_ryanair_ui = False
    force_refresh_ui = False

# Save current params so they persist when panel is closed AND across sessions
if _show_search:
    st.session_state["_last_origins"] = origins
    st.session_state["_last_month_range"] = month_range
    st.session_state["_last_max_price"] = max_price
    st.session_state["_last_max_stopovers"] = max_stopovers
    st.session_state["_last_dep_days"] = departure_days
    st.session_state["_last_ret_days"] = return_days
    st.session_state["_force_refresh"] = force_refresh_ui

    # Persist to disk for cross-session recall
    _stopovers_to_label = {0: "Direct", 1: "1 stop", 2: "Any"}
    _save_search_prefs({
        "airports": origins,
        "month_range": list(month_range),
        "max_price": max_price,
        "stops": _stopovers_to_label.get(max_stopovers, "Any"),
        "dep_days": {str(k): list(v) for k, v in departure_days.items()},
        "ret_days": {str(k): list(v) for k, v in return_days.items()},
    })

# ── Build ScanParams ─────────────────────────────────────────────────────────

scan_params = ScanParams(
    origins=origins,
    departure_days=departure_days,
    return_days=return_days,
    near_term_enabled=False,
    near_term_weeks_from=4,
    near_term_weeks_to=8,
    long_term_enabled=True,
    long_term_months_from=month_range[0],
    long_term_months_to=month_range[1],
    max_price_gbp=float(max_price),
    min_price_gbp=float(dp.get("min_price_gbp", 20)),
    max_stopovers=max_stopovers,
    results_per_call=int(config.get("api", {}).get("results_per_call", 50)),
    preferred_regions=dp.get("preferred_regions", []),
    show_all=dp.get("show_all", True),
    top_deals_count=int(dp.get("top_deals_count", 50)),
    price_bucket_gbp=float(dp.get("price_bucket_gbp", 5)),
    use_serpapi=use_serpapi_ui,
    use_ryanair=use_ryanair_ui,
)

# ── Helpers ──────────────────────────────────────────────────────────────────

def _deal_source(deal) -> str:
    return "Ryanair" if "ryanair" in (deal.airline or "").lower() else "Other"


_KIWI_RE = re.compile(
    r"kiwi\.com/en/search/results/([A-Z]{3})/([A-Z]{3})/(\d{4}-\d{2}-\d{2})/(\d{4}-\d{2}-\d{2})"
)


def _fix_deep_link(link: str) -> str:
    if not link:
        return ""
    m = _KIWI_RE.search(link)
    if m:
        origin, dest, out_str, ret_str = m.groups()
        from datetime import date as _date
        out_d = _date.fromisoformat(out_str)
        ret_d = _date.fromisoformat(ret_str)
        return (
            f"https://www.skyscanner.net/transport/flights"
            f"/{origin.lower()}/{dest.lower()}"
            f"/{out_d.strftime('%y%m%d')}/{ret_d.strftime('%y%m%d')}"
            f"/?adults=1&currency=GBP"
        )
    return link


def _fmt_dt(dt):
    if not dt:
        return ""
    if dt.hour == 0 and dt.minute == 0:
        return dt.strftime("%a %d %b")
    return dt.strftime("%a %d %b %H:%M")


def _share_deal_text(d) -> str:
    city = d.destination_city or d.destination
    dep = _fmt_dt(d.outbound_departure)
    ret = _fmt_dt(d.return_departure)
    return (
        f"{city} ({d.destination_country}) - GBP{d.price_gbp:.0f} return"
        f" | {dep} -> {ret} | {d.airline}"
        f" | {_fix_deep_link(d.deep_link)}"
    )


def _make_df(deals_list) -> pd.DataFrame:
    rows = []
    for d in deals_list:
        rows.append({
            "Dest":      d.destination_city or d.destination,
            "Country":   d.destination_country,
            "From":      d.origin,
            "Dep":       _fmt_dt(d.outbound_departure),
            "Return":    _fmt_dt(d.return_departure),
            "Nights":    d.nights,
            "Price (£)": round(d.price_gbp, 0),
            "Airline":   d.airline,
            "Stops":     d.stops,
            "Book":      _fix_deep_link(d.deep_link),
        })
    return pd.DataFrame(rows)


def _col_config() -> dict:
    return {
        "From":      st.column_config.TextColumn("From", width="small"),
        "Dep":       st.column_config.TextColumn("Outbound", width="medium"),
        "Return":    st.column_config.TextColumn("Return", width="medium"),
        "Nights":    st.column_config.NumberColumn("Nts", width="small"),
        "Price (£)": st.column_config.NumberColumn("Price", width="small", format="£%.0f"),
        "Airline":   st.column_config.TextColumn(width="medium"),
        "Stops":     st.column_config.NumberColumn("Stops", width="small"),
        "Book":      st.column_config.LinkColumn("Book", width="small", display_text="Book →"),
    }


def _render_live_deals_html(deals_list) -> str:
    """Render live deals as styled HTML for scanning progress."""
    groups = {}
    for d in deals_list:
        key = d.destination_city or d.destination
        if key not in groups:
            groups[key] = {"city": key, "country": d.destination_country or "",
                          "min_price": d.price_gbp, "count": 0}
        groups[key]["count"] += 1
        if d.price_gbp < groups[key]["min_price"]:
            groups[key]["min_price"] = d.price_gbp

    sorted_g = sorted(groups.values(), key=lambda g: g["min_price"])
    cards = []
    plural = lambda n: "s" if n != 1 else ""
    for g in sorted_g[:18]:  # Show top 18
        cards.append(
            f'<div style="background:rgba(255,255,255,0.06);border:1px solid rgba(255,255,255,0.12);'
            f'border-radius:6px;padding:8px 12px;min-width:160px;flex:1;opacity:0.55">'
            f'<div style="display:flex;justify-content:space-between;align-items:baseline">'
            f'<span style="font-weight:700;color:rgba(255,255,255,0.7);text-transform:uppercase;'
            f'font-size:0.7rem;letter-spacing:0.04em">{g["city"]}</span>'
            f'<span style="font-weight:800;color:rgba(255,255,255,0.7);font-size:0.8rem">£{g["min_price"]:.0f}</span>'
            f'</div>'
            f'<div style="color:rgba(255,255,255,0.3);font-family:JetBrains Mono,monospace;'
            f'font-size:0.55rem;margin-top:3px;letter-spacing:0.08em;text-transform:uppercase">'
            f'{g["country"]} — {g["count"]} date{plural(g["count"])}</div>'
            f'</div>'
        )

    # Arrange in rows of 3
    rows_html = ""
    for i in range(0, len(cards), 3):
        row = cards[i:i+3]
        rows_html += '<div style="display:flex;gap:6px;margin-bottom:6px">' + "".join(row) + '</div>'

    return rows_html


def _group_destinations(deals_list):
    groups = {}
    for d in deals_list:
        key = d.destination_city or d.destination
        if key not in groups:
            groups[key] = {
                "city": key,
                "country": d.destination_country or "",
                "dest_code": d.destination,
                "deals": [],
                "min_price": d.price_gbp,
                "airlines": set(),
                "origins": set(),
            }
        g = groups[key]
        g["deals"].append(d)
        g["airlines"].add(d.airline or "Unknown")
        g["origins"].add(d.origin)
        if d.price_gbp < g["min_price"]:
            g["min_price"] = d.price_gbp
    for g in groups.values():
        g["deals"].sort(key=lambda d: d.price_gbp)
        cheapest = g["deals"][0]
        g["example_dep"] = _fmt_dt(cheapest.outbound_departure)
        g["example_ret"] = _fmt_dt(cheapest.return_departure)
        g["example_nights"] = cheapest.nights
        g["deal_count"] = len(g["deals"])
    return groups


# ── Run search ───────────────────────────────────────────────────────────────

# Step 1: When search button clicked, close panel, set flag and rerun
if run_search:
    st.session_state["_run_search"] = True
    st.session_state["search_open"] = False
    st.session_state.pop("deals", None)
    st.session_state.pop("last_log", None)
    st.session_state["selected_dest"] = None
    st.session_state.pop("_enriched_flights", None)
    st.rerun()

# Step 2: On rerun, expander is collapsed and we execute the actual scan
if _is_searching:
    st.session_state.pop("_run_search", None)

    if not os.environ.get("SERPAPI_KEY") and not config.get("ryanair", {}).get("enabled", True):
        st.error("No API source configured.")
    else:
        # Clear old results — wipe DB so stale deals don't persist
        conn = get_connection(db_path)
        clear_all_deals(conn)
        # conn.close()  # Supabase client doesn't need closing



        # Calculate total routes for the "big job" summary
        from travel_scanner.date_windows import generate_windows
        from travel_scanner.api_client_serpapi import _monthly_weekend_pairs
        _windows = generate_windows(scan_params)
        _total_routes = sum(len(_monthly_weekend_pairs(w, scan_params)) for w in _windows) * len(origins) if _windows else 0
        _n_airports = len(origins)
        _airport_list = ", ".join(_airport_names.get(o, o) for o in origins)

        # Progress section
        route_summary = st.empty()
        route_summary.markdown(
            f"<p style='font-family:JetBrains Mono,monospace;font-size:0.78rem;"
            f"color:rgba(255,255,255,0.8);letter-spacing:0.06em;margin:8px 0 12px'>"
            f"SCANNING {_total_routes} ROUTES ACROSS {_n_airports} AIRPORT{'S' if _n_airports != 1 else ''}"
            f" ({_airport_list})</p>",
            unsafe_allow_html=True,
        )
        scan_phrase_slot = st.empty()
        scan_phrase_slot.markdown(
            f'<p class="scan-status">{_random_scan_phrase()}</p>',
            unsafe_allow_html=True,
        )
        progress_bar = st.progress(0.0)
        status_line = st.empty()
        live_count = st.empty()
        cards_slot = st.empty()

        log_handler = _attach_log_capture()
        step_log: list[str] = []
        _last_phrase_step = -1

        try:
            _force = st.session_state.get("_force_refresh", False)
            for _live_deals, msg, step, total in run_scan_streaming(scan_params, config, force_refresh=_force):
                frac = min(step / max(total, 1), 1.0)
                pct = int(frac * 100)
                progress_bar.progress(frac)

                # Show airport-specific scanning message
                _origin_in_msg = None
                for _code in origins:
                    if _code in msg:
                        _origin_in_msg = _code
                        break
                _airport_label = _airport_names.get(_origin_in_msg, _origin_in_msg) if _origin_in_msg else None

                if step - _last_phrase_step >= 2:
                    _phrase = f"Scanning {_airport_label} departures" if _airport_label else _random_scan_phrase()
                    scan_phrase_slot.markdown(
                        f'<p class="scan-status">{_phrase}</p>',
                        unsafe_allow_html=True,
                    )
                    _last_phrase_step = step

                # Friendly airport name in status line
                _friendly_msg = msg
                for _code, _name in _airport_names.items():
                    if f" {_code} " in msg:
                        _friendly_msg = msg.replace(f" {_code} ", f" {_name} ({_code}) ")
                        break

                status_line.markdown(
                    f"<p style='font-family:JetBrains Mono,monospace;font-size:0.72rem;"
                    f"color:rgba(255,255,255,0.6);letter-spacing:0.05em;margin:2px 0'>"
                    f"[{pct}%] {_friendly_msg}</p>",
                    unsafe_allow_html=True,
                )
                step_log.append(msg)
                if _live_deals:
                    n = len(_live_deals)
                    dests = len({d.destination_city or d.destination for d in _live_deals})
                    live_count.markdown(
                        f"<p style='font-family:JetBrains Mono,monospace;font-size:0.75rem;"
                        f"color:#fff;letter-spacing:0.08em;margin:4px 0'>"
                        f"<b>{n}</b> DEALS ———— <b>{dests}</b> DESTINATIONS</p>",
                        unsafe_allow_html=True,
                    )
                    cards_slot.markdown(
                        _render_live_deals_html(_live_deals),
                        unsafe_allow_html=True,
                    )
        finally:
            _detach_log_capture(log_handler)

        progress_bar.progress(1.0)
        route_summary.empty()
        scan_phrase_slot.empty()
        status_line.empty()
        live_count.empty()
        cards_slot.empty()

        errors   = [r for r in log_handler.records if r.levelno >= logging.ERROR]
        warnings = [r for r in log_handler.records if r.levelno == logging.WARNING]
        infos    = [r for r in log_handler.records if r.levelno == logging.INFO]

        if errors:
            st.error(f"{len(errors)} error(s)")
        elif warnings:
            st.warning(f"{len(warnings)} warning(s)")
        else:
            st.success(f"Done — {len(infos)} API calls")

        with st.expander("Search log", expanded=bool(errors or warnings)):
            for line in step_log:
                st.markdown(f"`{line}`")

        st.session_state["last_log"] = {
            "step_log": step_log,
            "errors":   [(r.name, r.getMessage()) for r in errors],
            "warnings": [(r.name, r.getMessage()) for r in warnings],
            "infos":    len(infos),
        }

        conn = get_connection(db_path)
        all_deals = load_deals(conn)
        # conn.close()  # Supabase client doesn't need closing
        st.session_state["deals"] = all_deals
        st.rerun()

# ── Load deals ───────────────────────────────────────────────────────────────

if "deals" not in st.session_state:
    conn = get_connection(db_path)
    st.session_state["deals"] = load_deals(conn)
    # conn.close()  # Supabase client doesn't need closing

# Filter deals to only show results from currently selected airports
_all_deals = st.session_state.get("deals", [])
deals = [d for d in _all_deals if d.origin in origins]

if "selected_dest" not in st.session_state:
    st.session_state["selected_dest"] = None

# ── Tabs ─────────────────────────────────────────────────────────────────────

tab_all, tab_favs = st.tabs(["All Destinations", "Favourites"])

# ══════════════════════════════════════════════════════════════════════════════
# ALL DESTINATIONS TAB
# ══════════════════════════════════════════════════════════════════════════════

with tab_all:
    tc1, tc2 = st.columns([4, 1.5])

    with tc1:
        if deals:
            _n_dests = len({d.destination_city or d.destination for d in deals})
            st.markdown(
                f"<span style='font-family:JetBrains Mono,monospace;font-size:0.75rem;"
                f"color:rgba(255,255,255,0.6);letter-spacing:0.05em'>"
                f"<b style='color:#fff;font-size:1.1rem'>{len(deals)}</b> DEALS "
                f"———— {_n_dests} DESTINATIONS</span>",
                unsafe_allow_html=True,
            )
        else:
            st.markdown(
                "<span style='font-family:JetBrains Mono,monospace;font-size:0.75rem;"
                "color:rgba(255,255,255,0.4)'>NO DEALS — RUN A SEARCH</span>",
                unsafe_allow_html=True,
            )

    with tc2:
        sort_opt = st.selectbox(
            "Sort by", ["Price", "Destination", "Dates available"],
            label_visibility="collapsed",
        )

    filtered = deals[:]

    # Last search log
    if "last_log" in st.session_state:
        log = st.session_state["last_log"]
        errors_saved = log.get("errors", [])
        warnings_saved = log.get("warnings", [])
        step_log_saved = log.get("step_log", [])

        if errors_saved:
            st.error(f"Last search: {len(errors_saved)} error(s)")
        elif warnings_saved:
            st.warning(f"{len(warnings_saved)} warning(s)")

        with st.expander("[ Search Log ]", expanded=bool(errors_saved)):
            for line in step_log_saved:
                st.markdown(f"`{line}`")

    # ── DETAIL VIEW ──────────────────────────────────────────────────────────

    selected = st.session_state.get("selected_dest")

    if selected and filtered:
        dest_deals = [d for d in filtered
                      if (d.destination_city or d.destination) == selected]

        if not dest_deals:
            st.session_state["selected_dest"] = None
            st.rerun()

        bc1, bc2 = st.columns([6, 1])
        with bc1:
            if st.button("← Back"):
                st.session_state["selected_dest"] = None
                st.rerun()

        cheapest = min(dest_deals, key=lambda d: d.price_gbp)
        airlines_str = ", ".join(sorted({d.airline or "?" for d in dest_deals}))
        country = cheapest.destination_country or ""

        st.markdown(
            f"<div style='margin:8px 0 4px'>"
            f"<span style='font-size:2rem;font-weight:900;color:#fff;text-transform:uppercase;"
            f"letter-spacing:-0.01em'>{selected}</span>"
            + (f"<span style='color:rgba(255,255,255,0.5);font-family:JetBrains Mono,monospace;"
               f"font-size:0.8rem;margin-left:14px'>{country}</span>" if country else "")
            + f"</div>"
            f"<div style='font-family:JetBrains Mono,monospace;font-size:0.85rem;"
            f"color:rgba(255,255,255,0.7);margin-bottom:12px'>"
            f"FROM £{cheapest.price_gbp:.0f} ———— {len(dest_deals)} OPTIONS ———— {airlines_str.upper()}"
            f"</div>",
            unsafe_allow_html=True,
        )

        # Map locator
        _map_query = f"{selected} {country}"
        _map_embed_q = _map_query.replace(' ', '+')
        with st.popover("🌍 Where is this?", key="map_detail"):
            st.markdown(
                f"<p style='font-family:JetBrains Mono,monospace;font-size:0.8rem;"
                f"color:#fff;letter-spacing:0.05em;margin-bottom:8px'>"
                f"<b>🌍 {selected}, {country.upper()}</b></p>",
                unsafe_allow_html=True,
            )
            st.markdown(
                f'<iframe width="380" height="300" style="border:0;border-radius:8px" loading="lazy" '
                f'referrerpolicy="no-referrer-when-downgrade" '
                f'src="https://maps.google.com/maps?q={_map_embed_q}&output=embed&z=6">'
                f'</iframe>',
                unsafe_allow_html=True,
            )
            _map_url = f"https://www.google.com/maps/search/?api=1&query={_map_embed_q}"
            st.markdown(
                f"<p style='font-family:JetBrains Mono,monospace;font-size:0.65rem;"
                f"color:rgba(255,255,255,0.5);margin-top:8px;letter-spacing:0.05em'>"
                f"✈ {cheapest.destination} · <a href='{_map_url}' target='_blank' "
                f"style='color:rgba(100,180,255,0.8)'>Open in Google Maps →</a></p>",
                unsafe_allow_html=True,
            )

        st.markdown('<div class="dot-separator"></div>', unsafe_allow_html=True)

        st.markdown('<div class="dot-separator"></div>', unsafe_allow_html=True)

        sorted_deals = sorted(dest_deals, key=lambda d: d.price_gbp)
        for i, deal in enumerate(sorted_deals):
            dep = _fmt_dt(deal.outbound_departure)
            ret = _fmt_dt(deal.return_departure)
            _is_fav = deal.id in st.session_state["fav_flights"]
            _heart = "♥" if _is_fav else ""
            _border_col = "#fff" if _is_fav else "rgba(255,255,255,0.2)"
            _bg = "rgba(255,255,255,0.08)" if _is_fav else "transparent"

            _deal_html = (
                f'<div style="background:{_bg};border:2px solid {_border_col};'
                f'padding:10px 16px;margin-bottom:4px;display:flex;'
                f'align-items:center;justify-content:space-between">'
                f'<div style="font-family:JetBrains Mono,monospace;font-size:0.78rem;'
                f'color:rgba(255,255,255,0.7)">'
                f'<b style="color:#fff;font-size:0.95rem">£{deal.price_gbp:.0f}</b>'
                f' ── {dep} → {ret}'
                f' ── {deal.airline}'
                f' ── {deal.nights}N'
                f' ── {deal.origin}'
                f'</div>'
                f'<span style="color:#fff;font-size:1.2rem">{_heart}</span>'
                f'</div>'
            )
            st.markdown(_deal_html, unsafe_allow_html=True)

            fc1, fc2, fc3 = st.columns([2, 2, 2])
            with fc1:
                _fav_lbl = "♥ Favourited" if _is_fav else "♡ Favourite"
                if st.button(_fav_lbl, key=f"fav_{selected}_{i}", use_container_width=True):
                    if _is_fav:
                        st.session_state["fav_flights"].discard(deal.id)
                    else:
                        st.session_state["fav_flights"].add(deal.id)
                    _save_favourites()
                    st.rerun()
            with fc2:
                link = _fix_deep_link(deal.deep_link)
                if link:
                    st.markdown(
                        f"<a href='{link}' target='_blank' style='display:block;text-align:center;"
                        f"padding:0.45rem;border:2px solid rgba(255,255,255,0.4);color:rgba(255,255,255,0.7);"
                        f"text-decoration:none;font-family:JetBrains Mono,monospace;font-size:0.72rem;"
                        f"letter-spacing:0.08em;text-transform:uppercase'>Book →</a>",
                        unsafe_allow_html=True,
                    )
            with fc3:
                with st.popover("Share", key=f"share_{selected}_{i}", use_container_width=True):
                    st.code(_share_deal_text(deal), language=None)

        conn = get_connection(db_path)
        mark_notified(conn, [d.id for d in dest_deals if not d.notified])
        # conn.close()  # Supabase client doesn't need closing
        shown_ids = {d.id for d in dest_deals}
        for deal in st.session_state["deals"]:
            if deal.id in shown_ids:
                deal.notified = True

    # ── SUMMARY VIEW ─────────────────────────────────────────────────────────

    elif filtered:
        groups = _group_destinations(filtered)
        # Summary page — destination cards

        if sort_opt == "Price":
            sorted_groups = sorted(groups.values(), key=lambda g: g["min_price"])
        elif sort_opt == "Destination":
            sorted_groups = sorted(groups.values(), key=lambda g: g["city"])
        else:
            sorted_groups = sorted(groups.values(), key=lambda g: -g["deal_count"])

        st.markdown(
            f"<span style='font-family:JetBrains Mono,monospace;font-size:0.7rem;"
            f"color:rgba(255,255,255,0.4);letter-spacing:0.1em;text-transform:uppercase'>"
            f"{len(groups)} destinations ———— {len(filtered)} total options</span>",
            unsafe_allow_html=True,
        )

        COLS_PER_ROW = 3
        for row_start in range(0, len(sorted_groups), COLS_PER_ROW):
            row_groups = sorted_groups[row_start:row_start + COLS_PER_ROW]
            cols = st.columns(COLS_PER_ROW)
            for col, g in zip(cols, row_groups):
                with col:
                    city = g["city"]
                    country = g["country"]
                    price = g["min_price"]
                    count = g["deal_count"]
                    airlines = sorted(g["airlines"])
                    origins_list = sorted(g["origins"])
                    example_dep = g["example_dep"]
                    example_nights = g["example_nights"]
                    plural = "s" if count != 1 else ""
                    _date_text = f"<b style='color:rgba(255,255,255,0.7)'>{count} date{plural}</b>"

                    # Visual card container
                    st.markdown(
                        f"<div style='border:2px solid rgba(255,255,255,0.15);background:rgba(0,0,0,0.1);"
                        f"padding:12px 14px 8px;margin-bottom:2px;border-radius:4px'>"
                        f"<div style='display:flex;justify-content:space-between;align-items:baseline'>"
                        f"<b style='color:#fff;font-size:0.95rem;letter-spacing:0.03em'>"
                        f"{city.upper()}</b>"
                        f"<b style='color:#fff;font-size:1.05rem'>£{price:.0f}</b>"
                        f"</div>"
                        f"<div style='font-family:JetBrains Mono,monospace;font-size:0.6rem;"
                        f"color:rgba(255,255,255,0.45);letter-spacing:0.08em;margin-top:4px'>"
                        f"{country.upper()} — {_date_text}"
                        f"</div>"
                        f"</div>",
                        unsafe_allow_html=True,
                    )

                    # Action buttons row
                    _bc1, _bc3 = st.columns([3, 1])
                    with _bc1:
                        if st.button("View deals", key=f"view_{city}", use_container_width=True):
                            st.session_state["selected_dest"] = city
                            st.rerun()
                    with _bc3:
                        _map_query = f"{city} {country}"
                        _map_embed_q = _map_query.replace(' ', '+')
                        with st.popover("🌍", key=f"map_{city}", use_container_width=True):
                            st.markdown(
                                f"<p style='font-family:JetBrains Mono,monospace;font-size:0.8rem;"
                                f"color:#fff;letter-spacing:0.05em;margin-bottom:8px'>"
                                f"<b>🌍 {city}, {country.upper()}</b></p>",
                                unsafe_allow_html=True,
                            )
                            st.markdown(
                                f'<iframe width="380" height="300" style="border:0;border-radius:8px" loading="lazy" '
                                f'referrerpolicy="no-referrer-when-downgrade" '
                                f'src="https://maps.google.com/maps?q={_map_embed_q}&output=embed&z=6">'
                                f'</iframe>',
                                unsafe_allow_html=True,
                            )
                            _map_url = f"https://www.google.com/maps/search/?api=1&query={_map_embed_q}"
                            st.markdown(
                                f"<p style='font-family:JetBrains Mono,monospace;font-size:0.65rem;"
                                f"color:rgba(255,255,255,0.5);margin-top:8px;letter-spacing:0.05em'>"
                                f"✈ {g['dest_code']} · <a href='{_map_url}' target='_blank' "
                                f"style='color:rgba(100,180,255,0.8)'>Open in Google Maps →</a></p>",
                                unsafe_allow_html=True,
                            )

        conn = get_connection(db_path)
        mark_notified(conn, [d.id for d in filtered if not d.notified])
        # conn.close()  # Supabase client doesn't need closing
        shown_ids = {d.id for d in filtered}
        for deal in st.session_state["deals"]:
            if deal.id in shown_ids:
                deal.notified = True

    else:
        if deals:
            st.markdown(
                "<p style='font-family:JetBrains Mono,monospace;color:rgba(255,255,255,0.4);"
                "font-size:0.8rem;padding:2rem 0;letter-spacing:0.05em'>"
                "NO DEALS MATCH CURRENT FILTER</p>",
                unsafe_allow_html=True,
            )
        else:
            st.markdown(
                "<p style='font-family:JetBrains Mono,monospace;color:rgba(255,255,255,0.4);"
                "font-size:0.8rem;padding:2rem 0;letter-spacing:0.05em'>"
                "NO DEALS YET — OPEN [ SEARCH ] AND RUN A SCAN</p>",
                unsafe_allow_html=True,
            )

# ══════════════════════════════════════════════════════════════════════════════
# FAVOURITES TAB
# ══════════════════════════════════════════════════════════════════════════════

with tab_favs:
    fav_ids = st.session_state.get("fav_flights", set())

    if not fav_ids:
        st.markdown(
            "<p style='font-family:JetBrains Mono,monospace;color:rgba(255,255,255,0.4);"
            "font-size:0.8rem;padding:2rem 0;letter-spacing:0.05em'>"
            "NO FAVOURITES YET — CLICK ♡ ON A FLIGHT TO SAVE IT HERE</p>",
            unsafe_allow_html=True,
        )
    else:
        if fav_ids:
            st.markdown(
                "<p style='font-family:JetBrains Mono,monospace;color:rgba(255,255,255,0.5);"
                "font-size:0.68rem;font-weight:700;text-transform:uppercase;letter-spacing:0.15em;"
                "margin-bottom:8px'>♥ Favourited Flights</p>",
                unsafe_allow_html=True,
            )
            fav_deals = [d for d in deals if d.id in fav_ids]
            if fav_deals:
                fav_deals.sort(key=lambda d: d.price_gbp)

                for i, deal in enumerate(fav_deals):
                    dep = _fmt_dt(deal.outbound_departure)
                    ret = _fmt_dt(deal.return_departure)
                    city = deal.destination_city or deal.destination
                    country = deal.destination_country or ""

                    st.markdown(
                        f"""<div style="background:rgba(0,0,0,0.15);border:2px solid rgba(255,255,255,0.25);
                            padding:12px 16px;margin-bottom:4px">
                            <div style="display:flex;justify-content:space-between;align-items:baseline">
                                <div style="font-family:JetBrains Mono,monospace;font-size:0.78rem;
                                    color:rgba(255,255,255,0.7)">
                                    <b style="color:#fff;font-size:0.95rem">£{deal.price_gbp:.0f}</b>
                                    &nbsp;──&nbsp; <b style="color:#fff">{city.upper()}</b>
                                    <span style="color:rgba(255,255,255,0.4);font-size:0.68rem">{country.upper()}</span>
                                    &nbsp;──&nbsp; {dep} → {ret}
                                    &nbsp;──&nbsp; {deal.airline}
                                    &nbsp;──&nbsp; {deal.nights}N
                                    &nbsp;──&nbsp; {deal.origin}
                                </div>
                                <span style="color:#fff;font-size:1rem">♥</span>
                            </div>
                        </div>""",
                        unsafe_allow_html=True,
                    )

                    fc1, fc2, fc3 = st.columns([2, 2, 2])
                    with fc1:
                        if st.button("Remove ♥", key=f"unfav_{i}", use_container_width=True):
                            st.session_state["fav_flights"].discard(deal.id)
                            _save_favourites()
                            st.rerun()
                    with fc2:
                        link = _fix_deep_link(deal.deep_link)
                        if link:
                            st.markdown(
                                f"<a href='{link}' target='_blank' style='display:block;text-align:center;"
                                f"padding:0.45rem;border:2px solid rgba(255,255,255,0.4);color:rgba(255,255,255,0.7);"
                                f"text-decoration:none;font-family:JetBrains Mono,monospace;font-size:0.72rem;"
                                f"letter-spacing:0.08em;text-transform:uppercase'>Book →</a>",
                                unsafe_allow_html=True,
                            )
                    with fc3:
                        with st.popover("Share", key=f"fav_share_{i}", use_container_width=True):
                            st.code(_share_deal_text(deal), language=None)
            else:
                st.markdown(
                    "<p style='font-family:JetBrains Mono,monospace;color:rgba(255,255,255,0.3);"
                    "font-size:0.75rem'>FAVOURITED FLIGHTS NOT IN CURRENT DATA</p>",
                    unsafe_allow_html=True,
                )

# ── Footer ───────────────────────────────────────────────────────────────────

st.markdown('<div class="dot-separator"></div>', unsafe_allow_html=True)
st.markdown(
    "<p style='font-family:JetBrains Mono,monospace;font-size:0.62rem;"
    "color:rgba(255,255,255,0.3);letter-spacing:0.08em;text-transform:uppercase;"
    "text-align:center;padding:8px 0'>"
    "Return prices per person (indicative) ———— Source: Google Flights via SerpAPI "
    "———— Links open Skyscanner</p>",
    unsafe_allow_html=True,
)
