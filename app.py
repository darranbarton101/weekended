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

st.set_page_config(page_title="Weekended", page_icon="✈", layout="wide")

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


# ── CSS — Windows 95 aesthetic ───────────────────────────────────────────────
# Classic gray, raised/sunken 3D borders, navy title bars, pixel-perfect retro

_BG = "#c0c0c0"           # Classic Win95 gray
_BG_DARK = "#808080"      # Darker gray / shadow
_BG_LIGHT = "#dfdfdf"     # Light gray / highlight
_WHITE = "#ffffff"        # Window white
_NAVY = "#000080"         # Title bar blue
_BLACK = "#000000"        # Text
_SELECTED = "#0000aa"     # Selected item highlight
_SELECTED_TEXT = "#ffffff"

# Win95 3D border helpers (raised / sunken)
_RAISED = "box-shadow: inset -1px -1px #0a0a0a, inset 1px 1px #ffffff, inset -2px -2px #808080, inset 2px 2px #dfdfdf;"
_SUNKEN = "box-shadow: inset 1px 1px #0a0a0a, inset -1px -1px #ffffff, inset 2px 2px #808080, inset -2px -2px #dfdfdf;"

st.markdown(f"""
<style>
    @import url('https://fonts.googleapis.com/css2?family=VT323&display=swap');

    /* ── Base ── */
    .stApp, .stApp > header {{
        background-color: {_BG} !important;
    }}
    .stApp {{
        color: {_BLACK};
        font-family: 'MS Sans Serif', 'Tahoma', Arial, sans-serif;
        font-size: 11px;
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
        color: {_WHITE} !important;
        font-family: 'VT323', 'MS Sans Serif', Arial, sans-serif !important;
        font-size: 3.2rem !important;
        font-weight: 400 !important;
        letter-spacing: 0.05em !important;
        line-height: 1 !important;
        margin-bottom: 0 !important;
        background: linear-gradient(to right, {_NAVY} 0%, #1084d0 100%);
        padding: 6px 14px !important;
        display: inline-block;
        text-shadow: 1px 1px 0px rgba(0,0,0,0.4);
    }}
    h2 {{
        color: {_WHITE} !important;
        font-family: 'MS Sans Serif', Arial, sans-serif !important;
        font-weight: 700 !important;
        font-size: 1rem !important;
        background: linear-gradient(to right, {_NAVY} 0%, #1084d0 100%);
        padding: 4px 10px !important;
        margin-bottom: 4px !important;
        letter-spacing: 0;
    }}
    h3 {{
        color: {_BLACK} !important;
        font-family: 'MS Sans Serif', Arial, sans-serif !important;
        font-weight: 700 !important;
        font-size: 0.85rem !important;
        letter-spacing: 0;
    }}

    /* Labels & text */
    label, .stCaption, [data-testid="stWidgetLabel"] p,
    [data-testid="stMarkdownContainer"] p {{
        color: {_BLACK} !important;
        font-family: 'MS Sans Serif', 'Tahoma', Arial, sans-serif !important;
        font-size: 0.78rem !important;
    }}

    /* ── Slider ── */
    [data-testid="stSlider"] [data-testid="stThumbValue"],
    [data-testid="stSlider"] [data-testid="stTickBarMin"],
    [data-testid="stSlider"] [data-testid="stTickBarMax"] {{
        color: {_BLACK} !important;
        font-family: 'MS Sans Serif', Arial, sans-serif !important;
    }}
    .stSlider > div > div > div > div {{
        background-color: {_NAVY} !important;
    }}
    [data-testid="stSlider"] > div > div {{
        background-color: {_BG_DARK} !important;
        {_SUNKEN}
    }}

    /* ── Radio ── */
    .stRadio label span {{ color: {_BLACK} !important; font-family: 'MS Sans Serif', Arial, sans-serif !important; font-size: 0.78rem !important; }}
    .stRadio label[data-checked="true"] span {{ color: {_BLACK} !important; font-weight: 700; }}

    /* ── Checkbox ── */
    .stCheckbox label span {{ color: {_BLACK} !important; }}

    /* ── Pills (day picker) ── */
    [data-testid="stPills"] button {{
        background-color: {_BG} !important;
        color: {_BLACK} !important;
        border: none !important;
        border-radius: 0px !important;
        font-family: 'MS Sans Serif', Arial, sans-serif !important;
        font-size: 0.75rem !important;
        font-weight: 400 !important;
        {_RAISED}
        padding: 3px 10px !important;
    }}
    [data-testid="stPills"] button[aria-checked="true"],
    [data-testid="stPills"] button[data-selected="true"] {{
        background-color: {_NAVY} !important;
        color: {_WHITE} !important;
        {_SUNKEN}
    }}
    [data-testid="stPills"] button:hover {{
        background-color: {_BG_LIGHT} !important;
        color: {_BLACK} !important;
    }}

    /* ── Buttons ── */
    .stButton > button[kind="primary"] {{
        background-color: {_BG};
        color: {_BLACK};
        border: none;
        border-radius: 0px;
        font-weight: 700;
        font-size: 0.78rem;
        font-family: 'MS Sans Serif', Arial, sans-serif;
        padding: 0.4rem 1.6rem;
        letter-spacing: 0;
        {_RAISED}
        min-width: 80px;
    }}
    .stButton > button[kind="primary"] p {{
        color: {_BLACK} !important;
    }}
    .stButton > button[kind="primary"]:hover {{
        background-color: {_BG_LIGHT} !important;
    }}
    .stButton > button[kind="primary"]:active {{
        {_SUNKEN}
        padding-left: 1.7rem !important;
        padding-top: 0.45rem !important;
    }}
    .stButton > button[kind="primary"]:hover p,
    .stButton > button[kind="primary"]:focus p,
    .stButton > button[kind="primary"]:active p {{
        color: {_BLACK} !important;
    }}
    .stButton > button:not([kind="primary"]) {{
        background-color: {_BG} !important;
        color: {_BLACK} !important;
        border: none !important;
        border-radius: 0px !important;
        font-size: 0.78rem;
        font-family: 'MS Sans Serif', Arial, sans-serif;
        {_RAISED}
        min-width: 60px;
    }}
    .stButton > button:not([kind="primary"]) p {{
        color: {_BLACK} !important;
    }}
    .stButton > button:not([kind="primary"]):hover {{
        background-color: {_BG_LIGHT} !important;
    }}
    .stButton > button:not([kind="primary"]):active {{
        {_SUNKEN}
    }}
    .stButton > button:not([kind="primary"]):hover p,
    .stButton > button:not([kind="primary"]):focus p {{
        color: {_BLACK} !important;
    }}

    /* ── Selectbox / multiselect ── */
    .stSelectbox > div > div,
    .stMultiSelect > div > div {{
        background-color: {_WHITE} !important;
        border: 2px solid !important;
        border-color: {_BG_DARK} {_WHITE} {_WHITE} {_BG_DARK} !important;
        border-radius: 0px !important;
        color: {_BLACK} !important;
        font-family: 'MS Sans Serif', Arial, sans-serif !important;
        font-size: 0.78rem !important;
    }}
    .stMultiSelect span[data-baseweb="tag"] {{
        background-color: {_NAVY} !important;
        color: {_WHITE} !important;
        border-radius: 0px;
        font-weight: 400;
        font-family: 'MS Sans Serif', Arial, sans-serif;
        font-size: 0.72rem;
    }}

    /* ── Progress bar ── */
    .stProgress > div > div {{ background-color: {_NAVY} !important; }}
    .stProgress {{ background-color: {_WHITE}; border-radius: 0px; {_SUNKEN} }}

    /* ── Expander ── */
    [data-testid="stExpander"] {{
        border: none !important;
        border-radius: 0px !important;
    }}
    [data-testid="stExpander"] > details {{
        border: none !important;
        border-radius: 0px !important;
        background: {_BG} !important;
        {_RAISED}
    }}
    [data-testid="stExpander"] > details > summary {{
        background: linear-gradient(to right, {_NAVY} 0%, #1084d0 100%) !important;
        color: {_WHITE} !important;
        border: none !important;
        border-radius: 0px !important;
        font-family: 'MS Sans Serif', Arial, sans-serif !important;
        font-weight: 700 !important;
        font-size: 0.78rem !important;
        padding: 4px 8px !important;
    }}
    [data-testid="stExpander"] > details > summary:hover {{
        background: linear-gradient(to right, #1084d0 0%, {_NAVY} 100%) !important;
        color: {_WHITE} !important;
    }}
    [data-testid="stExpander"] > details > div {{
        background-color: {_BG} !important;
        border: none !important;
        border-radius: 0px !important;
        padding: 8px !important;
    }}
    details > summary {{
        background: linear-gradient(to right, {_NAVY} 0%, #1084d0 100%) !important;
        color: {_WHITE} !important;
        border: none !important;
        border-radius: 0px !important;
        padding: 4px 8px !important;
    }}
    details > summary:hover {{
        color: {_WHITE} !important;
    }}

    /* ── Info / alerts ── */
    .stAlert {{ border-radius: 0px; {_RAISED} }}

    /* ── Tabs ── */
    .stTabs [data-baseweb="tab-list"] {{
        gap: 4px;
        border-bottom: 2px solid {_BG_DARK};
        background-color: {_BG};
        padding-top: 4px;
    }}
    .stTabs [data-baseweb="tab"] {{
        color: {_BLACK} !important;
        font-family: 'MS Sans Serif', Arial, sans-serif;
        font-size: 0.78rem;
        font-weight: 400;
        padding: 4px 14px;
        border-radius: 0;
        background-color: {_BG};
        {_RAISED}
        border-bottom: none !important;
        position: relative;
        bottom: -2px;
    }}
    .stTabs [aria-selected="true"] {{
        color: {_BLACK} !important;
        background-color: {_BG} !important;
        font-weight: 700 !important;
        border-bottom: 2px solid {_BG} !important;
        z-index: 1;
    }}

    /* ── Divider ── */
    hr {{
        border: none !important;
        border-top: 1px solid {_BG_DARK} !important;
        border-bottom: 1px solid {_WHITE} !important;
    }}

    /* ── Caption ── */
    .stCaption {{ color: {_BG_DARK} !important; font-family: 'MS Sans Serif', Arial, sans-serif !important; }}

    /* ── Links ── */
    a {{ color: {_NAVY} !important; }}
    a:hover {{ color: {_SELECTED} !important; text-decoration: underline; }}

    /* ── Header bar / title bar ── */
    .header-bar {{
        display: flex;
        align-items: center;
        justify-content: space-between;
        padding: 4px 8px;
        background: linear-gradient(to right, {_NAVY} 0%, #1084d0 100%);
        margin-bottom: 10px;
        {_RAISED}
    }}
    .header-bar span {{
        color: {_WHITE};
        font-family: 'MS Sans Serif', Arial, sans-serif;
        font-size: 0.72rem;
        font-weight: 700;
        letter-spacing: 0;
    }}

    /* ── Popover button ── */
    [data-testid="stPopoverButton"] > button {{
        background-color: {_BG} !important;
        color: {_BLACK} !important;
        border: none !important;
        border-radius: 0px !important;
        font-family: 'MS Sans Serif', Arial, sans-serif !important;
        {_RAISED}
    }}
    [data-testid="stPopoverButton"] > button p {{
        color: {_BLACK} !important;
    }}
    [data-testid="stPopoverButton"] > button:hover {{
        background-color: {_BG_LIGHT} !important;
    }}
    [data-testid="stPopoverButton"] > button:hover p {{
        color: {_BLACK} !important;
    }}

    /* ── Secondary buttons ── */
    button[kind="secondary"] {{
        background-color: {_BG} !important;
        color: {_BLACK} !important;
        border: none !important;
        border-radius: 0px !important;
        {_RAISED}
    }}
    button[kind="secondary"] p {{
        color: {_BLACK} !important;
    }}
    button[kind="secondary"]:hover {{
        background-color: {_BG_LIGHT} !important;
    }}
    button[kind="secondary"]:hover p {{
        color: {_BLACK} !important;
    }}

    /* ── Scanning animation ── */
    @keyframes scanning-dots {{
        0% {{ content: '.'; }}
        33% {{ content: '..'; }}
        66% {{ content: '...'; }}
    }}
    .scan-status {{
        font-family: 'MS Sans Serif', Arial, sans-serif;
        font-size: 0.82rem;
        color: {_BLACK};
        font-weight: 700;
        background-color: {_BG};
        border: 2px inset {_BG_DARK};
        padding: 4px 10px;
        display: inline-block;
    }}
    .scan-status::after {{
        content: '...';
        animation: scanning-dots 1.5s infinite;
    }}

    /* Win95 window-style separator */
    .dot-separator {{
        border-top: 1px solid {_BG_DARK};
        border-bottom: 1px solid {_WHITE};
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

from travel_scanner.airports import AIRPORT_OPTIONS

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
    _col = "#000080" if enabled else "#808080"
    _wt = "600" if enabled else "400"
    c2.markdown(
        f"<p style='margin:0;padding:7px 0 0;font-size:0.78rem;"
        f"font-weight:{_wt};color:{_col};font-family:Arial, sans-serif;"
        f"letter-spacing:0.08em;text-transform:uppercase'>"
        f"{DAY_NAMES[wd][:3]}</p>",
        unsafe_allow_html=True,
    )
    if enabled:
        return True, ("00:00", "23:59")
    else:
        return False, ("00:00", "23:59")


# ── Header ───────────────────────────────────────────────────────────────────

_hdr_left, _hdr_right = st.columns([6, 1])
with _hdr_left:
    # Massive impactful logo — pure HTML, no button constraints
    st.markdown(
        "<div style='background:linear-gradient(to right,#000080 0%,#1084d0 100%);"
        "padding:6px 14px;margin-bottom:6px;display:inline-flex;align-items:center;gap:12px;"
        "box-shadow:inset -1px -1px #0a0a0a,inset 1px 1px #ffffff,inset -2px -2px #808080,inset 2px 2px #dfdfdf'>"
        "<span style='font-family:VT323,Arial,sans-serif;font-size:3.2rem;font-weight:400;"
        "color:#ffffff;letter-spacing:0.04em;line-height:1'>WEEKENDED</span>"
        "<span style='font-family:Arial,sans-serif;font-size:0.72rem;color:#c0c0c0;"
        "font-weight:400'>Find cheap weekend flights</span>"
        "</div>",
        unsafe_allow_html=True,
    )
with _hdr_right:
    if st.session_state.get("selected_dest"):
        if st.button("← Back", key="home_btn"):
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
        <span>{_now_str} ———— Weekended ———— V.018</span>
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
        "<div style='background:#c0c0c0;"
        "box-shadow:inset -1px -1px #0a0a0a,inset 1px 1px #ffffff,inset -2px -2px #808080,inset 2px 2px #dfdfdf;"
        "padding:16px 20px 10px;margin-bottom:8px'>",
        unsafe_allow_html=True,
    )

    r1c1, r1c2, r1c3, r1c4 = st.columns([3, 1.5, 1.5, 1.5])

    with r1c1:
        st.caption("AIRPORTS (UP TO 3)")
        # Initialise widget state from saved prefs only on first run,
        # then let the widget own its own value to avoid stale defaults
        # overriding user selections on rerun.
        if "_ms_airports" not in st.session_state:
            _pref_airports = _saved_prefs.get("airports", ["GLA", "EDI"])
            # Filter out any codes that no longer exist in the options list
            st.session_state["_ms_airports"] = [
                c for c in _pref_airports if c in AIRPORT_OPTIONS
            ] or ["GLA", "EDI"]
        selected_airports = st.multiselect(
            "airports",
            options=list(AIRPORT_OPTIONS.keys()),
            key="_ms_airports",
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

    # Day pickers — pill buttons
    _day_options = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
    _day_to_idx = {name: i for i, name in enumerate(_day_options)}

    r2c1, r2c2 = st.columns(2)

    with r2c1:
        _pref_dep = _saved_prefs.get("dep_days", {})
        if _pref_dep:
            _dep_default = [_day_options[int(k)] for k in _pref_dep.keys() if int(k) < 7]
        else:
            _dep_default = ["Thu", "Fri"]
        _dep_selected = st.pills(
            "Depart on", _day_options,
            selection_mode="multi",
            default=_dep_default,
            key="dep_pills",
        )
        departure_days: dict[int, tuple[str, str]] = {}
        for name in (_dep_selected or []):
            departure_days[_day_to_idx[name]] = ("00:00", "23:59")
        if not departure_days:
            departure_days[3] = ("00:00", "23:59")

    with r2c2:
        _pref_ret = _saved_prefs.get("ret_days", {})
        if _pref_ret:
            _ret_default = [_day_options[int(k)] for k in _pref_ret.keys() if int(k) < 7]
        else:
            _ret_default = ["Sun"]
        _ret_selected = st.pills(
            "Return on", _day_options,
            selection_mode="multi",
            default=_ret_default,
            key="ret_pills",
        )
        return_days: dict[int, tuple[str, str]] = {}
        for name in (_ret_selected or []):
            return_days[_day_to_idx[name]] = ("00:00", "23:59")
        if not return_days:
            return_days[6] = ("00:00", "23:59")
        st.markdown(
            "<p style='margin:2px 0 0;color:#808080;font-size:0.55rem;"
            "font-family:Arial, sans-serif;letter-spacing:0.06em'>"
            "Tip: add Mon for early morning return flights</p>",
            unsafe_allow_html=True,
        )

    # Row 3
    r3c1, r3c2 = st.columns([3, 1.5])

    serpapi_key = os.environ.get("SERPAPI_KEY", "")

    with r3c1:
        _serp_status = "CONNECTED" if serpapi_key else "KEY NOT SET"
        _status_col = "#000080" if serpapi_key else "#808080"
        st.markdown(
            f"<span style='font-family:Arial, sans-serif;font-size:0.68rem;"
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
    link = _fix_deep_link(d.deep_link)
    return (
        f"✈ {city}, {d.destination_country}\n"
        f"£{d.price_gbp:.0f} return · {d.airline}\n"
        f"{dep} → {ret} · {d.nights} nights\n"
        f"\n{link}\n"
        f"\nFound on Weekended"
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
            f'<div style="background:#c0c0c0;'
            f'box-shadow:inset -1px -1px #0a0a0a,inset 1px 1px #ffffff,inset -2px -2px #808080,inset 2px 2px #dfdfdf;'
            f'padding:6px 10px;min-width:140px;flex:1;opacity:0.75">'
            f'<div style="background:linear-gradient(to right,#000080,#1084d0);padding:2px 6px;margin:-6px -10px 5px">'
            f'<span style="font-weight:700;color:#ffffff;font-family:Arial,sans-serif;'
            f'font-size:0.68rem">{g["city"].upper()}</span>'
            f'</div>'
            f'<div style="display:flex;justify-content:space-between;align-items:baseline;padding-top:2px">'
            f'<span style="color:#808080;font-family:Arial,sans-serif;font-size:0.6rem">{g["country"]}</span>'
            f'<b style="color:#000080;font-family:Arial,sans-serif;font-size:0.85rem">£{g["min_price"]:.0f}</b>'
            f'</div>'
            f'<div style="color:#808080;font-family:Arial,sans-serif;'
            f'font-size:0.58rem;margin-top:2px">'
            f'{g["count"]} date{plural(g["count"])}</div>'
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
    # Clean slate for new search
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
            f"<p style='font-family:Arial, sans-serif;font-size:0.78rem;"
            f"color:#000000;letter-spacing:0.06em;margin:8px 0 12px'>"
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
                    f"<p style='font-family:Arial, sans-serif;font-size:0.72rem;"
                    f"color:#000000;letter-spacing:0.05em;margin:2px 0'>"
                    f"[{pct}%] {_friendly_msg}</p>",
                    unsafe_allow_html=True,
                )
                step_log.append(msg)
                if _live_deals:
                    n = len(_live_deals)
                    dests = len({d.destination_city or d.destination for d in _live_deals})
                    live_count.markdown(
                        f"<p style='font-family:Arial, sans-serif;font-size:0.75rem;"
                        f"color:#000080;letter-spacing:0.08em;margin:4px 0'>"
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

_fav_count = len(st.session_state.get("fav_flights", set()))
_fav_label = f"Favourites ({_fav_count})" if _fav_count else "Favourites"
tab_all, tab_favs = st.tabs(["All Destinations", _fav_label])

# ══════════════════════════════════════════════════════════════════════════════
# ALL DESTINATIONS TAB
# ══════════════════════════════════════════════════════════════════════════════

with tab_all:
    tc1, tc2 = st.columns([4, 1.5])

    with tc1:
        if deals:
            _n_dests = len({d.destination_city or d.destination for d in deals})
            st.markdown(
                f"<span style='font-family:Arial, sans-serif;font-size:0.75rem;"
                f"color:#000000;letter-spacing:0.05em'>"
                f"<b style='color:#000080;font-size:1.1rem'>{len(deals)}</b> DEALS "
                f"———— {_n_dests} DESTINATIONS</span>",
                unsafe_allow_html=True,
            )
        else:
            st.markdown(
                "<span style='font-family:Arial, sans-serif;font-size:0.75rem;"
                "color:#808080'>NO DEALS — RUN A SEARCH</span>",
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
            f"<span style='font-size:2rem;font-weight:900;color:#000080;text-transform:uppercase;"
            f"letter-spacing:-0.01em'>{selected}</span>"
            + (f"<span style='color:#808080;font-family:Arial, sans-serif;"
               f"font-size:0.8rem;margin-left:14px'>{country}</span>" if country else "")
            + f"</div>"
            f"<div style='font-family:Arial, sans-serif;font-size:0.85rem;"
            f"color:#000000;margin-bottom:12px'>"
            f"FROM £{cheapest.price_gbp:.0f} ———— {len(dest_deals)} OPTIONS ———— {airlines_str.upper()}"
            f"</div>",
            unsafe_allow_html=True,
        )

        # Map locator
        _map_query = f"{selected} {country}"
        _map_embed_q = _map_query.replace(' ', '+')
        with st.popover("🌍 Where is this?", key="map_detail"):
            st.markdown(
                f"<p style='font-family:Arial, sans-serif;font-size:0.8rem;"
                f"color:#000080;letter-spacing:0.05em;margin-bottom:8px'>"
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
                f"<p style='font-family:Arial, sans-serif;font-size:0.65rem;"
                f"color:#808080;margin-top:8px;letter-spacing:0.05em'>"
                f"✈ {cheapest.destination} · <a href='{_map_url}' target='_blank' "
                f"style='color:#000000'>Open in Google Maps →</a></p>",
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
            _border_col = "#000080" if _is_fav else "#808080"
            _bg = "#ffffff" if _is_fav else "transparent"

            _raised = "box-shadow:inset -1px -1px #0a0a0a,inset 1px 1px #ffffff,inset -2px -2px #808080,inset 2px 2px #dfdfdf"
            _deal_html = (
                f'<div style="background:{_bg};{_raised};'
                f'padding:8px 14px;margin-bottom:4px;display:flex;'
                f'align-items:center;justify-content:space-between">'
                f'<div style="font-family:Arial,sans-serif;font-size:0.78rem;'
                f'color:#000000">'
                f'<b style="color:#000080;font-size:0.95rem">£{deal.price_gbp:.0f}</b>'
                f'&nbsp;&nbsp;{dep} → {ret}'
                f'&nbsp;&nbsp;{deal.airline}'
                f'&nbsp;&nbsp;{deal.nights}N'
                f'&nbsp;&nbsp;{deal.origin}'
                f'</div>'
                f'<span style="color:#000080;font-size:1.2rem">{_heart}</span>'
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
                        f"padding:0.4rem;background:#c0c0c0;color:#000000;"
                        f"box-shadow:inset -1px -1px #0a0a0a,inset 1px 1px #ffffff,inset -2px -2px #808080,inset 2px 2px #dfdfdf;"
                        f"text-decoration:none;font-family:Arial,sans-serif;font-size:0.75rem;"
                        f"font-weight:700'>Book →</a>",
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
            f"<span style='font-family:Arial, sans-serif;font-size:0.7rem;"
            f"color:#808080;letter-spacing:0.1em;text-transform:uppercase'>"
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
                    _date_text = f"<b style='color:#000000'>{count} date{plural}</b>"

                    # Visual card container
                    st.markdown(
                        f"<div style='background:#ffffff;"
                        f"box-shadow:inset -1px -1px #0a0a0a,inset 1px 1px #ffffff,inset -2px -2px #808080,inset 2px 2px #dfdfdf;"
                        f"padding:8px 12px;margin-bottom:2px'>"
                        f"<div style='background:linear-gradient(to right,#000080 0%,#1084d0 100%);"
                        f"padding:3px 8px;margin:-8px -12px 8px;'>"
                        f"<span style='color:#ffffff;font-family:Arial,sans-serif;font-size:0.75rem;font-weight:700'>"
                        f"{city.upper()}</span>"
                        f"</div>"
                        f"<div style='display:flex;justify-content:space-between;align-items:baseline'>"
                        f"<span style='font-family:Arial,sans-serif;font-size:0.72rem;color:#000000'>"
                        f"{country}</span>"
                        f"<b style='color:#000080;font-size:1.05rem;font-family:Arial,sans-serif'>£{price:.0f}</b>"
                        f"</div>"
                        f"<div style='font-family:Arial,sans-serif;font-size:0.65rem;"
                        f"color:#808080;margin-top:2px'>"
                        f"{_date_text}"
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
                                f"<p style='font-family:Arial, sans-serif;font-size:0.8rem;"
                                f"color:#000080;letter-spacing:0.05em;margin-bottom:8px'>"
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
                                f"<p style='font-family:Arial, sans-serif;font-size:0.65rem;"
                                f"color:#808080;margin-top:8px;letter-spacing:0.05em'>"
                                f"✈ {g['dest_code']} · <a href='{_map_url}' target='_blank' "
                                f"style='color:#000000'>Open in Google Maps →</a></p>",
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
                "<p style='font-family:Arial, sans-serif;color:#808080;"
                "font-size:0.8rem;padding:2rem 0;letter-spacing:0.05em'>"
                "NO DEALS MATCH CURRENT FILTER</p>",
                unsafe_allow_html=True,
            )
        else:
            st.markdown(
                "<div style='padding:3rem 0;text-align:center'>"
                "<p style='font-family:Arial, sans-serif;color:#000000;"
                "font-size:1rem;letter-spacing:0.05em;font-weight:700;margin-bottom:16px'>"
                "READY TO FIND YOUR NEXT WEEKEND AWAY?</p>"
                "<p style='font-family:Arial, sans-serif;color:#808080;"
                "font-size:0.72rem;letter-spacing:0.06em;line-height:2'>"
                "1. Open <b style='color:#000000'>[ SEARCH ]</b> above<br>"
                "2. Pick your departure airport and travel dates<br>"
                "3. Hit <b style='color:#000000'>Search</b> — "
                "we scan Google Flights for the cheapest weekend return flights<br>"
                "4. Browse destinations, then hit <b style='color:#000000'>Book</b> "
                "to see exact flight times and book on Skyscanner</p>"
                "<p style='font-family:Arial, sans-serif;color:#808080;"
                "font-size:0.62rem;letter-spacing:0.08em;margin-top:20px'>"
                "TIP: Add Mon to your return days to catch early morning flights back for work</p>"
                "</div>",
                unsafe_allow_html=True,
            )

# ══════════════════════════════════════════════════════════════════════════════
# FAVOURITES TAB
# ══════════════════════════════════════════════════════════════════════════════

with tab_favs:
    fav_ids = st.session_state.get("fav_flights", set())

    if not fav_ids:
        st.markdown(
            "<p style='font-family:Arial, sans-serif;color:#808080;"
            "font-size:0.8rem;padding:2rem 0;letter-spacing:0.05em'>"
            "NO FAVOURITES YET — CLICK ♡ ON A FLIGHT TO SAVE IT HERE</p>",
            unsafe_allow_html=True,
        )
    else:
        if fav_ids:
            st.markdown(
                "<p style='font-family:Arial, sans-serif;color:#808080;"
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
                        f"""<div style="background:#ffffff;box-shadow:inset -1px -1px #0a0a0a,inset 1px 1px #ffffff,inset -2px -2px #808080,inset 2px 2px #dfdfdf;
                            padding:8px 14px;margin-bottom:4px">
                            <div style="display:flex;justify-content:space-between;align-items:baseline">
                                <div style="font-family:Arial,sans-serif;font-size:0.78rem;
                                    color:#000000">
                                    <b style="color:#000080;font-size:0.95rem">£{deal.price_gbp:.0f}</b>
                                    &nbsp;&nbsp; <b style="color:#000080">{city.upper()}</b>
                                    <span style="color:#808080;font-size:0.68rem">&nbsp;{country.upper()}</span>
                                    &nbsp;&nbsp;{dep} → {ret}
                                    &nbsp;&nbsp;{deal.airline}
                                    &nbsp;&nbsp;{deal.nights}N
                                    &nbsp;&nbsp;{deal.origin}
                                </div>
                                <span style="color:#000080;font-size:1rem">♥</span>
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
                                f"padding:0.45rem;border:1px solid #808080;color:#000000;"
                                f"text-decoration:none;font-family:Arial, sans-serif;font-size:0.72rem;"
                                f"letter-spacing:0.08em;text-transform:uppercase'>Book →</a>",
                                unsafe_allow_html=True,
                            )
                    with fc3:
                        with st.popover("Share", key=f"fav_share_{i}", use_container_width=True):
                            st.code(_share_deal_text(deal), language=None)
            else:
                st.markdown(
                    "<p style='font-family:Arial, sans-serif;color:#808080;"
                    "font-size:0.75rem'>FAVOURITED FLIGHTS NOT IN CURRENT DATA</p>",
                    unsafe_allow_html=True,
                )

# ── Footer ───────────────────────────────────────────────────────────────────

st.markdown('<div class="dot-separator"></div>', unsafe_allow_html=True)
st.markdown(
    "<div style='text-align:center;padding:16px 0 8px'>"
    "<p style='font-family:Arial, sans-serif;font-size:0.62rem;"
    "color:#808080;letter-spacing:0.08em;text-transform:uppercase;"
    "margin:0'>"
    "Prices are indicative returns per person ———— Source: Google Flights "
    "———— Booking links open Skyscanner</p>"
    "<p style='font-family:Arial, sans-serif;font-size:0.55rem;"
    "color:#808080;letter-spacing:0.06em;margin:6px 0 0'>"
    "Prices may change between scanning and booking · Always confirm before you pay</p>"
    "</div>",
    unsafe_allow_html=True,
)
