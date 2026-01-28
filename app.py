import hashlib
import json
from io import BytesIO
import numpy as np

import streamlit as st
from openpyxl import load_workbook, Workbook

from core_literatur_recherche9 import run_search_to_df, write_to_excel_bytes


# ============================================================
# WICHTIG (Deployment-Hinweis):
# Um das Streamlit-Menü inkl. "View source" zuverlässig zu entfernen,
# lege zusätzlich in deinem Repo diese Datei an:
#
#   .streamlit/config.toml
#
# mit folgendem Inhalt:
#   [client]
#   toolbarMode = "minimal"
#
# (Dann zeigt Streamlit nur noch extern/über set_page_config definierte
# Menüeinträge – und wenn keine da sind, wird das Menü ausgeblendet.)
# ============================================================


# ----------------------------
# Page setup
# ----------------------------
st.set_page_config(
    page_title="LiteraturrechercheTool",
    layout="wide",
    # In Kombination mit client.toolbarMode="minimal" sorgt ein leeres menu_items
    # dafür, dass kein Menü (und damit auch kein "View source") angezeigt wird.
    menu_items={}
)

# UI-Hardening (nicht 100% gegen DevTools, aber entfernt die "bequemen" Wege)
st.markdown(
    """
    <style>
      /* Streamlit "Chrome" ausblenden (Toolbar/Header/Footer) */
      div[data-testid="stToolbar"] {visibility: hidden !important; height: 0px !important; position: fixed !important;}
      div[data-testid="stHeader"] {visibility: hidden !important; height: 0px !important; position: fixed !important;}
      #MainMenu {visibility: hidden !important;}
      footer {visibility: hidden !important;}

      /* Copy-to-clipboard Buttons in Code-Blöcken ausblenden (falls vorhanden) */
      button[data-testid="stCopyToClipboardButton"] {display: none !important;}

      /* Optional: obere Deko-Leiste (variiert je nach Streamlit-Version) */
      div[data-testid="stDecoration"] {visibility: hidden !important; height: 0px !important;}
    </style>
    """,
    unsafe_allow_html=True,
)

st.title("LiteraturrechercheTool")


# ----------------------------
# Session state init
# ----------------------------
if "s2_cache" not in st.session_state:
    st.session_state["s2_cache"] = {}

if "last_config_hash" not in st.session_state:
    st.session_state["last_config_hash"] = None

if "last_df" not in st.session_state:
    st.session_state["last_df"] = None

if "last_sheet" not in st.session_state:
    st.session_state["last_sheet"] = None

if "last_template_bytes" not in st.session_state:
    st.session_state["last_template_bytes"] = None


# ----------------------------
# Option 3: Template mit Header erzeugen (Download)
# ----------------------------
DEFAULT_TEMPLATE_HEADERS = [
    "Nr",
    "Titel",
    "Autor(en)",
    "Erscheinungsjahr",
    "DOI / URL",
    "Publikationsart",
    "Keywords",
    "Abstract",
    "Journal / Verlag",
    "API Quelle",
    "OA URL",
]


def make_template_bytes(sheet_name: str = "Final_Thema2", header_row: int = 1) -> bytes:
    """Erstellt ein neues Excel-Template mit einem Sheet und Header-Zeile."""
    wb = Workbook()
    ws = wb.active
    ws.title = sheet_name

    for _ in range(1, int(header_row)):
        ws.append([])

    ws.append(DEFAULT_TEMPLATE_HEADERS)

    widths = {
        "A": 6,   # Nr
        "B": 45,  # Titel
        "C": 28,  # Autor(en)
        "D": 14,  # Jahr
        "E": 28,  # DOI/URL
        "F": 16,  # Publikationsart
        "G": 30,  # Keywords
        "H": 45,  # Abstract
        "I": 26,  # Journal/Verlag
        "J": 18,  # API Quelle
        "K": 30,  # OA URL
    }
    for col, w in widths.items():
        ws.column_dimensions[col].width = w

    bio = BytesIO()
    wb.save(bio)
    return bio.getvalue()


st.subheader("0) Optional: Leeres Template herunterladen (empfohlen)")
with st.expander("📥 Excel-Template mit korrekten Headern erstellen", expanded=False):
    st.write(
        "Wenn dein eigenes Template keine Spalte **„Titel“** hat oder die Header in einer anderen Zeile stehen, "
        "kannst du hier ein frisches Template herunterladen, das garantiert kompatibel ist."
    )
    t1, t2, t3 = st.columns([2, 1, 1])
    with t1:
        template_sheet_name = st.text_input("Sheet-Name im Template", value="Final_Thema2")
    with t2:
        template_header_row = st.number_input("Header-Zeile im Template", min_value=1, max_value=50, value=1, step=1)
    with t3:
        st.write("")
    st.download_button(
        "Template herunterladen (.xlsx)",
        data=make_template_bytes(sheet_name=template_sheet_name, header_row=int(template_header_row)),
        file_name="literatur_template.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )


# ----------------------------
# Defaults (Queries) – Expertenmodus
# ----------------------------
DEFAULT_CROSSREF = [
    "Teilkostenrechnung Projektcontrolling",
    "Deckungsbeitrag Projektcontrolling",
    "Projektkostenrechnung Teilkostenrechnung",
    "Projektkostenrechnung Deckungsbeitrag",
    "Projektcontrolling Earned Value",
    "Projektcontrolling Budgetierung",
    "Projektkostenrechnung Kalkulation",
    "Filmproduktion Projektcontrolling",
    "Filmproduktion Kostenmanagement",
    "Creative Industries project controlling",
    "project-based firm management accounting",
    "Projektcontrolling Kontingenzansatz",
    "Branchenvergleich Projektcontrolling",
]

DEFAULT_OPENLIB = [
    "Projektcontrolling Kostenrechnung",
    "Projektkostenrechnung Kostenmanagement",
    "Teilkostenrechnung Deckungsbeitrag",
    "Kosten- und Leistungsrechnung Projekt",
    "Projektmanagement Kosten Controlling",
    "Projektcontrolling Earned Value",
    "Kostenmanagement im Projekt",
    "Projektgeschäft Controlling",
    "Filmproduktion Kostenmanagement",
    "Filmproduktion Projektcontrolling",
    "Medienwirtschaft Controlling Kosten",
    "Creative industries project controlling",
    "Projektcontrolling Deutschland",
    "Projektcontrolling Österreich",
]

DEFAULT_OPENALEX = [
    "Projektcontrolling Kostenrechnung",
    "Teilkostenrechnung Projektcontrolling",
    "Deckungsbeitrag Projektcontrolling",
    "Kostenmanagement Projektcontrolling",
    "Projektkostenrechnung",
    "earned value project controlling",
    "project-based firm management accounting",
    "creative industries project controlling",
    "film production project controlling",
]

DEFAULT_DNB = [
    "Projektcontrolling Kostenrechnung",
    "Projektkostenrechnung",
    "Kostenmanagement Projekt",
    "Kosten- und Leistungsrechnung",
    "Teilkostenrechnung",
    "Deckungsbeitrag",
    "Projektgeschäft Controlling",
    "Filmproduktion Controlling",
    "Medienwirtschaft Controlling",
]


def _dedupe_lines(text: str) -> list[str]:
    """1 Query pro Zeile, trim + dedupe (case/whitespace-insensitive)."""
    lines = [l.strip() for l in (text or "").splitlines() if l.strip()]
    seen = set()
    out = []
    for q in lines:
        k = " ".join(q.lower().split())
        if k not in seen:
            seen.add(k)
            out.append(q)
    return out


def _split_csv(s: str) -> list[str]:
    return [x.strip() for x in (s or "").split(",") if x.strip()]


def _ensure_state():
    # Query textareas (Expert mode)
    if "q_text_crossref" not in st.session_state:
        st.session_state["q_text_crossref"] = "\n".join(DEFAULT_CROSSREF)
    if "q_text_openlib" not in st.session_state:
        st.session_state["q_text_openlib"] = "\n".join(DEFAULT_OPENLIB)
    if "q_text_openalex" not in st.session_state:
        st.session_state["q_text_openalex"] = "\n".join(DEFAULT_OPENALEX)
    if "q_text_dnb" not in st.session_state:
        st.session_state["q_text_dnb"] = "\n".join(DEFAULT_DNB)

    # Generic search controls
    if "user_query" not in st.session_state:
        st.session_state["user_query"] = ""
    if "mode_label" not in st.session_state:
        st.session_state["mode_label"] = "Normal"
    if "must_text" not in st.session_state:
        st.session_state["must_text"] = ""
    if "should_text" not in st.session_state:
        st.session_state["should_text"] = ""
    if "exclude_text" not in st.session_state:
        st.session_state["exclude_text"] = "medizin, fertility, cancer, genome"  # hilfreicher Start

    # Facets
    if "year_to" not in st.session_state:
        st.session_state["year_to"] = 2100
    if "oa_only" not in st.session_state:
        st.session_state["oa_only"] = False
    if "sources_filter" not in st.session_state:
        st.session_state["sources_filter"] = []
    if "pub_types_filter" not in st.session_state:
        st.session_state["pub_types_filter"] = []
    if "publisher_contains" not in st.session_state:
        st.session_state["publisher_contains"] = ""


def _reset_queries_to_defaults():
    st.session_state["q_text_crossref"] = "\n".join(DEFAULT_CROSSREF)
    st.session_state["q_text_openlib"] = "\n".join(DEFAULT_OPENLIB)
    st.session_state["q_text_openalex"] = "\n".join(DEFAULT_OPENALEX)
    st.session_state["q_text_dnb"] = "\n".join(DEFAULT_DNB)


_ensure_state()


# ----------------------------
# UI: Upload
# ----------------------------
st.subheader("1) Excel Template hochladen")
uploaded = st.file_uploader("Excel (.xlsx)", type=["xlsx"])

if not uploaded:
    st.info("Bitte eine Excel-Datei hochladen (oder oben ein Template herunterladen).")
    st.stop()

template_bytes = uploaded.getvalue()

try:
    wb = load_workbook(BytesIO(template_bytes), read_only=False, data_only=False)
except Exception as e:
    st.error(f"Excel konnte nicht gelesen werden: {e}")
    st.stop()

sheet_name = st.selectbox("Sheet auswählen", wb.sheetnames, index=0)
header_row = st.number_input("Header-Zeile (meist 1)", min_value=1, max_value=50, value=1, step=1)

with st.expander("🔎 Debug: Header in der gewählten Header-Zeile anzeigen", expanded=False):
    ws = wb[sheet_name]
    row = int(header_row)
    headers = [ws.cell(row=row, column=c).value for c in range(1, ws.max_column + 1)]
    st.write(headers)
    st.info("Wenn 'Titel' nicht exakt vorkommt, nutze das Template oben oder ändere Header-Zeile/Sheet.")


# ----------------------------
# UI: Einstellungen in einem Form (damit Run nur beim Submit)
# ----------------------------
st.subheader("2) Suche & Parameter (Generisch + Präzise Filter)")

with st.form("settings_form", clear_on_submit=False):

    # --- Generic search controls ---
    st.markdown("### A) Suchsteuerung (empfohlen)")
    g1, g2 = st.columns([2, 1])
    with g1:
        user_query = st.text_input(
            "Hauptsuchbegriff (z.B. Steuerberatung, ESG Reporting, Supply Chain ...)",
            value=st.session_state["user_query"],
            placeholder="z.B. Steuerberatung",
        )
    with g2:
        mode_label = st.selectbox(
            "Suchmodus",
            ["Offen", "Normal", "Streng"],
            index=["Offen", "Normal", "Streng"].index(st.session_state["mode_label"]),
            help="Offen: mehr Treffer, Normal: ausgewogen, Streng: Fokusfilter + härtere Schwellen",
        )

    f1, f2, f3 = st.columns(3)
    with f1:
        must_text = st.text_input(
            "MUSS enthalten (Komma-separiert, optional)",
            value=st.session_state["must_text"],
            placeholder="z.B. steuerberatung, tax advisory",
        )
    with f2:
        should_text = st.text_input(
            "SOLL enthalten (Komma-separiert, optional)",
            value=st.session_state["should_text"],
            placeholder="z.B. digitalisierung, compliance",
        )
    with f3:
