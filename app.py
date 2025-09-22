# app.py
import streamlit as st
import requests
import pandas as pd
import sqlite3
import json

# -----------------------
# Config / Title
# -----------------------
st.set_page_config(page_title="Harvard’s Artifacts Collection", layout="wide")
st.title("Harvard’s Artifacts Collection")

API_KEY = "07e3776a-04b1-4c47-98c8-5a8dd7f08b51"
BASE_URL = "https://api.harvardartmuseums.org/object"
DB_NAME = "artifacts.db"
DEFAULT_FETCH_LIMIT = 2500

# -----------------------
# Session state defaults
# -----------------------
for key in ("meta_df", "media_df", "colors_df", "collected", "inserted", "show_choice", "migrate_click", "show_queries", "raw_data", "show_tables_after_insert", "combined_meta_df", "combined_media_df", "combined_colors_df", "inserted_classifications"):
    if key not in st.session_state:
        if key.startswith("combined_"):
            st.session_state[key] = pd.DataFrame()
        elif key == "inserted_classifications":
            st.session_state[key] = set()
        else:
            st.session_state[key] = False if key in ("collected", "inserted", "show_choice", "migrate_click", "show_queries", "show_tables_after_insert") else None
        if key == "raw_data":
            st.session_state[key] = {}


# -----------------------
# DB functions
# -----------------------
def get_conn():
    return sqlite3.connect(DB_NAME, check_same_thread=False)

def init_db():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS artifact_metadata (
            id INTEGER PRIMARY KEY,
            title TEXT,
            culture TEXT,
            dated TEXT,
            period TEXT,
            division TEXT,
            medium TEXT,
            dimensions TEXT,
            weight TEXT,
            department TEXT,
            accessionyear INTEGER,
            classification TEXT
        );
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS artifact_media (
            object_id INTEGER PRIMARY KEY,
            imagecount INTEGER,
            mediacount INTEGER,
            colorcount INTEGER,
            rank REAL,
            datedbegin INTEGER,
            datedend INTEGER
        );
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS artifact_colors (
            object_id INTEGER,
            hue TEXT,
            percentage REAL,
            PRIMARY KEY (object_id, hue)
        );
    """)
    conn.commit()
    conn.close()

init_db()

# -----------------------
# Fetch / transform functions
# -----------------------
def fetch_data(classification: str, limit: int = DEFAULT_FETCH_LIMIT):
    """Fetch up to `limit` records for a classification from Harvard API."""
    raw_records = []
    meta_rows, media_rows, color_rows = [], [], []
    page = 1
    collected = 0
    while collected < limit:
        params = {"apikey": API_KEY, "classification": classification, "size": 100, "page": page}
        r = requests.get(BASE_URL, params=params, timeout=60)
        r.raise_for_status()
        payload = r.json()
        records = payload.get("records", [])
        if not records:
            break
        raw_records.extend(records)
        for obj in records:
            meta_rows.append({
                "id": obj.get("id"),
                "title": obj.get("title"),
                "culture": obj.get("culture"),
                "dated": obj.get("dated"),
                "period": obj.get("period"),
                "division": obj.get("division"),
                "medium": obj.get("medium"),
                "dimensions": obj.get("dimensions"),
                "weight": obj.get("weight"),
                "department": obj.get("department"),
                "accessionyear": obj.get("accessionyear"),
                "classification": classification
            })
            media_rows.append({
                "object_id": obj.get("id"),
                "imagecount": obj.get("imagecount"),
                "mediacount": obj.get("mediacount"),
                "colorcount": obj.get("colorcount"),
                "rank": obj.get("rank"),
                "datedbegin": obj.get("datedbegin"),
                "datedend": obj.get("datedend")
            })
            for c in (obj.get("colors") or []):
                color_rows.append({
                    "object_id": obj.get("id"),
                    "hue": c.get("hue"),
                    "percentage": c.get("percent")
                })
        collected += len(records)
        page += 1
        if "info" in payload and page > payload["info"].get("pages", page):
            break

    meta_df = pd.DataFrame(meta_rows).drop_duplicates(subset=["id"], keep="first")
    media_df = pd.DataFrame(media_rows).drop_duplicates(subset=["object_id"], keep="first")
    colors_df = pd.DataFrame(color_rows).dropna(subset=["hue"])
    return meta_df, media_df, colors_df, raw_records

# -----------------------
# Insert (upsert-safe)
# -----------------------
def insert_frames(meta_df: pd.DataFrame, media_df: pd.DataFrame, colors_df: pd.DataFrame):
    conn = get_conn()
    cur = conn.cursor()
    # metadata upsert
    if not meta_df.empty:
        cols = ["id","title","culture","dated","period","division","medium","dimensions","weight","department","accessionyear","classification"]
        placeholders = ",".join(["?"]*len(cols))
        sql = f"INSERT OR REPLACE INTO artifact_metadata ({','.join(cols)}) VALUES ({placeholders})"
        records = []
        for _, row in meta_df.iterrows():
            vals = []
            for c in cols:
                v = row.get(c)
                if c == "id" and pd.notna(v):
                    vals.append(int(v))
                else:
                    vals.append(None if pd.isna(v) else v)
            records.append(tuple(vals))
        cur.executemany(sql, records)
    # media upsert
    if not media_df.empty:
        cols = ["object_id","imagecount","mediacount","colorcount","rank","datedbegin","datedend"]
        placeholders = ",".join(["?"]*len(cols))
        sql = f"INSERT OR REPLACE INTO artifact_media ({','.join(cols)}) VALUES ({placeholders})"
        records = []
        for _, row in media_df.iterrows():
            vals = [None if pd.isna(row.get(c)) else row.get(c) for c in cols]
            records.append(tuple(vals))
        cur.executemany(sql, records)
    # colors upsert (composite PK)
    if not colors_df.empty:
        cols = ["object_id","hue","percentage"]
        placeholders = ",".join(["?"]*len(cols))
        sql = f"INSERT OR REPLACE INTO artifact_colors ({','.join(cols)}) VALUES ({placeholders})"
        records = []
        for _, row in colors_df.iterrows():
            vals = [None if pd.isna(row.get(c)) else row.get(c) for c in cols]
            records.append(tuple(vals))
        cur.executemany(sql, records)
    conn.commit()
    conn.close()

# -----------------------
# Queries (25). #14 handled dynamically in UI.
# -----------------------
QUERIES = {
    "1. List all artifacts from the 11th century belonging to Byzantine culture":
        "SELECT * FROM artifact_metadata WHERE culture='Byzantine' AND dated LIKE '%11th century%';",
    "2. What are the unique cultures represented in the artifacts?":
        "SELECT DISTINCT culture FROM artifact_metadata WHERE culture IS NOT NULL;",
    "3. List all artifacts from the Archaic Period":
        "SELECT * FROM artifact_metadata WHERE period LIKE '%Archaic%';",
    "4. List artifact titles ordered by accession year in descending order":
        "SELECT title, accessionyear FROM artifact_metadata WHERE accessionyear IS NOT NULL ORDER BY accessionyear DESC;",
    "5. How many artifacts are there per department?":
        "SELECT department, COUNT(*) AS total FROM artifact_metadata GROUP BY department ORDER BY total DESC;",
    "6. Which artifacts have more than 1 image?":
        "SELECT m.title, a.imagecount FROM artifact_media a JOIN artifact_metadata m ON m.id = a.object_id WHERE a.imagecount > 1;",
    "7. What is the average rank of all artifacts?":
        "SELECT AVG(rank) AS avg_rank FROM artifact_media WHERE rank IS NOT NULL;",
    "8. Which artifacts have a higher colorcount than mediacount?":
        "SELECT m.title, a.colorcount, a.mediacount FROM artifact_media a JOIN artifact_metadata m ON m.id = a.object_id WHERE COALESCE(a.colorcount,0) > COALESCE(a.mediacount,0);",
    "9. List all artifacts created between 1500 and 1600":
        "SELECT m.title, m.datedbegin, m.datedend FROM artifact_metadata m JOIN artifact_media a ON m.id = a.object_id WHERE m.datedbegin >= 1500 AND m.datedend <= 1600;",
    "10. How many artifacts have no media files?":
        "SELECT COUNT(*) AS no_media FROM artifact_media WHERE COALESCE(mediacount,0) = 0;",
    "11. What are all the distinct hues used in the dataset?":
        "SELECT DISTINCT hue FROM artifact_colors WHERE hue IS NOT NULL;",
    "12. What are the top 5 most used colors by frequency?":
        "SELECT hue, COUNT(*) AS freq FROM artifact_colors WHERE hue IS NOT NULL GROUP BY hue ORDER BY freq DESC LIMIT 5;",
    "13. What is the average coverage percentage for each hue?":
        "SELECT hue, AVG(percentage) AS avg_percentage FROM artifact_colors WHERE percentage IS NOT NULL GROUP BY hue ORDER BY avg_percentage DESC;",
    # 14 dynamic
    "15. What is the total number of color entries in the dataset?":
        "SELECT COUNT(*) AS total_colors FROM artifact_colors;",
    "16. List artifact titles and hues for all artifacts belonging to the Byzantine culture":
        "SELECT m.title, c.hue FROM artifact_metadata m JOIN artifact_colors c ON m.id = c.object_id WHERE m.culture='Byzantine';",
    "17. List each artifact title with its associated hues":
        "SELECT m.title, c.hue FROM artifact_metadata m JOIN artifact_colors c ON m.id = c.object_id;",
    "18. Get artifact titles, cultures, and media ranks where the period is not null":
        "SELECT m.title, m.culture, a.rank FROM artifact_metadata m JOIN artifact_media a ON m.id = a.object_id WHERE m.period IS NOT NULL;",
    "19. Find artifact titles ranked in the top 10 that include the color hue 'Grey'":
        "SELECT m.title, a.rank FROM artifact_metadata m JOIN artifact_media a ON m.id = a.object_id JOIN artifact_colors c ON m.id = c.object_id WHERE c.hue='Grey' ORDER BY a.rank ASC LIMIT 10;",
    "20. How many artifacts exist per classification, and what is the average media count for each?":
        "SELECT m.classification, COUNT(*) AS total, AVG(a.mediacount) AS avg_media FROM artifact_metadata m JOIN artifact_media a ON m.id = a.object_id GROUP BY m.classification ORDER BY total DESC;",
    "21. Which culture have created more artifacts?":
        "SELECT culture, COUNT(*) AS total FROM artifact_metadata GROUP BY culture ORDER BY total DESC LIMIT 1;",
    "22. During which period most of the artifacts are created?":
        "SELECT period, COUNT(*) AS total FROM artifact_metadata WHERE period IS NOT NULL GROUP BY period ORDER BY total DESC LIMIT 1;",
    "23. What is the most common artifact in every period?":
        """
        WITH counts AS (
            SELECT period, title, COUNT(*) AS cnt
            FROM artifact_metadata
            WHERE period IS NOT NULL
            GROUP BY period, title
        ), maxes AS (
            SELECT period, MAX(cnt) AS max_cnt FROM counts GROUP BY period
        )
        SELECT c.period, c.title, c.cnt FROM counts c JOIN maxes m ON c.period=m.period AND c.cnt=m.max_cnt ORDER BY c.period;
        """,
    "24. How many colors are used these artifacts":
        "SELECT COUNT(DISTINCT hue) AS total_unique_colors FROM artifact_colors WHERE hue IS NOT NULL;",
    "25. What is the most common artifact in every culture?":
        """
        WITH counts AS (
            SELECT culture, title, COUNT(*) AS cnt
            FROM artifact_metadata
            WHERE culture IS NOT NULL
            GROUP BY culture, title
        ), maxes AS (
            SELECT culture, MAX(cnt) AS max_cnt FROM counts GROUP BY culture
        )
        SELECT c.culture, c.title, c.cnt FROM counts c JOIN maxes m ON c.culture=m.culture AND c.cnt=m.max_cnt ORDER BY c.culture;
        """
}

# -----------------------
# Top controls (exact: single classification input + Collect Data)
# -----------------------
left, middle, right = st.columns([5, 1, 1])
with left:
    classification = st.text_input("Enter a classification", value="")
with middle:
    pass
with right:
    if st.button("Collect Data"):
        if classification:
            if classification.lower() in st.session_state["inserted_classifications"]:
                st.error(f"The classification '{classification}' has already been inserted!")
            else:
                with st.spinner("Fetching data..."):
                    try:
                        meta_df, media_df, colors_df, raw_data = fetch_data(classification)
                        st.session_state["meta_df"] = meta_df
                        st.session_state["media_df"] = media_df
                        st.session_state["colors_df"] = colors_df
                        st.session_state["raw_data"] = raw_data
                        st.session_state["collected"] = True
                        st.session_state["inserted"] = False
                        st.session_state["show_choice"] = True
                        st.session_state["migrate_click"] = False
                        st.session_state["show_queries"] = False
                        st.session_state["show_tables_after_insert"] = False
                        st.success(f"Collected {len(meta_df)} records for '{classification}'.")
                    except Exception as e:
                        st.error(f"Failed to collect data: {e}")
        else:
            st.warning("Please enter a classification to begin.")

st.markdown("---")
# Row of two buttons (Collected Data, Migrate to SQL, SQL Queries)
b1, b2, b3 = st.columns(3)

if b1.button("Collected Data"):
    st.session_state.show_choice = True
    st.session_state.migrate_click = False
    st.session_state.show_queries = False
    st.session_state.show_tables_after_insert = False

if b2.button("➡️ Migrate to SQL"):
    st.session_state.migrate_click = True
    st.session_state.show_choice = False
    st.session_state.show_queries = False
    st.session_state.show_tables_after_insert = False
    
if b3.button("📄 SQL Queries"):
    st.session_state.show_queries = True
    st.session_state.show_choice = False
    st.session_state.migrate_click = False
    st.session_state.show_tables_after_insert = False


# -----------------------
# Select Your Choice (show collected data as expanded JSON)
# -----------------------
if st.session_state.get("show_choice", False):
    if not st.session_state.get("collected", False):
        st.warning("Please collect data first.")
    else:
        c1, c2, c3 = st.columns(3)
        with c1:
            st.markdown("**Metadata**")
            json_str = json.dumps(st.session_state["meta_df"].to_dict('records'), indent=4)
            st.code(json_str, language="json")
        with c2:
            st.markdown("**Media**")
            json_str = json.dumps(st.session_state["media_df"].to_dict('records'), indent=4)
            st.code(json_str, language="json")
        with c3:
            st.markdown("**Colors**")
            json_str = json.dumps(st.session_state["colors_df"].to_dict('records'), indent=4)
            st.code(json_str, language="json")

# -----------------------
# Migrate to SQL (Insert only)
# -----------------------
if st.session_state.get("migrate_click", False):
    st.subheader("Migrate to SQL")
    if not st.session_state.get("collected", False):
        st.warning("Please collect data first.")
    else:
        if st.button("Insert"):
            if classification.lower() in st.session_state["inserted_classifications"]:
                st.error(f"The classification '{classification}' already exists!")
            else:
                try:
                    insert_frames(st.session_state["meta_df"], st.session_state["media_df"], st.session_state["colors_df"])
                    st.session_state["inserted"] = True
                    st.session_state["show_tables_after_insert"] = True
                    
                    st.session_state["inserted_classifications"].add(classification.lower())
                    
                    st.session_state["combined_meta_df"] = pd.concat([st.session_state["combined_meta_df"], st.session_state["meta_df"]], ignore_index=True).drop_duplicates(subset=["id"], keep="first")
                    st.session_state["combined_media_df"] = pd.concat([st.session_state["combined_media_df"], st.session_state["media_df"]], ignore_index=True).drop_duplicates(subset=["object_id"], keep="first")
                    st.session_state["combined_colors_df"] = pd.concat([st.session_state["combined_colors_df"], st.session_state["colors_df"]], ignore_index=True).drop_duplicates(subset=["object_id", "hue"], keep="first")

                    st.success("Data inserted into database successfully.")
                except Exception as e:
                    st.error(f"Insert failed: {e}")

# -----------------------
# Display the single combined table after each insert
# -----------------------
if st.session_state.get("show_tables_after_insert", False):
    if not st.session_state["combined_meta_df"].empty:
        st.markdown("### Artifacts Metadata")
        st.dataframe(st.session_state["combined_meta_df"], use_container_width=True)

        st.markdown("### Artifacts Media")
        st.dataframe(st.session_state["combined_media_df"], use_container_width=True)

        st.markdown("### Artifacts Colors")
        st.dataframe(st.session_state["combined_colors_df"], use_container_width=True)

# -----------------------
# SQL Queries (25) - only run after insert
# -----------------------
if st.session_state.get("show_queries", False):
    st.subheader("SQL Queries")
    if not st.session_state.get("inserted", False):
        st.warning("Please insert data first (➡️ Migrate to SQL → Insert) before running queries.")
    else:
        options = list(QUERIES.keys())
        options.insert(13, "14. List all colors used for a given artifact ID")
        chosen = st.selectbox("Choose a question:", options, index=0)

        sql_to_run = None
        if chosen.startswith("14."):
            artifact_id = st.text_input("Enter Artifact ID (numeric)", value="")
            if artifact_id and artifact_id.strip().isdigit():
                sql_to_run = f"SELECT hue, percentage FROM artifact_colors WHERE object_id = {int(artifact_id)};"
            else:
                st.info("Please enter a numeric Artifact ID to run question 14.")
        else:
            sql_to_run = QUERIES[chosen]

        if st.button("Run Query"):
            if not sql_to_run:
                st.info("Provide required input first.")
            else:
                try:
                    conn = get_conn()
                    df_res = pd.read_sql_query(sql_to_run, conn)
                    conn.close()
                    if df_res.empty:
                        st.info("Query ran successfully but returned no rows.")
                    else:
                        st.dataframe(df_res, use_container_width=True)
                except Exception as e:
                    st.error(f"Query failed: {e}")
                    
