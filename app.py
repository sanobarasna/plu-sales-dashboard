# ==========================================================
# PLU Sales + Supplier Profitability Dashboard (CLEAN DF)
# + Space Occupiers (Low-selling items) Export (latest stock)
# + Custom Time Range Performance
# + Consolidated Top Items (Custom Time Range + Optional Supplier Filter)
#     - NEW: Toggle "Breakdown by supplier" for each item
#         * OFF  -> aggregated across suppliers (one row per item)
#         * ON   -> item x supplier rows (shows profitability + units per supplier)
# ==========================================================

import re
import streamlit as st
import pandas as pd
import numpy as np
from io import BytesIO

# -----------------------
# CONFIG
# -----------------------
st.set_page_config(page_title="PLU Sales & Profit Dashboard", layout="wide")
st.title("🧾 PLU Sales & Supplier Profitability Dashboard")

BRACKET_RE = re.compile(r"\[([^\]]+)\]")

# -----------------------
# HELPERS
# -----------------------
def _strip_weird_breaks(x) -> str:
    s = str(x)
    return s.replace("_x000D_", " ").replace("\r", " ").replace("\n", " ").strip()

def parse_group_tags(group_val):
    """'[CATEGORY][SUP1][SUP2]...' -> (CATEGORY, [SUP1, SUP2, ...])"""
    if pd.isna(group_val):
        return (np.nan, [])
    s = _strip_weird_breaks(group_val)
    tags = [t.strip() for t in BRACKET_RE.findall(s) if t.strip()]
    if not tags:
        return (np.nan, [])
    return tags[0], tags[1:]

def to_num_series(s: pd.Series) -> pd.Series:
    """Robust numeric conversion for Excel weirdness."""
    ss = (
        s.astype(str)
         .str.replace(",", "", regex=False)
         .str.strip()
         .replace({"nan": np.nan, "None": np.nan, "": np.nan})
    )
    return pd.to_numeric(ss, errors="coerce")

def infer_plu_column(columns) -> str:
    """In your file, PLU column header sometimes appears as '1'."""
    for c in columns:
        if str(c).strip() == "1":
            return c
    for cand in ["PLU_CODE", "PLU", "BARCODE", "UPC"]:
        if cand in columns:
            return cand
    return None

def resolve_supplier_from_group(clean: pd.DataFrame) -> pd.DataFrame:
    """
    Ensures ONE supplier per row without double counting, using your rules:
      - If GROUP has 1 supplier -> use it
      - If GROUP has 0 supplier -> use most common supplier for that category
      - If GROUP has multiple suppliers -> use item-preferred supplier if available,
        else category-preferred if available, else first supplier in list.
    """
    parsed = clean["GROUP_RAW"].apply(parse_group_tags)
    clean["CATEGORY_FROM_GROUP"] = parsed.apply(lambda x: x[0])
    clean["SUPPLIERS_LIST"] = parsed.apply(lambda x: x[1])

    clean["SINGLE_SUPPLIER"] = clean["SUPPLIERS_LIST"].apply(
        lambda L: L[0] if isinstance(L, list) and len(L) == 1 else np.nan
    )

    # item preferred supplier learned from single-supplier rows
    item_counts = (
        clean.dropna(subset=["SINGLE_SUPPLIER", "PLU_CODE", "DESCRIPTION"])
            .groupby(["PLU_CODE", "DESCRIPTION", "SINGLE_SUPPLIER"])
            .size()
            .reset_index(name="N")
    )
    item_pref = {}
    if not item_counts.empty:
        item_pref = (
            item_counts.sort_values(["PLU_CODE", "DESCRIPTION", "N"], ascending=[True, True, False])
                      .drop_duplicates(["PLU_CODE", "DESCRIPTION"])
                      .set_index(["PLU_CODE", "DESCRIPTION"])["SINGLE_SUPPLIER"]
                      .to_dict()
        )

    # category preferred supplier learned from single-supplier rows
    cat_counts = (
        clean.dropna(subset=["SINGLE_SUPPLIER", "CATEGORY"])
            .groupby(["CATEGORY", "SINGLE_SUPPLIER"])
            .size()
            .reset_index(name="N")
    )
    cat_mode = {}
    if not cat_counts.empty:
        cat_mode = (
            cat_counts.sort_values(["CATEGORY", "N"], ascending=[True, False])
                     .drop_duplicates(["CATEGORY"])
                     .set_index("CATEGORY")["SINGLE_SUPPLIER"]
                     .to_dict()
        )

    def resolve_row(row):
        suppliers = row["SUPPLIERS_LIST"] if isinstance(row["SUPPLIERS_LIST"], list) else []
        category = row["CATEGORY"]
        key = (row["PLU_CODE"], row["DESCRIPTION"])

        if len(suppliers) == 1:
            return suppliers[0]

        if len(suppliers) == 0:
            return cat_mode.get(category, "UNKNOWN")

        pref_item = item_pref.get(key)
        if pref_item and pref_item in suppliers:
            return pref_item

        pref_cat = cat_mode.get(category)
        if pref_cat and pref_cat in suppliers:
            return pref_cat

        return suppliers[0]

    clean["SUPPLIER_RESOLVED"] = clean.apply(resolve_row, axis=1).astype(str).str.strip()
    clean.loc[clean["SUPPLIER_RESOLVED"].isin(["nan", "None", ""]), "SUPPLIER_RESOLVED"] = "UNKNOWN"
    return clean

def df_to_excel_bytes(df: pd.DataFrame, sheet_name: str = "data") -> bytes:
    buf = BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name=sheet_name)
    buf.seek(0)
    return buf.getvalue()

def latest_stock_per_item(df_full: pd.DataFrame) -> pd.DataFrame:
    """
    Returns latest known stock per item (PLU_CODE, DESCRIPTION) based on DATE.
    Uses the last non-null STOCK value in time order (forward-filled within item).
    """
    tmp = df_full.sort_values(["PLU_CODE", "DESCRIPTION", "DATE"]).copy()
    tmp["STOCK_FFILL"] = tmp.groupby(["PLU_CODE", "DESCRIPTION"])["STOCK"].ffill()
    out = (
        tmp.groupby(["PLU_CODE", "DESCRIPTION"], as_index=False)
           .tail(1)[["PLU_CODE", "DESCRIPTION", "STOCK_FFILL", "DATE"]]
           .rename(columns={"STOCK_FFILL": "LATEST_STOCK", "DATE": "STOCK_ASOF_DATE"})
    )
    return out

def clamp_date_range(start_date, end_date):
    if start_date is None or end_date is None:
        return None, None
    if start_date > end_date:
        return end_date, start_date
    return start_date, end_date

# -----------------------
# CLEAN LOADER
# -----------------------
@st.cache_data
def load_plu_report_clean(file, sheet_name=None) -> pd.DataFrame:
    raw = pd.read_excel(file, sheet_name=sheet_name if sheet_name else 0, header=4)

    # Remove "Unnamed" columns
    raw = raw.loc[:, ~raw.columns.astype(str).str.upper().str.contains("^UNNAMED", na=False)].copy()

    # DATE is first col by position
    date_col = raw.columns[0]
    raw = raw.rename(columns={date_col: "DATE"})

    # PLU column header appears as "1"
    plu_col = infer_plu_column(raw.columns)
    if plu_col is None:
        raise ValueError(f"Could not find PLU column (expected header '1'). Found: {list(raw.columns)}")
    raw = raw.rename(columns={plu_col: "PLU_CODE"})

    # Required
    for col in ["DESCRIPTION", "GROUP", "USAGE"]:
        if col not in raw.columns:
            raise ValueError(f"Missing {col} column. Found: {list(raw.columns)}")

    # Optional columns
    has_profit = "PROFIT" in raw.columns
    has_total_sales = "TOTAL" in raw.columns
    has_stock = "STOCK" in raw.columns

    clean = pd.DataFrame({
        "DATE": pd.to_datetime(raw["DATE"], errors="coerce").ffill(),
        "DESCRIPTION": raw["DESCRIPTION"].astype(str).str.strip(),
        "PLU_CODE": to_num_series(raw["PLU_CODE"]),
        "GROUP_RAW": raw["GROUP"],
        "USAGE_NET": to_num_series(raw["USAGE"]).fillna(0),
    })

    clean["PROFIT"] = to_num_series(raw["PROFIT"]).fillna(0) if has_profit else 0.0
    clean["TOTAL_SALES"] = to_num_series(raw["TOTAL"]).fillna(0) if has_total_sales else np.nan
    clean["STOCK"] = to_num_series(raw["STOCK"]).fillna(np.nan) if has_stock else np.nan

    # Cleanup
    clean = clean.dropna(subset=["DATE", "PLU_CODE"]).copy()
    clean = clean[clean["DESCRIPTION"].notna() & (clean["DESCRIPTION"].astype(str).str.strip() != "")]
    clean["PLU_CODE"] = clean["PLU_CODE"].astype(int)

    # Units sold metric
    clean["USAGE_SOLD"] = clean["USAGE_NET"].clip(lower=0)

    # Category from group
    clean["CATEGORY"] = clean["GROUP_RAW"].apply(lambda x: parse_group_tags(x)[0])

    # Resolve supplier
    clean = resolve_supplier_from_group(clean)

    return clean.sort_values(["DATE", "PLU_CODE"]).reset_index(drop=True)

# -----------------------
# UI: FILE UPLOAD
# -----------------------
plu_file = st.file_uploader("📁 Upload your PLU report (.xlsx)", type=["xlsx"])
if plu_file is None:
    st.info("👆 Upload the Excel file to continue.")
    st.stop()

sheet_name = st.sidebar.text_input("Sheet name (leave blank for first sheet)", value="")
try:
    df = load_plu_report_clean(plu_file, sheet_name=sheet_name.strip() if sheet_name.strip() else None)
except Exception as e:
    st.error(f"Failed to load file: {e}")
    st.stop()

# -----------------------
# SIDEBAR: GLOBAL FILTERS
# -----------------------
st.sidebar.header("🎛️ Global Filters")

use_net_units = st.sidebar.checkbox("Use NET units (include negatives)", value=False)
units_col = "USAGE_NET" if use_net_units else "USAGE_SOLD"

date_window = st.sidebar.selectbox(
    "Quick date range (applies to Space Occupiers + Item Profile charts)",
    ["All", "Last 7 days", "Last 30 days", "Last 60 days", "Last 90 days"],
    index=1
)

max_date = df["DATE"].max()
min_date = df["DATE"].min()

if date_window != "All":
    days = int(date_window.split()[1])
    start = max_date - pd.Timedelta(days=days - 1)
    dff = df[df["DATE"] >= start].copy()
else:
    dff = df.copy()

# ==========================================================
# SECTION 0: SPACE OCCUPIERS (LOW-SELLERS) + EXCEL EXPORT
# ==========================================================
st.subheader("🧱 Space Occupiers (Low-selling / barely-moving items)")
st.caption(
    "Find items that sold very little in a recent lookback window. "
    "Includes LATEST STOCK (what you have left on hand)."
)

colA, colB, colC, colD = st.columns(4)
with colA:
    low_lookback_days = st.selectbox("Lookback window (days)", [30, 60, 90, 180, 365], index=2)
with colB:
    max_units_threshold = st.number_input("Max TOTAL units in lookback", 0, 500, 5, 1)
with colC:
    stale_days = st.number_input("Not sold in last N days (0=ignore)", 0, 3650, 30, 1)
with colD:
    stock_min = st.number_input("Optional: show only items with stock >= (0=ignore)", 0, 10_000, 0, 1)

end_dt = df["DATE"].max()
start_dt = end_dt - pd.Timedelta(days=low_lookback_days - 1)

lookback_df = df[(df["DATE"] >= start_dt) & (df["DATE"] <= end_dt)].copy()

lookback_item = (
    lookback_df.groupby(["PLU_CODE", "DESCRIPTION"], dropna=False)
               .agg(
                   TOTAL_UNITS_LOOKBACK=(units_col, "sum"),
                   TOTAL_PROFIT_LOOKBACK=("PROFIT", "sum"),
                   TOTAL_SALES_LOOKBACK=("TOTAL_SALES", "sum"),
                   LAST_SOLD_DATE_LOOKBACK=("DATE", "max"),
               )
               .reset_index()
)

ever_item = (
    df.groupby(["PLU_CODE", "DESCRIPTION"], dropna=False)
      .agg(
          FIRST_SOLD_DATE=("DATE", "min"),
          LAST_SOLD_DATE_EVER=("DATE", "max"),
      )
      .reset_index()
)

best_supplier_profit = (
    lookback_df.groupby(["PLU_CODE", "DESCRIPTION", "SUPPLIER_RESOLVED"], dropna=False)["PROFIT"]
              .sum()
              .reset_index(name="SUPPLIER_PROFIT_LOOKBACK")
)
best_supplier_profit = (
    best_supplier_profit.sort_values(["PLU_CODE", "DESCRIPTION", "SUPPLIER_PROFIT_LOOKBACK"],
                                     ascending=[True, True, False])
                        .drop_duplicates(["PLU_CODE", "DESCRIPTION"])
                        .rename(columns={"SUPPLIER_RESOLVED": "BEST_SUPPLIER_BY_PROFIT"})
                        [["PLU_CODE", "DESCRIPTION", "BEST_SUPPLIER_BY_PROFIT", "SUPPLIER_PROFIT_LOOKBACK"]]
)

cat_mode = (
    df.dropna(subset=["CATEGORY"])
      .groupby(["PLU_CODE", "DESCRIPTION"])["CATEGORY"]
      .agg(lambda x: x.value_counts().index[0])
      .reset_index()
)

stock_latest = latest_stock_per_item(df)

space = (
    lookback_item.merge(ever_item, on=["PLU_CODE", "DESCRIPTION"], how="left")
                .merge(best_supplier_profit, on=["PLU_CODE", "DESCRIPTION"], how="left")
                .merge(cat_mode, on=["PLU_CODE", "DESCRIPTION"], how="left")
                .merge(stock_latest, on=["PLU_CODE", "DESCRIPTION"], how="left")
)

space["DAYS_SINCE_LAST_SOLD_EVER"] = (end_dt - space["LAST_SOLD_DATE_EVER"]).dt.days

# LOSS_% calculation (profit-based)
space["LOSS_%"] = np.nan
sales_ok = space["TOTAL_SALES_LOOKBACK"].fillna(0) > 0
neg_profit = space["TOTAL_PROFIT_LOOKBACK"] < 0

space.loc[sales_ok & neg_profit, "LOSS_%"] = (
    (-space.loc[sales_ok & neg_profit, "TOTAL_PROFIT_LOOKBACK"]
     / space.loc[sales_ok & neg_profit, "TOTAL_SALES_LOOKBACK"]) * 100
).round(2)

fallback_mask = (~sales_ok) & neg_profit
space.loc[fallback_mask, "LOSS_%"] = (
    (-space.loc[fallback_mask, "TOTAL_PROFIT_LOOKBACK"]
     / (space.loc[fallback_mask, "TOTAL_PROFIT_LOOKBACK"].abs() + 1)) * 100
).round(2)

# Filter: low units + optional stale + optional stock constraint
space_filtered = space[space["TOTAL_UNITS_LOOKBACK"] <= max_units_threshold].copy()
if stale_days > 0:
    space_filtered = space_filtered[space_filtered["DAYS_SINCE_LAST_SOLD_EVER"] >= stale_days].copy()
if stock_min > 0:
    space_filtered = space_filtered[space_filtered["LATEST_STOCK"].fillna(0) >= stock_min].copy()

# Sort: least sold, most stale, highest stock, then highest loss%
space_filtered = space_filtered.sort_values(
    ["TOTAL_UNITS_LOOKBACK", "DAYS_SINCE_LAST_SOLD_EVER", "LATEST_STOCK", "LOSS_%"],
    ascending=[True, False, False, False]
)

show_cols = [
    "PLU_CODE",
    "DESCRIPTION",
    "CATEGORY",
    "BEST_SUPPLIER_BY_PROFIT",
    "TOTAL_UNITS_LOOKBACK",
    "LATEST_STOCK",
    "STOCK_ASOF_DATE",
    "FIRST_SOLD_DATE",
    "LAST_SOLD_DATE_EVER",
    "DAYS_SINCE_LAST_SOLD_EVER",
    "TOTAL_PROFIT_LOOKBACK",
    "TOTAL_SALES_LOOKBACK",
    "LOSS_%"
]

st.write(f"Lookback: **{start_dt.date()} → {end_dt.date()}** | Items shown: **{len(space_filtered)}**")
st.dataframe(space_filtered[show_cols], use_container_width=True, height=420)

st.download_button(
    "⬇️ Download Space Occupiers with Stock (Excel)",
    data=df_to_excel_bytes(space_filtered[show_cols], sheet_name="space_occupiers"),
    file_name=f"space_occupiers_with_stock_{low_lookback_days}d.xlsx",
    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
)

# ==========================================================
# SECTION 1: CUSTOM TIME RANGE PERFORMANCE
# ==========================================================
st.subheader("📅 Sales Performance (Custom Time Range)")
st.caption("Pick any start and end date (e.g., Jan 22 → Mar 22). Optional: search item name. Ranked by units or profit.")

with st.expander("Open Custom Time Range Filter", expanded=True):
    c1, c2, c3 = st.columns([1.2, 1.2, 1.6])
    with c1:
        range_start = st.date_input(
            "Start date",
            value=min_date.date(),
            min_value=min_date.date(),
            max_value=max_date.date(),
            key="range_start",
        )
    with c2:
        range_end = st.date_input(
            "End date",
            value=max_date.date(),
            min_value=min_date.date(),
            max_value=max_date.date(),
            key="range_end",
        )
    with c3:
        range_rank_by = st.selectbox("Rank by", ["TOTAL_UNITS", "TOTAL_PROFIT"], index=0, key="range_rank")

    rstart, rend = clamp_date_range(pd.Timestamp(range_start), pd.Timestamp(range_end))
    range_top_n = st.number_input("Top N items (range)", min_value=10, max_value=2000, value=100, step=10, key="range_topn")
    range_search = st.text_input("Search item name (optional, min 3 letters)", value="", key="range_search")

    range_df = df[(df["DATE"] >= rstart) & (df["DATE"] <= rend)].copy()

    if range_df.empty:
        st.warning("No rows found in that selected date range.")
    else:
        items_range = (
            range_df.groupby(["PLU_CODE", "DESCRIPTION"], dropna=False)
                    .agg(
                        TOTAL_UNITS=(units_col, "sum"),
                        TOTAL_PROFIT=("PROFIT", "sum"),
                        _ACTIVE_DAYS=("DATE", "nunique"),
                    )
                    .reset_index()
        )
        items_range["TOTAL_UNITS_PER_DAY"] = np.where(
            items_range["_ACTIVE_DAYS"] > 0,
            items_range["TOTAL_UNITS"] / items_range["_ACTIVE_DAYS"],
            0
        ).round(3)
        items_range = items_range.drop(columns=["_ACTIVE_DAYS"])

        s = (range_search or "").strip().lower()
        if len(s) >= 3:
            items_range = items_range[items_range["DESCRIPTION"].str.lower().str.contains(s, na=False)].copy()

        sort_col = "TOTAL_UNITS" if range_rank_by == "TOTAL_UNITS" else "TOTAL_PROFIT"
        items_range = items_range.sort_values(sort_col, ascending=False).reset_index(drop=True)

        st.write(f"Selected range: **{rstart.date()} → {rend.date()}** | Items in result: **{len(items_range)}**")
        st.dataframe(items_range.head(int(range_top_n)), use_container_width=True, height=420)

        if not items_range.empty:
            top = items_range.iloc[0]
            if sort_col == "TOTAL_UNITS":
                st.success(
                    f"🏆 Top item by **Units** in selected range: "
                    f"**{top['DESCRIPTION']}** (PLU {int(top['PLU_CODE'])}) — Units: {int(top['TOTAL_UNITS'])}"
                )
            else:
                st.success(
                    f"🏆 Top item by **Profit** in selected range: "
                    f"**{top['DESCRIPTION']}** (PLU {int(top['PLU_CODE'])}) — Profit: {top['TOTAL_PROFIT']:.2f}"
                )

        st.download_button(
            "⬇️ Download Custom Range Performance (Excel)",
            data=df_to_excel_bytes(items_range, sheet_name="range_performance"),
            file_name=f"range_performance_{rstart.date()}_to_{rend.date()}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )

# ==========================================================
# SECTION 2: TOP ITEMS (Custom Time Range + Optional Supplier + Supplier Breakdown Toggle)
# ==========================================================
st.subheader("🏆 Top Items (Custom Time Range + Optional Supplier Filter)")
st.caption(
    "Pick a custom time range, optionally filter to a supplier, optionally search item name.\n"
    "NEW: Toggle supplier breakdown to see per-supplier results for each item."
)

with st.expander("Open Top Items Filter", expanded=True):
    t1, t2, t3, t4 = st.columns([1.2, 1.2, 1.2, 1.4])
    with t1:
        top_start = st.date_input(
            "Start",
            value=(max_date - pd.Timedelta(days=30)).date(),
            min_value=min_date.date(),
            max_value=max_date.date(),
            key="top_start"
        )
    with t2:
        top_end = st.date_input(
            "End",
            value=max_date.date(),
            min_value=min_date.date(),
            max_value=max_date.date(),
            key="top_end"
        )
    with t3:
        top_rank_by = st.selectbox("Rank by", ["TOTAL_UNITS", "TOTAL_PROFIT"], index=0, key="top_rank")
    with t4:
        suppliers_all = ["All Suppliers"] + sorted(df["SUPPLIER_RESOLVED"].dropna().unique().tolist())
        supplier_filter = st.selectbox("Supplier", suppliers_all, index=0, key="top_supplier")

    top_search = st.text_input("Search item name (optional, min 3 letters)", value="", key="top_search")
    top_n = st.number_input("Top N rows", min_value=10, max_value=5000, value=100, step=10, key="top_n")
    breakdown = st.toggle("Breakdown by supplier (show per-supplier rows)", value=False)

    ts, te = clamp_date_range(pd.Timestamp(top_start), pd.Timestamp(top_end))
    top_df = df[(df["DATE"] >= ts) & (df["DATE"] <= te)].copy()

    if supplier_filter != "All Suppliers":
        top_df = top_df[top_df["SUPPLIER_RESOLVED"] == supplier_filter].copy()

    s = (top_search or "").strip().lower()
    if len(s) >= 3:
        top_df = top_df[top_df["DESCRIPTION"].str.lower().str.contains(s, na=False)].copy()

    if top_df.empty:
        st.warning("No rows found for that filter (date range / supplier / search).")
    else:
        sort_col = "TOTAL_UNITS" if top_rank_by == "TOTAL_UNITS" else "TOTAL_PROFIT"

        if not breakdown:
            # One row per item (aggregated across suppliers)
            top_items_df = (
                top_df.groupby(["PLU_CODE", "DESCRIPTION"], dropna=False)
                      .agg(
                          TOTAL_UNITS=(units_col, "sum"),
                          TOTAL_PROFIT=("PROFIT", "sum"),
                          _ACTIVE_DAYS=("DATE", "nunique"),
                      )
                      .reset_index()
            )
            top_items_df["TOTAL_UNITS_PER_DAY"] = np.where(
                top_items_df["_ACTIVE_DAYS"] > 0,
                top_items_df["TOTAL_UNITS"] / top_items_df["_ACTIVE_DAYS"],
                0
            ).round(3)
            top_items_df = top_items_df.drop(columns=["_ACTIVE_DAYS"])
            top_items_df = top_items_df.sort_values(sort_col, ascending=False)

            st.write(f"Filter: **{ts.date()} → {te.date()}** | Supplier: **{supplier_filter}** | Rows: **{len(top_items_df)}**")
            st.dataframe(top_items_df.head(int(top_n)), use_container_width=True, height=420)

            st.download_button(
                "⬇️ Download Top Items (Item totals) (Excel)",
                data=df_to_excel_bytes(top_items_df, sheet_name="top_items"),
                file_name=f"top_items_{ts.date()}_to_{te.date()}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )

        else:
            # Per item x supplier breakdown
            top_items_sup = (
                top_df.groupby(["PLU_CODE", "DESCRIPTION", "SUPPLIER_RESOLVED"], dropna=False)
                      .agg(
                          TOTAL_UNITS=(units_col, "sum"),
                          TOTAL_PROFIT=("PROFIT", "sum"),
                          TOTAL_SALES=("TOTAL_SALES", "sum"),
                          _ACTIVE_DAYS=("DATE", "nunique"),
                      )
                      .reset_index()
            )
            top_items_sup["TOTAL_UNITS_PER_DAY"] = np.where(
                top_items_sup["_ACTIVE_DAYS"] > 0,
                top_items_sup["TOTAL_UNITS"] / top_items_sup["_ACTIVE_DAYS"],
                0
            ).round(3)
            top_items_sup["PROFIT_PER_UNIT"] = np.where(
                top_items_sup["TOTAL_UNITS"] > 0,
                top_items_sup["TOTAL_PROFIT"] / top_items_sup["TOTAL_UNITS"],
                0
            ).round(4)
            top_items_sup = top_items_sup.drop(columns=["_ACTIVE_DAYS"])

            # Sort by chosen metric primarily, then units
            top_items_sup = top_items_sup.sort_values(
                [sort_col, "TOTAL_UNITS"],
                ascending=[False, False]
            )

            st.write(
                f"Filter: **{ts.date()} → {te.date()}** | Supplier: **{supplier_filter}** "
                f"| Breakdown rows: **{len(top_items_sup)}**"
            )
            st.dataframe(top_items_sup.head(int(top_n)), use_container_width=True, height=420)

            st.download_button(
                "⬇️ Download Top Items (Supplier breakdown) (Excel)",
                data=df_to_excel_bytes(top_items_sup, sheet_name="top_items_supplier"),
                file_name=f"top_items_supplier_breakdown_{ts.date()}_to_{te.date()}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )

# ==========================================================
# SECTION 3: ITEM SEARCH + ITEM PROFILE (profitability ranking)
# (still uses quick date_window dff for charts)
# ==========================================================
st.sidebar.header("🔎 Item Search (Profile)")
query = st.sidebar.text_input("Type item name (min 5 letters)", value="")

top_items_for_search = (
    dff.groupby(["PLU_CODE", "DESCRIPTION"], dropna=False)[units_col]
       .sum()
       .reset_index(name="TOTAL_UNITS")
       .sort_values("TOTAL_UNITS", ascending=False)
)

selected_item = None
if query and len(query.strip()) >= 5:
    q = query.strip().lower()
    matches = top_items_for_search[top_items_for_search["DESCRIPTION"].str.lower().str.contains(q, na=False)].copy()
    matches = matches.sort_values("TOTAL_UNITS", ascending=False).head(80)

    if matches.empty:
        st.warning("No matching items found (within the quick date range filter).")
    else:
        options = [
            f'{r["DESCRIPTION"]} | PLU: {r["PLU_CODE"]} | Units: {int(r["TOTAL_UNITS"])}'
            for _, r in matches.iterrows()
        ]
        pick = st.selectbox("Select the exact item", options)
        pick_idx = options.index(pick)
        selected_item = (int(matches.iloc[pick_idx]["PLU_CODE"]), matches.iloc[pick_idx]["DESCRIPTION"])
else:
    st.info("Type at least 5 letters in the sidebar to search items.")

if selected_item:
    plu, desc = selected_item
    item_df = dff[(dff["PLU_CODE"] == plu) & (dff["DESCRIPTION"] == desc)].copy()

    st.subheader(f"📌 Item Profile: {desc} (PLU {plu})")

    total_units = float(item_df[units_col].sum())
    total_profit = float(item_df["PROFIT"].sum())
    days_present = int(item_df["DATE"].nunique())
    avg_units_per_day = total_units / max(days_present, 1)

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total Units", f"{int(total_units)}")
    c2.metric("Active Days (unique dates)", f"{days_present}")
    c3.metric("Avg Units / Day", f"{avg_units_per_day:.2f}")
    c4.metric("Total Profit (sum)", f"{total_profit:.2f}")

    st.markdown("### 💰 Suppliers for this item (Most → Least PROFITABLE)")
    sup_profit = (
        item_df.groupby("SUPPLIER_RESOLVED")
              .agg(
                  TOTAL_UNITS=(units_col, "sum"),
                  TOTAL_PROFIT=("PROFIT", "sum"),
                  TOTAL_SALES=("TOTAL_SALES", "sum"),
                  ACTIVE_DAYS=("DATE", "nunique"),
              )
              .reset_index()
    )
    sup_profit["PROFIT_PER_UNIT"] = np.where(
        sup_profit["TOTAL_UNITS"] > 0,
        sup_profit["TOTAL_PROFIT"] / sup_profit["TOTAL_UNITS"],
        0
    )
    sup_profit = sup_profit.sort_values(["TOTAL_PROFIT", "TOTAL_UNITS"], ascending=[False, False])
    st.dataframe(sup_profit, use_container_width=True, height=280)

    best_supplier = sup_profit.iloc[0]["SUPPLIER_RESOLVED"] if not sup_profit.empty else "UNKNOWN"
    st.success(f"🏅 Most profitable supplier for this item (in quick date range): **{best_supplier}**")

    st.markdown("### 📈 Item performance over time")

    item_df = item_df.copy()
    item_df["YEAR_MONTH"] = item_df["DATE"].dt.to_period("M").astype(str)

    monthly = (
        item_df.groupby("YEAR_MONTH")
              .agg(UNITS=(units_col, "sum"), PROFIT=("PROFIT", "sum"))
              .reset_index()
              .sort_values("YEAR_MONTH")
    )

    cc1, cc2 = st.columns(2)
    with cc1:
        st.markdown("**Monthly Units**")
        st.bar_chart(monthly.set_index("YEAR_MONTH")[["UNITS"]])
    with cc2:
        st.markdown("**Monthly Profit**")
        st.bar_chart(monthly.set_index("YEAR_MONTH")[["PROFIT"]])

    st.markdown("**Rolling window (daily)**")
    roll_days = st.slider("Rolling window length (days)", 3, 60, 14)

    daily = (
        item_df.groupby("DATE")
              .agg(UNITS=(units_col, "sum"), PROFIT=("PROFIT", "sum"))
              .reset_index()
              .sort_values("DATE")
              .set_index("DATE")
    )
    daily_roll = daily.rolling(window=roll_days, min_periods=1).sum()

    r1, r2 = st.columns(2)
    with r1:
        st.markdown(f"Rolling {roll_days}-day Units")
        st.line_chart(daily_roll[["UNITS"]])
    with r2:
        st.markdown(f"Rolling {roll_days}-day Profit")
        st.line_chart(daily_roll[["PROFIT"]])

    with st.expander("Show raw rows for this item"):
        show_cols_item = [
            "DATE", "PLU_CODE", "DESCRIPTION", "CATEGORY", "SUPPLIER_RESOLVED", "GROUP_RAW",
            "USAGE_NET", "USAGE_SOLD", "STOCK", "PROFIT", "TOTAL_SALES"
        ]
        st.dataframe(item_df[show_cols_item].sort_values("DATE"), use_container_width=True, height=380)

# ==========================================================
# SECTION 4: FAST / SLOW MOVERS (UNITS) - uses quick date window dff
# ==========================================================
st.subheader("🚀 Fast Movers / 🐢 Slow Movers (Units based)")

compare_days = st.selectbox("Mover comparison window (days)", [7, 14, 30, 60], index=2)

end_recent = dff["DATE"].max()
start_recent = end_recent - pd.Timedelta(days=compare_days - 1)
start_prev = start_recent - pd.Timedelta(days=compare_days)
end_prev = start_recent - pd.Timedelta(days=1)

recent = dff[(dff["DATE"] >= start_recent) & (dff["DATE"] <= end_recent)]
prev = dff[(dff["DATE"] >= start_prev) & (dff["DATE"] <= end_prev)]

recent_sum = recent.groupby(["PLU_CODE", "DESCRIPTION"])[units_col].sum().reset_index(name="RECENT_UNITS")
prev_sum = prev.groupby(["PLU_CODE", "DESCRIPTION"])[units_col].sum().reset_index(name="PREV_UNITS")

movers = recent_sum.merge(prev_sum, on=["PLU_CODE", "DESCRIPTION"], how="outer").fillna(0)

min_units = st.number_input(
    "Minimum units (either period) to be considered (movers only)",
    min_value=1, max_value=500, value=10, step=1
)
movers = movers[(movers["RECENT_UNITS"] >= min_units) | (movers["PREV_UNITS"] >= min_units)].copy()

movers["DELTA"] = movers["RECENT_UNITS"] - movers["PREV_UNITS"]
movers["PCT_CHANGE"] = (movers["DELTA"] / movers["PREV_UNITS"].replace(0, 1)) * 100
movers["PCT_CHANGE"] = movers["PCT_CHANGE"].round(2)

fast_thresh = st.number_input("Fast mover threshold (% increase)", min_value=5, max_value=1000, value=50, step=5)
slow_thresh = st.number_input("Slow mover threshold (% decrease)", min_value=5, max_value=1000, value=30, step=5)

fast = movers[movers["PCT_CHANGE"] >= fast_thresh].sort_values(["PCT_CHANGE", "RECENT_UNITS"], ascending=[False, False])
slow = movers[movers["PCT_CHANGE"] <= -slow_thresh].sort_values(["PCT_CHANGE", "RECENT_UNITS"], ascending=[True, False])

if fast.empty and slow.empty:
    st.warning("No movers matched your settings. Try lowering min units / thresholds or using a shorter window (7/14 days).")

c1, c2 = st.columns(2)
with c1:
    st.markdown("### 🚀 Fast Movers")
    st.dataframe(fast.head(50), use_container_width=True, height=420)
with c2:
    st.markdown("### 🐢 Slow Movers")
    st.dataframe(slow.head(50), use_container_width=True, height=420)

st.caption(
    f"Movers compare Recent ({start_recent.date()} → {end_recent.date()}) vs "
    f"Previous ({start_prev.date()} → {end_prev.date()}). Units column used: {units_col}."
)
