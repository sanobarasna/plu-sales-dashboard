# ==========================================================
# PLU Sales + Supplier Profitability Dashboard (CLEAN DF)
# + Space Occupiers Export (with Latest Stock)
# + NEW: Month/Year filter to view month-wise sales performance
#        and identify top-selling items for a selected month/year
# ==========================================================
# Added in this version:
# - Sidebar "📆 Month/Year Performance" controls:
#     * Pick Year + Month
#     * See Top items sold in that Month/Year (by units)
#     * Optional: Also show Top items by PROFIT for that Month/Year
#     * Monthly trend for selected item remains in Item Profile section
#
# Notes:
# - header=4 (Excel row 5 is header)
# - DATE is first column by position even if header cell is weird
# - PLU column header may be "1" -> mapped to PLU_CODE
# - GROUP contains tags: [CATEGORY][SUP1][SUP2]...
# - USAGE is units sold; STOCK is remaining stock
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
    Uses the last non-null STOCK value in time order.
    """
    tmp = df_full.sort_values(["PLU_CODE", "DESCRIPTION", "DATE"]).copy()
    tmp["STOCK_FFILL"] = tmp.groupby(["PLU_CODE", "DESCRIPTION"])["STOCK"].ffill()
    out = (
        tmp.groupby(["PLU_CODE", "DESCRIPTION"], as_index=False)
           .tail(1)[["PLU_CODE", "DESCRIPTION", "STOCK_FFILL", "DATE"]]
           .rename(columns={"STOCK_FFILL": "LATEST_STOCK", "DATE": "STOCK_ASOF_DATE"})
    )
    return out

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

    # Time fields
    clean["YEAR"] = clean["DATE"].dt.year
    clean["MONTH"] = clean["DATE"].dt.month
    clean["YEAR_MONTH"] = clean["DATE"].dt.to_period("M").astype(str)

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

date_window = st.sidebar.selectbox(
    "Date range (applies to most sections)",
    ["All", "Last 7 days", "Last 30 days", "Last 60 days", "Last 90 days"],
    index=1
)

use_net_units = st.sidebar.checkbox("Use NET units (include negatives)", value=False)
units_col = "USAGE_NET" if use_net_units else "USAGE_SOLD"

max_date = df["DATE"].max()
if date_window != "All":
    days = int(date_window.split()[1])
    start = max_date - pd.Timedelta(days=days - 1)
    dff = df[df["DATE"] >= start].copy()
else:
    dff = df.copy()

# ==========================================================
# NEW: MONTH/YEAR PERFORMANCE FILTER (independent view)
# ==========================================================
st.sidebar.header("📆 Month/Year Performance")

available_years = sorted(df["YEAR"].dropna().unique().tolist())
month_names = [
    (1, "Jan"), (2, "Feb"), (3, "Mar"), (4, "Apr"), (5, "May"), (6, "Jun"),
    (7, "Jul"), (8, "Aug"), (9, "Sep"), (10, "Oct"), (11, "Nov"), (12, "Dec")
]

if available_years:
    my_year = st.sidebar.selectbox("Year", available_years, index=len(available_years) - 1)
    # months available in that year
    months_in_year = sorted(df[df["YEAR"] == my_year]["MONTH"].dropna().unique().tolist())
    month_options = [(m, dict(month_names).get(m, str(m))) for m in months_in_year] if months_in_year else month_names
    my_month = st.sidebar.selectbox(
        "Month",
        [m for m, _ in month_options],
        format_func=lambda m: dict(month_names).get(m, str(m)),
        index=len(month_options) - 1 if month_options else 0
    )
else:
    my_year, my_month = None, None

# Monthly filtered df is based on FULL data (df), not the 7/30/60/90 filter.
month_df = pd.DataFrame()
if my_year is not None and my_month is not None:
    month_df = df[(df["YEAR"] == my_year) & (df["MONTH"] == my_month)].copy()

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
space["DAYS_SINCE_LAST_SOLD_LOOKBACK"] = (end_dt - space["LAST_SOLD_DATE_LOOKBACK"]).dt.days

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

space_filtered = space[space["TOTAL_UNITS_LOOKBACK"] <= max_units_threshold].copy()
if stale_days > 0:
    space_filtered = space_filtered[space_filtered["DAYS_SINCE_LAST_SOLD_EVER"] >= stale_days].copy()
if stock_min > 0:
    space_filtered = space_filtered[space_filtered["LATEST_STOCK"].fillna(0) >= stock_min].copy()

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
    "LAST_SOLD_DATE_LOOKBACK",
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

st.caption(
    "LATEST_STOCK is the last known stock value in your dataset for that item (forward-filled within item). "
    "If STOCK is missing in the report, LATEST_STOCK will be blank."
)

# ==========================================================
# NEW SECTION: MONTH/YEAR TOP ITEMS (Units + Profit)
# ==========================================================
st.subheader("📆 Month-wise Sales Performance (Pick Month + Year)")

if my_year is None or my_month is None or month_df.empty:
    st.info("Select a Year and Month from the sidebar (📆 Month/Year Performance) to view month-wise results.")
else:
    left, right = st.columns([2, 1])
    with right:
        month_top_n = st.number_input("Top N items (Month/Year)", min_value=10, max_value=500, value=50, step=10)
        month_rank_by = st.selectbox("Rank by", ["Units Sold", "Profit"], index=0)

    month_items = (
        month_df.groupby(["PLU_CODE", "DESCRIPTION"], dropna=False)
                .agg(
                    TOTAL_UNITS=(units_col, "sum"),
                    TOTAL_PROFIT=("PROFIT", "sum"),
                    TOTAL_SALES=("TOTAL_SALES", "sum"),
                    DAYS=("DATE", "nunique"),
                )
                .reset_index()
    )
    month_items["PROFIT_PER_UNIT"] = np.where(
        month_items["TOTAL_UNITS"] > 0,
        month_items["TOTAL_PROFIT"] / month_items["TOTAL_UNITS"],
        0
    )

    if month_rank_by == "Units Sold":
        month_items = month_items.sort_values(["TOTAL_UNITS", "TOTAL_PROFIT"], ascending=[False, False])
    else:
        month_items = month_items.sort_values(["TOTAL_PROFIT", "TOTAL_UNITS"], ascending=[False, False])

    with left:
        month_label = dict([(k, v) for k, v in month_names]).get(my_month, str(my_month))
        st.markdown(f"### Top items for **{month_label} {my_year}**")
        st.dataframe(month_items.head(int(month_top_n)), use_container_width=True, height=420)

    # Quick highlight: #1 item
    if not month_items.empty:
        best = month_items.iloc[0]
        if month_rank_by == "Units Sold":
            st.success(
                f"🏆 Top-selling item in **{month_label} {my_year}** by **Units**: "
                f"**{best['DESCRIPTION']}** (PLU {int(best['PLU_CODE'])}) — Units: {int(best['TOTAL_UNITS'])}"
            )
        else:
            st.success(
                f"🏆 Top item in **{month_label} {my_year}** by **Profit**: "
                f"**{best['DESCRIPTION']}** (PLU {int(best['PLU_CODE'])}) — Profit: {best['TOTAL_PROFIT']:.2f}"
            )

    st.download_button(
        "⬇️ Download Month/Year Top Items (Excel)",
        data=df_to_excel_bytes(month_items, sheet_name="month_top_items"),
        file_name=f"top_items_{my_year}_{my_month:02d}.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )

# ==========================================================
# SECTION 1: TOP SOLD ITEMS (TOTALS) in selected date_window
# ==========================================================
st.subheader("🏆 Top Sold Items (Total Units in selected date range)")

top_items = (
    dff.groupby(["PLU_CODE", "DESCRIPTION"], dropna=False)[units_col]
       .sum()
       .reset_index(name="TOTAL_UNITS")
       .sort_values("TOTAL_UNITS", ascending=False)
)

top_n = st.number_input("Show Top N items (date range)", min_value=10, max_value=500, value=50, step=10)
st.dataframe(top_items.head(int(top_n)), use_container_width=True, height=320)

# ==========================================================
# SECTION 2: MONTH/YEAR TOTALS FOR ALL ITEMS (within date_window)
# ==========================================================
with st.expander("📅 Monthly totals view (All items within selected date range)"):
    years = sorted(dff["YEAR"].unique().tolist())
    if years:
        y = st.selectbox("Year (date range)", years, index=len(years) - 1)
        months = sorted(dff[dff["YEAR"] == y]["MONTH"].unique().tolist())
        m = st.selectbox("Month (date range)", months, index=len(months) - 1) if months else 1

        month_df2 = dff[(dff["YEAR"] == y) & (dff["MONTH"] == m)]
        month_totals = (
            month_df2.groupby(["PLU_CODE", "DESCRIPTION"])[units_col]
                     .sum()
                     .reset_index(name="TOTAL_UNITS_MONTH")
                     .sort_values("TOTAL_UNITS_MONTH", ascending=False)
        )
        st.dataframe(month_totals.head(100), use_container_width=True, height=350)
    else:
        st.info("No dates available after filtering.")

# ==========================================================
# SECTION 3: ITEM SEARCH + ITEM PROFILE (profitability ranking)
# ==========================================================
st.sidebar.header("🔎 Item Search")
query = st.sidebar.text_input("Type item name (min 5 letters)", value="")

selected_item = None
if query and len(query.strip()) >= 5:
    q = query.strip().lower()
    matches = top_items[top_items["DESCRIPTION"].str.lower().str.contains(q, na=False)].copy()
    matches = matches.sort_values("TOTAL_UNITS", ascending=False).head(80)

    if matches.empty:
        st.warning("No matching items found (within your selected date range).")
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
    c2.metric("Days Present", f"{days_present}")
    c3.metric("Avg Units / Day", f"{avg_units_per_day:.2f}")
    c4.metric("Total Profit (sum)", f"{total_profit:.2f}")

    st.markdown("### 💰 Suppliers for this item (Most → Least PROFITABLE)")
    sup_profit = (
        item_df.groupby("SUPPLIER_RESOLVED")
              .agg(
                  TOTAL_UNITS=(units_col, "sum"),
                  TOTAL_PROFIT=("PROFIT", "sum"),
                  TOTAL_SALES=("TOTAL_SALES", "sum"),
                  DAYS=("DATE", "nunique"),
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
    st.success(f"🏅 Most profitable supplier for this item (in current date range): **{best_supplier}**")

    st.markdown("### 📈 Item performance over time")

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
# SECTION 4: TOP ITEMS BY SUPPLIER (UNITS + PROFIT)
# ==========================================================
st.subheader("🏷️ Top Items by Supplier (Units + Profit in selected date range)")

suppliers = sorted(dff["SUPPLIER_RESOLVED"].dropna().unique().tolist())
if not suppliers:
    st.warning("No suppliers found after parsing.")
else:
    supplier_pick = st.selectbox("Select supplier", suppliers)
    supplier_df = dff[dff["SUPPLIER_RESOLVED"] == supplier_pick].copy()

    top_by_supplier = (
        supplier_df.groupby(["PLU_CODE", "DESCRIPTION"])
                  .agg(
                      TOTAL_UNITS=(units_col, "sum"),
                      TOTAL_PROFIT=("PROFIT", "sum"),
                      TOTAL_SALES=("TOTAL_SALES", "sum"),
                      DAYS=("DATE", "nunique")
                  )
                  .reset_index()
                  .sort_values(["TOTAL_UNITS", "TOTAL_PROFIT"], ascending=[False, False])
    )
    top_by_supplier["PROFIT_PER_UNIT"] = np.where(
        top_by_supplier["TOTAL_UNITS"] > 0,
        top_by_supplier["TOTAL_PROFIT"] / top_by_supplier["TOTAL_UNITS"],
        0
    )

    top_k = st.number_input("Show Top K items for selected supplier", min_value=10, max_value=300, value=30, step=10)
    st.dataframe(top_by_supplier.head(int(top_k)), use_container_width=True, height=380)

# ==========================================================
# SECTION 5: FAST / SLOW MOVERS (UNITS)
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
