# ==========================================================
# PLU Sales + Supplier Profitability Dashboard
# Modern Card-Based Layout (Edge-style)
# ==========================================================

import re
import streamlit as st
import pandas as pd
import numpy as np
from io import BytesIO
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime

# -----------------------
# CONFIG
# -----------------------
st.set_page_config(page_title="PLU Sales Dashboard", layout="wide", initial_sidebar_state="collapsed")

# Custom CSS for card-based layout
st.markdown("""
<style>
    /* Main container */
    .main > div {
        padding-top: 2rem;
    }
    
    /* Search bar styling */
    .search-container {
        background: white;
        padding: 2rem;
        border-radius: 16px;
        box-shadow: 0 2px 8px rgba(0,0,0,0.1);
        margin-bottom: 2rem;
    }
    
    /* Metric cards */
    .metric-card {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        padding: 1.5rem;
        border-radius: 12px;
        color: white;
        box-shadow: 0 4px 12px rgba(0,0,0,0.15);
        transition: transform 0.2s;
    }
    
    .metric-card:hover {
        transform: translateY(-4px);
    }
    
    .metric-card.green {
        background: linear-gradient(135deg, #11998e 0%, #38ef7d 100%);
    }
    
    .metric-card.red {
        background: linear-gradient(135deg, #eb3349 0%, #f45c43 100%);
    }
    
    .metric-card.blue {
        background: linear-gradient(135deg, #4facfe 0%, #00f2fe 100%);
    }
    
    .metric-card.orange {
        background: linear-gradient(135deg, #fa709a 0%, #fee140 100%);
    }
    
    /* Make metric values smaller and more elegant */
    [data-testid="stMetricValue"] {
        font-size: 1.8rem !important;
        font-weight: 600 !important;
    }
    
    [data-testid="stMetricLabel"] {
        font-size: 0.85rem !important;
        font-weight: 500 !important;
        opacity: 0.9;
    }
    
    [data-testid="stMetricDelta"] {
        font-size: 0.75rem !important;
    }
    
    /* Section cards */
    .section-card {
        background: white;
        padding: 1.5rem;
        border-radius: 12px;
        box-shadow: 0 2px 8px rgba(0,0,0,0.1);
        margin-bottom: 1rem;
        border-left: 4px solid #667eea;
    }
    
    .section-card.warning {
        border-left-color: #f45c43;
    }
    
    .section-card.success {
        border-left-color: #38ef7d;
    }
    
    .section-card.info {
        border-left-color: #4facfe;
    }
    
    /* Hide streamlit branding */
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    
    /* Improve spacing */
    .block-container {
        padding-top: 1rem;
        padding-bottom: 1rem;
    }
    
    h1 {
        font-size: 2.5rem;
        font-weight: 700;
        margin-bottom: 0.5rem;
    }
    
    h2 {
        font-size: 1.8rem;
        font-weight: 600;
        margin-top: 1.5rem;
        margin-bottom: 1rem;
    }
    
    h3 {
        font-size: 1.4rem;
        font-weight: 600;
        margin-top: 1rem;
    }
</style>
""", unsafe_allow_html=True)

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

def search_items_by_plu_or_name(df: pd.DataFrame, query: str) -> pd.DataFrame:
    """
    Search items by last 5-7 digits of PLU code OR by partial name match.
    Returns dataframe with matching items.
    """
    query = query.strip()
    if not query:
        return pd.DataFrame()
    
    # Check if query is numeric (PLU search by last digits)
    if query.isdigit():
        # Search by last 5-7 digits of PLU
        query_len = len(query)
        if query_len >= 5:
            matches = df[df["PLU_CODE"].astype(str).str.endswith(query)].copy()
            if not matches.empty:
                return matches[["PLU_CODE", "DESCRIPTION"]].drop_duplicates()
    
    # Search by name (partial match, case insensitive)
    if len(query) >= 3:
        matches = df[df["DESCRIPTION"].str.lower().str.contains(query.lower(), na=False)].copy()
        if not matches.empty:
            return matches[["PLU_CODE", "DESCRIPTION"]].drop_duplicates()
    
    return pd.DataFrame()

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
st.title("📊 PLU Sales & Profitability Dashboard")

plu_file = st.file_uploader("📁 Upload your PLU report (.xlsx)", type=["xlsx"], label_visibility="collapsed")
if plu_file is None:
    st.info("👆 Upload your PLU Excel file to get started")
    st.stop()

try:
    df = load_plu_report_clean(plu_file, sheet_name=None)
except Exception as e:
    st.error(f"❌ Failed to load file: {e}")
    st.stop()

# Global settings
use_net_units = st.sidebar.checkbox("Use NET units (include negatives)", value=False)
units_col = "USAGE_NET" if use_net_units else "USAGE_SOLD"

max_date = df["DATE"].max()
min_date = df["DATE"].min()

# ==========================================================
# LARGE SEARCH BAR (Edge-style)
# ==========================================================
st.markdown("### 🔍 Search by PLU or Item Name")
search_query = st.text_input(
    "Search",
    value="",
    key="main_search",
    placeholder="Enter last 5-7 digits of PLU or item name (min 3 letters)...",
    label_visibility="collapsed"
)

# Show search results if query exists
if search_query and len(search_query.strip()) >= 3:
    search_results = search_items_by_plu_or_name(df, search_query)
    
    if not search_results.empty:
        if len(search_results) > 1:
            options = [
                f"{row['DESCRIPTION']} (PLU: {int(row['PLU_CODE'])})"
                for _, row in search_results.iterrows()
            ]
            selected = st.selectbox("📋 Select an item:", options, key="item_select")
            selected_idx = options.index(selected)
            selected_plu = int(search_results.iloc[selected_idx]["PLU_CODE"])
            selected_desc = search_results.iloc[selected_idx]["DESCRIPTION"]
        else:
            selected_plu = int(search_results.iloc[0]["PLU_CODE"])
            selected_desc = search_results.iloc[0]["DESCRIPTION"]
            st.success(f"✅ Found: **{selected_desc}** (PLU: {selected_plu})")
        
        # Show item performance
        item_data = df[(df["PLU_CODE"] == selected_plu) & (df["DESCRIPTION"] == selected_desc)].copy()
        
        if not item_data.empty:
            use_custom_range = st.toggle("📅 Use custom date range", value=False, key="custom_range")
            
            if use_custom_range:
                col1, col2 = st.columns(2)
                with col1:
                    custom_start = st.date_input("Start", value=min_date.date(), min_value=min_date.date(), max_value=max_date.date(), key="lookup_start")
                with col2:
                    custom_end = st.date_input("End", value=max_date.date(), min_value=min_date.date(), max_value=max_date.date(), key="lookup_end")
                
                cs, ce = clamp_date_range(pd.Timestamp(custom_start), pd.Timestamp(custom_end))
                item_data = item_data[(item_data["DATE"] >= cs) & (item_data["DATE"] <= ce)].copy()
                date_range_label = f"{cs.date()} to {ce.date()}"
            else:
                date_range_label = "All Time"
            
            # Metrics
            category = item_data["CATEGORY"].mode()[0] if not item_data["CATEGORY"].mode().empty else "N/A"
            stock_info = latest_stock_per_item(df[df["PLU_CODE"] == selected_plu])
            current_stock = stock_info.iloc[0]["LATEST_STOCK"] if not stock_info.empty else 0
            
            total_units = float(item_data[units_col].sum())
            total_profit = float(item_data["PROFIT"].sum())
            days_active = int(item_data["DATE"].nunique())
            avg_units_per_day = total_units / days_active if days_active > 0 else 0
            first_sale = item_data["DATE"].min()
            last_sale = item_data["DATE"].max()
            
            st.markdown("---")
            st.markdown(f"## 📊 {selected_desc}")
            st.caption(f"**Period:** {date_range_label} | **PLU:** {selected_plu} | **Category:** {category}")
            
            # Metrics row
            m1, m2, m3, m4, m5 = st.columns(5)
            m1.metric("Total Units", f"{int(total_units):,}")
            m2.metric("Total Profit", f"${total_profit:,.2f}")
            m3.metric("Current Stock", f"{int(current_stock) if not pd.isna(current_stock) else 0}")
            m4.metric("Avg Units/Day", f"{avg_units_per_day:.2f}")
            m5.metric("Days Active", f"{days_active}")
            
            # Show ALL suppliers from GROUP field
            st.markdown("### 📦 All Suppliers for This Item")
            all_suppliers_for_item = set()
            for suppliers_list in item_data["SUPPLIERS_LIST"].dropna():
                if isinstance(suppliers_list, list):
                    all_suppliers_for_item.update(suppliers_list)
            
            if all_suppliers_for_item:
                suppliers_display = ", ".join(sorted(all_suppliers_for_item))
                st.info(f"**Suppliers:** {suppliers_display}")
            else:
                st.warning("No suppliers found in GROUP field for this item")
            
            # Supplier comparison - Get ALL suppliers from SUPPLIERS_LIST
            st.markdown("### 💰 Supplier Performance")
            
            # Explode SUPPLIERS_LIST to get all suppliers for this item
            item_suppliers_expanded = item_data.copy()
            item_suppliers_expanded = item_suppliers_expanded[item_suppliers_expanded["SUPPLIERS_LIST"].apply(lambda x: isinstance(x, list) and len(x) > 0)]
            
            if not item_suppliers_expanded.empty:
                # Explode the suppliers list to create one row per supplier
                item_suppliers_expanded = item_suppliers_expanded.explode("SUPPLIERS_LIST")
                item_suppliers_expanded = item_suppliers_expanded[item_suppliers_expanded["SUPPLIERS_LIST"].notna()]
                
                supplier_stats = (
                    item_suppliers_expanded.groupby("SUPPLIERS_LIST", dropna=False)
                            .agg(UNITS=(units_col, "sum"), PROFIT=("PROFIT", "sum"), SALES=("TOTAL_SALES", "sum"))
                            .reset_index()
                            .rename(columns={"SUPPLIERS_LIST": "SUPPLIER"})
                )
                supplier_stats["PROFIT_PER_UNIT"] = np.where(supplier_stats["UNITS"] > 0, supplier_stats["PROFIT"] / supplier_stats["UNITS"], 0).round(4)
                supplier_stats = supplier_stats.sort_values("PROFIT", ascending=False).reset_index(drop=True)
            else:
                # Fallback to SUPPLIER_RESOLVED if no SUPPLIERS_LIST available
                supplier_stats = (
                    item_data.groupby("SUPPLIER_RESOLVED", dropna=False)
                            .agg(UNITS=(units_col, "sum"), PROFIT=("PROFIT", "sum"), SALES=("TOTAL_SALES", "sum"))
                            .reset_index()
                            .rename(columns={"SUPPLIER_RESOLVED": "SUPPLIER"})
                )
                supplier_stats["PROFIT_PER_UNIT"] = np.where(supplier_stats["UNITS"] > 0, supplier_stats["PROFIT"] / supplier_stats["UNITS"], 0).round(4)
                supplier_stats = supplier_stats.sort_values("PROFIT", ascending=False).reset_index(drop=True)
            
            if not supplier_stats.empty:
                best = supplier_stats.iloc[0]
                worst = supplier_stats.iloc[-1]
                
                c1, c2 = st.columns(2)
                with c1:
                    st.success(f"🏆 **Best:** {best['SUPPLIER']} | Profit: ${best['PROFIT']:,.2f} | Profit/Unit: ${best['PROFIT_PER_UNIT']:.2f}")
                with c2:
                    if worst['PROFIT'] < 0:
                        st.error(f"🚨 **Worst:** {worst['SUPPLIER']} | Profit: ${worst['PROFIT']:,.2f} | Profit/Unit: ${worst['PROFIT_PER_UNIT']:.2f}")
                    else:
                        st.warning(f"⚠️ **Worst:** {worst['SUPPLIER']} | Profit: ${worst['PROFIT']:,.2f} | Profit/Unit: ${worst['PROFIT_PER_UNIT']:.2f}")
                
                st.dataframe(supplier_stats, use_container_width=True, height=200)
            
            # Charts
            item_data["YEAR_MONTH"] = item_data["DATE"].dt.to_period("M").astype(str)
            monthly = item_data.groupby("YEAR_MONTH").agg(UNITS=(units_col, "sum"), PROFIT=("PROFIT", "sum")).reset_index().sort_values("YEAR_MONTH")
            
            if not monthly.empty:
                ch1, ch2 = st.columns(2)
                with ch1:
                    fig = px.bar(monthly, x="YEAR_MONTH", y="UNITS", title="Monthly Units")
                    st.plotly_chart(fig, use_container_width=True)
                with ch2:
                    fig = px.bar(monthly, x="YEAR_MONTH", y="PROFIT", title="Monthly Profit", color="PROFIT", color_continuous_scale=["red", "yellow", "green"])
                    st.plotly_chart(fig, use_container_width=True)
            
            st.markdown("---")

# ==========================================================
# QUICK METRICS CARDS
# ==========================================================
st.markdown("## 📈 Quick Insights")

# Calculate quick metrics (last 30 days)
last_30_days = max_date - pd.Timedelta(days=30)
recent_df = df[df["DATE"] >= last_30_days].copy()

total_profit_30d = float(recent_df["PROFIT"].sum())
total_units_30d = float(recent_df[units_col].sum())
top_item_30d = recent_df.groupby("DESCRIPTION")[units_col].sum().idxmax() if not recent_df.empty else "N/A"
low_stock_count = len(latest_stock_per_item(df)[latest_stock_per_item(df)["LATEST_STOCK"] < 10])

# Truncate top seller name if too long
top_item_display = top_item_30d if len(str(top_item_30d)) < 20 else str(top_item_30d)[:17] + "..."

metric1, metric2, metric3, metric4 = st.columns(4)
with metric1:
    st.metric("💰 Profit (30d)", f"${total_profit_30d:,.0f}")
with metric2:
    st.metric("📦 Units Sold (30d)", f"{int(total_units_30d):,}")
with metric3:
    st.metric("🏆 Top Seller (30d)", top_item_display)
with metric4:
    st.metric("⚠️ Low Stock Items", f"{low_stock_count}")

st.markdown("---")

# ==========================================================
# CARD GRID LAYOUT
# ==========================================================
st.markdown("## 📋 Dashboard Sections")

# Create tabs for better organization
tab1, tab2, tab3, tab4 = st.tabs(["🧱 Space Occupiers", "🏆 Top Items", "🚀 Fast/Slow Movers", "📊 Advanced Analytics"])

# ==========================================================
# TAB 1: SPACE OCCUPIERS
# ==========================================================
with tab1:
    st.markdown("### 🧱 Space Occupiers (Low-selling items)")
    
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        low_lookback_days = st.selectbox("Lookback window", [30, 60, 90, 180, 365], index=2, key="space_lookback")
    with col2:
        max_units_threshold = st.number_input("Max units", 0, 500, 5, 1, key="space_max_units")
    with col3:
        stale_days = st.number_input("Not sold in (days)", 0, 3650, 30, 1, key="space_stale")
    with col4:
        stock_min = st.number_input("Min stock to show", 0, 10_000, 0, 1, key="space_stock_min")
    
    end_dt = df["DATE"].max()
    start_dt = end_dt - pd.Timedelta(days=low_lookback_days - 1)
    lookback_df = df[(df["DATE"] >= start_dt) & (df["DATE"] <= end_dt)].copy()
    
    lookback_item = lookback_df.groupby(["PLU_CODE", "DESCRIPTION"], dropna=False).agg(
        TOTAL_UNITS_LOOKBACK=(units_col, "sum"),
        TOTAL_PROFIT_LOOKBACK=("PROFIT", "sum"),
        TOTAL_SALES_LOOKBACK=("TOTAL_SALES", "sum"),
        LAST_SOLD_DATE_LOOKBACK=("DATE", "max"),
    ).reset_index()
    
    ever_item = df.groupby(["PLU_CODE", "DESCRIPTION"], dropna=False).agg(
        FIRST_SOLD_DATE=("DATE", "min"),
        LAST_SOLD_DATE_EVER=("DATE", "max"),
    ).reset_index()
    
    best_supplier_profit = lookback_df.groupby(["PLU_CODE", "DESCRIPTION", "SUPPLIER_RESOLVED"], dropna=False)["PROFIT"].sum().reset_index(name="SUPPLIER_PROFIT_LOOKBACK")
    best_supplier_profit = best_supplier_profit.sort_values(["PLU_CODE", "DESCRIPTION", "SUPPLIER_PROFIT_LOOKBACK"], ascending=[True, True, False]).drop_duplicates(["PLU_CODE", "DESCRIPTION"]).rename(columns={"SUPPLIER_RESOLVED": "BEST_SUPPLIER_BY_PROFIT"})[["PLU_CODE", "DESCRIPTION", "BEST_SUPPLIER_BY_PROFIT", "SUPPLIER_PROFIT_LOOKBACK"]]
    
    cat_mode = df.dropna(subset=["CATEGORY"]).groupby(["PLU_CODE", "DESCRIPTION"])["CATEGORY"].agg(lambda x: x.value_counts().index[0]).reset_index()
    stock_latest = latest_stock_per_item(df)
    
    space = lookback_item.merge(ever_item, on=["PLU_CODE", "DESCRIPTION"], how="left").merge(best_supplier_profit, on=["PLU_CODE", "DESCRIPTION"], how="left").merge(cat_mode, on=["PLU_CODE", "DESCRIPTION"], how="left").merge(stock_latest, on=["PLU_CODE", "DESCRIPTION"], how="left")
    
    space["DAYS_SINCE_LAST_SOLD_EVER"] = (end_dt - space["LAST_SOLD_DATE_EVER"]).dt.days
    
    space["LOSS_%"] = np.nan
    sales_ok = space["TOTAL_SALES_LOOKBACK"].fillna(0) > 0
    neg_profit = space["TOTAL_PROFIT_LOOKBACK"] < 0
    space.loc[sales_ok & neg_profit, "LOSS_%"] = ((-space.loc[sales_ok & neg_profit, "TOTAL_PROFIT_LOOKBACK"] / space.loc[sales_ok & neg_profit, "TOTAL_SALES_LOOKBACK"]) * 100).round(2)
    fallback_mask = (~sales_ok) & neg_profit
    space.loc[fallback_mask, "LOSS_%"] = ((-space.loc[fallback_mask, "TOTAL_PROFIT_LOOKBACK"] / (space.loc[fallback_mask, "TOTAL_PROFIT_LOOKBACK"].abs() + 1)) * 100).round(2)
    
    space_filtered = space[space["TOTAL_UNITS_LOOKBACK"] <= max_units_threshold].copy()
    if stale_days > 0:
        space_filtered = space_filtered[space_filtered["DAYS_SINCE_LAST_SOLD_EVER"] >= stale_days].copy()
    if stock_min > 0:
        space_filtered = space_filtered[space_filtered["LATEST_STOCK"].fillna(0) >= stock_min].copy()
    
    space_filtered = space_filtered.sort_values(["TOTAL_UNITS_LOOKBACK", "DAYS_SINCE_LAST_SOLD_EVER", "LATEST_STOCK", "LOSS_%"], ascending=[True, False, False, False]).reset_index(drop=True)
    
    show_cols = ["PLU_CODE", "DESCRIPTION", "CATEGORY", "BEST_SUPPLIER_BY_PROFIT", "TOTAL_UNITS_LOOKBACK", "LATEST_STOCK", "STOCK_ASOF_DATE", "FIRST_SOLD_DATE", "LAST_SOLD_DATE_EVER", "DAYS_SINCE_LAST_SOLD_EVER", "TOTAL_PROFIT_LOOKBACK", "TOTAL_SALES_LOOKBACK", "LOSS_%"]
    
    st.info(f"📅 Period: **{start_dt.date()} → {end_dt.date()}** | Items: **{len(space_filtered)}**")
    st.dataframe(space_filtered[show_cols], use_container_width=True, height=400)
    
    st.download_button("⬇️ Download Excel", data=df_to_excel_bytes(space_filtered[show_cols], sheet_name="space_occupiers"), file_name=f"space_occupiers_{low_lookback_days}d.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

# ==========================================================
# TAB 2: TOP ITEMS
# ==========================================================
with tab2:
    st.markdown("### 🏆 Top Items Performance")
    
    t1, t2, t3 = st.columns(3)
    with t1:
        top_start = st.date_input("Start", value=(max_date - pd.Timedelta(days=30)).date(), min_value=min_date.date(), max_value=max_date.date(), key="top_start")
    with t2:
        top_end = st.date_input("End", value=max_date.date(), min_value=min_date.date(), max_value=max_date.date(), key="top_end")
    with t3:
        top_rank_by = st.selectbox("Rank by", ["TOTAL_UNITS", "TOTAL_PROFIT"], index=0, key="top_rank")
    
    t4, t5, t6 = st.columns(3)
    with t4:
        categories_raw = df["CATEGORY"].dropna().unique().tolist()
        categories_sorted = sorted([c for c in categories_raw if str(c).strip() != ""])
        categories_all = ["All Categories", "No Category"] + categories_sorted
        category_filter = st.selectbox("Category", categories_all, index=0, key="top_category")
    with t5:
        suppliers_all = ["All Suppliers"] + sorted(df["SUPPLIER_RESOLVED"].dropna().unique().tolist())
        supplier_filter = st.selectbox("Supplier", suppliers_all, index=0, key="top_supplier")
    with t6:
        top_n = st.number_input("Top N", min_value=10, max_value=5000, value=50, step=10, key="top_n")
    
    top_search = st.text_input("Search item (optional)", value="", key="top_search", placeholder="Min 3 letters...")
    breakdown = st.toggle("Breakdown by supplier", value=False, key="top_breakdown")
    
    ts, te = clamp_date_range(pd.Timestamp(top_start), pd.Timestamp(top_end))
    top_df = df[(df["DATE"] >= ts) & (df["DATE"] <= te)].copy()
    
    if category_filter == "No Category":
        top_df = top_df[top_df["CATEGORY"].isna()].copy()
    elif category_filter != "All Categories":
        top_df = top_df[top_df["CATEGORY"] == category_filter].copy()
    
    if supplier_filter != "All Suppliers":
        top_df = top_df[top_df["SUPPLIER_RESOLVED"] == supplier_filter].copy()
    
    s = (top_search or "").strip().lower()
    if len(s) >= 3:
        top_df = top_df[top_df["DESCRIPTION"].str.lower().str.contains(s, na=False)].copy()
    
    if top_df.empty:
        st.warning("No data found for selected filters")
    else:
        sort_col = "TOTAL_UNITS" if top_rank_by == "TOTAL_UNITS" else "TOTAL_PROFIT"
        
        if not breakdown:
            top_items_df = top_df.groupby(["PLU_CODE", "DESCRIPTION"], dropna=False).agg(TOTAL_UNITS=(units_col, "sum"), TOTAL_PROFIT=("PROFIT", "sum"), _ACTIVE_DAYS=("DATE", "nunique")).reset_index()
            top_items_df["TOTAL_UNITS_PER_DAY"] = np.where(top_items_df["_ACTIVE_DAYS"] > 0, top_items_df["TOTAL_UNITS"] / top_items_df["_ACTIVE_DAYS"], 0).round(3)
            top_items_df = top_items_df.drop(columns=["_ACTIVE_DAYS"]).sort_values(sort_col, ascending=False).reset_index(drop=True)
            
            st.info(f"📅 **{ts.date()} → {te.date()}** | Category: **{category_filter}** | Supplier: **{supplier_filter}** | Items: **{len(top_items_df)}**")
            st.dataframe(top_items_df.head(int(top_n)), use_container_width=True, height=400)
            st.download_button("⬇️ Download Excel", data=df_to_excel_bytes(top_items_df, sheet_name="top_items"), file_name=f"top_items_{ts.date()}_to_{te.date()}.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
        else:
            top_items_sup = top_df.groupby(["PLU_CODE", "DESCRIPTION", "SUPPLIER_RESOLVED"], dropna=False).agg(TOTAL_UNITS=(units_col, "sum"), TOTAL_PROFIT=("PROFIT", "sum"), TOTAL_SALES=("TOTAL_SALES", "sum"), _ACTIVE_DAYS=("DATE", "nunique")).reset_index()
            top_items_sup["TOTAL_UNITS_PER_DAY"] = np.where(top_items_sup["_ACTIVE_DAYS"] > 0, top_items_sup["TOTAL_UNITS"] / top_items_sup["_ACTIVE_DAYS"], 0).round(3)
            top_items_sup["PROFIT_PER_UNIT"] = np.where(top_items_sup["TOTAL_UNITS"] > 0, top_items_sup["TOTAL_PROFIT"] / top_items_sup["TOTAL_UNITS"], 0).round(4)
            top_items_sup = top_items_sup.drop(columns=["_ACTIVE_DAYS"]).sort_values([sort_col, "TOTAL_UNITS"], ascending=[False, False]).reset_index(drop=True)
            
            st.info(f"📅 **{ts.date()} → {te.date()}** | Category: **{category_filter}** | Supplier: **{supplier_filter}** | Rows: **{len(top_items_sup)}**")
            st.dataframe(top_items_sup.head(int(top_n)), use_container_width=True, height=400)
            st.download_button("⬇️ Download Excel", data=df_to_excel_bytes(top_items_sup, sheet_name="top_items_supplier"), file_name=f"top_items_supplier_{ts.date()}_to_{te.date()}.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

# ==========================================================
# TAB 3: FAST/SLOW MOVERS
# ==========================================================
with tab3:
    st.markdown("### 🚀 Fast Movers / 🐢 Slow Movers")
    
    date_window = st.selectbox("Quick date range", ["Last 7 days", "Last 30 days", "Last 60 days", "Last 90 days"], index=1, key="movers_window")
    days = int(date_window.split()[1])
    start = max_date - pd.Timedelta(days=days - 1)
    dff = df[df["DATE"] >= start].copy()
    
    compare_days = st.selectbox("Comparison window (days)", [7, 14, 30, 60], index=2, key="movers_compare")
    
    end_recent = dff["DATE"].max()
    start_recent = end_recent - pd.Timedelta(days=compare_days - 1)
    start_prev = start_recent - pd.Timedelta(days=compare_days)
    end_prev = start_recent - pd.Timedelta(days=1)
    
    recent = dff[(dff["DATE"] >= start_recent) & (dff["DATE"] <= end_recent)]
    prev = dff[(dff["DATE"] >= start_prev) & (dff["DATE"] <= end_prev)]
    
    recent_sum = recent.groupby(["PLU_CODE", "DESCRIPTION"])[units_col].sum().reset_index(name="RECENT_UNITS")
    prev_sum = prev.groupby(["PLU_CODE", "DESCRIPTION"])[units_col].sum().reset_index(name="PREV_UNITS")
    
    movers = recent_sum.merge(prev_sum, on=["PLU_CODE", "DESCRIPTION"], how="outer").fillna(0)
    
    col1, col2, col3 = st.columns(3)
    with col1:
        min_units = st.number_input("Min units (either period)", min_value=1, max_value=500, value=10, step=1, key="movers_min")
    with col2:
        fast_thresh = st.number_input("Fast mover threshold (%)", min_value=5, max_value=1000, value=50, step=5, key="movers_fast")
    with col3:
        slow_thresh = st.number_input("Slow mover threshold (%)", min_value=5, max_value=1000, value=30, step=5, key="movers_slow")
    
    movers = movers[(movers["RECENT_UNITS"] >= min_units) | (movers["PREV_UNITS"] >= min_units)].copy()
    movers["DELTA"] = movers["RECENT_UNITS"] - movers["PREV_UNITS"]
    movers["PCT_CHANGE"] = (movers["DELTA"] / movers["PREV_UNITS"].replace(0, 1) * 100).round(2)
    
    fast = movers[movers["PCT_CHANGE"] >= fast_thresh].sort_values(["PCT_CHANGE", "RECENT_UNITS"], ascending=[False, False]).reset_index(drop=True)
    slow = movers[movers["PCT_CHANGE"] <= -slow_thresh].sort_values(["PCT_CHANGE", "RECENT_UNITS"], ascending=[True, False]).reset_index(drop=True)
    
    c1, c2 = st.columns(2)
    with c1:
        st.markdown("#### 🚀 Fast Movers")
        st.dataframe(fast.head(50), use_container_width=True, height=400)
    with c2:
        st.markdown("#### 🐢 Slow Movers")
        st.dataframe(slow.head(50), use_container_width=True, height=400)
    
    st.caption(f"Recent: **{start_recent.date()} → {end_recent.date()}** | Previous: **{start_prev.date()} → {end_prev.date()}**")

# ==========================================================
# TAB 4: ADVANCED ANALYTICS
# ==========================================================
with tab4:
    st.markdown("### 📊 Advanced Analytics")
    st.info("🚧 Coming soon: Category trends, Supplier comparisons, Predictive analytics")
