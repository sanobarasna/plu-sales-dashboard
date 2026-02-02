import streamlit as st
import pandas as pd
import plotly.express as px

# ==================================================
# PAGE CONFIG
# ==================================================
st.set_page_config(
    page_title="PLU Copilot Analytics",
    layout="wide"
)

# ==================================================
# SESSION STATE
# ==================================================
if "df" not in st.session_state:
    st.session_state.df = pd.DataFrame()

if "active_item" not in st.session_state:
    st.session_state.active_item = None

if "date_filter" not in st.session_state:
    st.session_state.date_filter = {"mode": "ALL", "start": None, "end": None}

# ==================================================
# GLOBAL STYLES (COPILOT FEEL)
# ==================================================
st.markdown("""
<style>
input {
    border-radius: 999px !important;
    padding: 14px !important;
    font-size: 16px !important;
}
.card {
    background: #ffffff;
    border-radius: 16px;
    padding: 20px;
    box-shadow: 0 6px 18px rgba(0,0,0,0.06);
    margin-bottom: 16px;
}
[data-testid="stFileUploader"] label {
    display: none;
}
</style>
""", unsafe_allow_html=True)

# ==================================================
# GLOBAL SEARCH BAR
# ==================================================
query = st.text_input(
    "",
    placeholder="Search PLU or item name",
    label_visibility="collapsed"
)

# ==================================================
# EXCEL UPLOAD (GLOBAL DATA CONTEXT)
# ==================================================
uploaded_file = st.file_uploader(
    "Upload Excel",
    type=["xlsx", "xls"],
    label_visibility="collapsed"
)

if uploaded_file:
    try:
        st.session_state.df = pd.read_excel(uploaded_file)
        st.success("Excel file loaded")
    except Exception as e:
        st.error(f"Failed to load file: {e}")

df = st.session_state.df

# ==================================================
# SEARCH → ACTIVE ITEM CONTEXT
# ==================================================
if query and not df.empty:
    matches = df[
        df["DESCRIPTION"].astype(str).str.contains(query, case=False, na=False) |
        df["PLU_CODE"].astype(str).str.endswith(query)
    ][["PLU_CODE", "DESCRIPTION"]].drop_duplicates()

    if len(matches) == 1:
        st.session_state.active_item = matches.iloc[0].to_dict()

    elif len(matches) > 1:
        selected = st.selectbox(
            "Select item",
            matches.apply(
                lambda x: f"{x['DESCRIPTION']} ({x['PLU_CODE']})",
                axis=1
            )
        )
        idx = matches.index[
            matches.apply(
                lambda x: f"{x['DESCRIPTION']} ({x['PLU_CODE']})" == selected,
                axis=1
            )
        ][0]
        st.session_state.active_item = matches.loc[idx].to_dict()

# ==================================================
# CARD HELPERS
# ==================================================
def card(title, render_fn):
    st.markdown(f"<div class='card'><h4>{title}</h4>", unsafe_allow_html=True)
    render_fn()
    st.markdown("</div>", unsafe_allow_html=True)

def get_filtered_item_df():
    ctx = st.session_state.active_item
    if not ctx:
        return pd.DataFrame()

    dff = df[df["PLU_CODE"] == ctx["PLU_CODE"]]

    f = st.session_state.date_filter
    if f["mode"] == "LAST_30":
        dff = dff[dff["DATE"] >= dff["DATE"].max() - pd.Timedelta(days=30)]
    elif f["mode"] == "CUSTOM":
        dff = dff[
            (dff["DATE"] >= pd.Timestamp(f["start"])) &
            (dff["DATE"] <= pd.Timestamp(f["end"]))
        ]

    return dff

# ==================================================
# CARD IMPLEMENTATIONS
# ==================================================
def item_performance_card():
    dff = get_filtered_item_df()
    if dff.empty:
        st.info("Search for an item")
        return

    c1, c2, c3 = st.columns(3)
    c1.metric("Units Sold", int(dff["USAGE_SOLD"].sum()))
    c2.metric("Total Profit", f"${dff['PROFIT'].sum():,.0f}")
    c3.metric("Active Days", dff["DATE"].nunique())

def supplier_card():
    dff = get_filtered_item_df()
    if dff.empty:
        return

    metric = st.radio("Rank by", ["Profit", "Units"], horizontal=True)

    sup = dff.groupby("SUPPLIER_RESOLVED").agg(
        Units=("USAGE_SOLD", "sum"),
        Profit=("PROFIT", "sum")
    ).reset_index()

    sup = sup.sort_values(
        "Profit" if metric == "Profit" else "Units",
        ascending=False
    )

    st.dataframe(sup, use_container_width=True, height=250)

def date_range_card():
    mode = st.radio(
        "Time Range",
        ["All Time", "Last 30 Days", "Custom"],
        horizontal=True
    )

    if mode == "All Time":
        st.session_state.date_filter = {"mode": "ALL", "start": None, "end": None}
    elif mode == "Last 30 Days":
        st.session_state.date_filter = {"mode": "LAST_30", "start": None, "end": None}
    else:
        c1, c2 = st.columns(2)
        start = c1.date_input("Start date")
        end = c2.date_input("End date")
        st.session_state.date_filter = {
            "mode": "CUSTOM",
            "start": start,
            "end": end
        }

def trend_card():
    dff = get_filtered_item_df()
    if dff.empty:
        return

    trend = (
        dff.groupby(dff["DATE"].dt.to_period("M"))
           .agg(Units=("USAGE_SOLD", "sum"))
           .reset_index()
    )

    fig = px.line(trend, x="DATE", y="Units")
    st.plotly_chart(fig, use_container_width=True)

def insights_card():
    dff = get_filtered_item_df()
    if dff.empty:
        return

    if dff["PROFIT"].sum() < 0:
        st.error("Item is unprofitable")

    if dff["USAGE_SOLD"].sum() < 10:
        st.warning("Low movement detected")

# ==================================================
# CARD REGISTRY (CUSTOMIZABLE)
# ==================================================
CARDS = {
    "Item Performance": item_performance_card,
    "Suppliers": supplier_card,
    "Date Range": date_range_card,
    "Trends": trend_card,
    "Insights": insights_card,
}

selected_cards = st.multiselect(
    "Customize cards",
    list(CARDS.keys()),
    default=["Item Performance", "Suppliers", "Trends"]
)

# ==================================================
# COPILOT CANVAS LAYOUT
# ==================================================
left, right = st.columns([2, 1])

for i, card_name in enumerate(selected_cards):
    target = left if i % 2 == 0 else right
    with target:
        card(card_name, CARDS[card_name])
