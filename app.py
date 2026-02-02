import streamlit as st
import pandas as pd
import plotly.express as px

# --------------------------------------------------
# PAGE CONFIG
# --------------------------------------------------
st.set_page_config(
    page_title="PLU Copilot Analytics",
    layout="wide"
)

# --------------------------------------------------
# SESSION STATE (GLOBAL CONTEXT)
# --------------------------------------------------
if "active_item" not in st.session_state:
    st.session_state.active_item = None

if "date_filter" not in st.session_state:
    st.session_state.date_filter = {"mode": "ALL", "start": None, "end": None}

# --------------------------------------------------
# MOCK DATA LOADER (REPLACE WITH YOUR CLEAN DF)
# --------------------------------------------------
@st.cache_data
def load_data():
    # replace with your load_plu_report_clean(...)
    return pd.DataFrame()

df = load_data()

# --------------------------------------------------
# GLOBAL SEARCH BAR (COPILOT STYLE)
# --------------------------------------------------
st.markdown("""
<style>
.copilot-search input {
    border-radius: 999px !important;
    padding: 14px !important;
    font-size: 16px !important;
}
.card {
    background: white;
    border-radius: 16px;
    padding: 20px;
    box-shadow: 0 6px 18px rgba(0,0,0,0.06);
    margin-bottom: 16px;
}
</style>
""", unsafe_allow_html=True)

with st.container():
    query = st.text_input(
        "",
        placeholder="Search PLU or item name",
        label_visibility="collapsed",
        key="global_search"
    )

    if query and not df.empty:
        matches = df[
            df["DESCRIPTION"].str.contains(query, case=False, na=False) |
            df["PLU_CODE"].astype(str).str.endswith(query)
        ][["PLU_CODE", "DESCRIPTION"]].drop_duplicates()

        if len(matches) == 1:
            st.session_state.active_item = matches.iloc[0].to_dict()
        elif len(matches) > 1:
            option = st.selectbox(
                "Select item",
                matches.apply(lambda x: f"{x['DESCRIPTION']} ({x['PLU_CODE']})", axis=1)
            )
            idx = matches.index[matches.apply(
                lambda x: f"{x['DESCRIPTION']} ({x['PLU_CODE']})" == option, axis=1
            )][0]
            st.session_state.active_item = matches.loc[idx].to_dict()

# --------------------------------------------------
# CARD HELPERS
# --------------------------------------------------
def card(title, body_fn):
    st.markdown(f"<div class='card'><h4>{title}</h4>", unsafe_allow_html=True)
    body_fn()
    st.markdown("</div>", unsafe_allow_html=True)

def filtered_item_df(df):
    ctx = st.session_state.active_item
    if not ctx:
        return pd.DataFrame()

    dff = df[df["PLU_CODE"] == ctx["PLU_CODE"]]

    f = st.session_state.date_filter
    if f["mode"] == "CUSTOM":
        dff = dff[
            (dff["DATE"] >= pd.Timestamp(f["start"])) &
            (dff["DATE"] <= pd.Timestamp(f["end"]))
        ]
    elif f["mode"] == "LAST_30":
        dff = dff[dff["DATE"] >= dff["DATE"].max() - pd.Timedelta(days=30)]

    return dff

# --------------------------------------------------
# CARD DEFINITIONS
# --------------------------------------------------
def item_performance_card():
    dff = filtered_item_df(df)
    if dff.empty:
        st.info("Search for an item to view performance")
        return

    c1, c2, c3 = st.columns(3)
    c1.metric("Units Sold", int(dff["USAGE_SOLD"].sum()))
    c2.metric("Total Profit", f"${dff['PROFIT'].sum():,.0f}")
    c3.metric("Active Days", dff["DATE"].nunique())

def supplier_card():
    dff = filtered_item_df(df)
    if dff.empty:
        return

    metric = st.radio("Rank by", ["Profit", "Units"], horizontal=True)

    sup = dff.groupby("SUPPLIER_RESOLVED").agg(
        UNITS=("USAGE_SOLD", "sum"),
        PROFIT=("PROFIT", "sum")
    ).reset_index()

    sup = sup.sort_values("PROFIT" if metric == "Profit" else "UNITS", ascending=False)
    st.dataframe(sup, use_container_width=True, height=240)

def date_control_card():
    mode = st.radio(
        "Window",
        ["All Time", "Last 30 Days", "Custom"],
        horizontal=True
    )

    if mode == "All Time":
        st.session_state.date_filter = {"mode": "ALL", "start": None, "end": None}
    elif mode == "Last 30 Days":
        st.session_state.date_filter = {"mode": "LAST_30", "start": None, "end": None}
    else:
        c1, c2 = st.columns(2)
        start = c1.date_input("Start")
        end = c2.date_input("End")
        st.session_state.date_filter = {
            "mode": "CUSTOM",
            "start": start,
            "end": end
        }

def trend_card():
    dff = filtered_item_df(df)
    if dff.empty:
        return

    monthly = (
        dff.groupby(dff["DATE"].dt.to_period("M"))
           .agg(UNITS=("USAGE_SOLD", "sum"))
           .reset_index()
    )
    fig = px.line(monthly, x="DATE", y="UNITS")
    st.plotly_chart(fig, use_container_width=True)

def insights_card():
    dff = filtered_item_df(df)
    if dff.empty:
        return

    if dff["PROFIT"].sum() < 0:
        st.error("Item is currently unprofitable")

    if dff["USAGE_SOLD"].sum() < 10:
        st.warning("Low movement item")

# --------------------------------------------------
# CARD REGISTRY (CUSTOMIZABLE)
# --------------------------------------------------
CARDS = {
    "Item Performance": item_performance_card,
    "Suppliers": supplier_card,
    "Date Range": date_control_card,
    "Trends": trend_card,
    "Insights": insights_card,
}

# --------------------------------------------------
# LAYOUT CONFIG
# --------------------------------------------------
selected_cards = st.multiselect(
    "Customize cards",
    list(CARDS.keys()),
    default=["Item Performance", "Suppliers", "Trends"]
)

# --------------------------------------------------
# COPILOT CANVAS
# --------------------------------------------------
left, right = st.columns([2, 1])

for i, card_name in enumerate(selected_cards):
    target = left if i % 2 == 0 else right
    with target:
        card(card_name, CARDS[card_name])
