import streamlit as st
import pandas as pd
import time
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from urllib.parse import unquote

# IMPORT TỪ DATA LAYER
import analytics

# 1. CẤU HÌNH GIAO DIỆN
st.set_page_config(page_title="Market Monitor", layout="wide")

st.markdown("""
<style>
    .block-container { padding-top: 1rem; padding-bottom: 0rem; }
    header, footer { visibility: hidden; }
    [data-testid="stDataFrame"] { border: 1px solid #333; }
    .stTabs [data-baseweb="tab-list"] { gap: 4px; }
    .stTabs [data-baseweb="tab"] { height: 35px; background-color: #222; border-radius: 4px; color: #ccc; }
    .stTabs [aria-selected="true"] { background-color: #00CC96; color: #000; font-weight: bold; }
</style>
""", unsafe_allow_html=True)

# 2. KHỞI TẠO KẾT NỐI (Dùng cache của Streamlit để không kết nối lại liên tục)
@st.cache_resource(show_spinner=False)
def get_db_session():
    return analytics.get_connection()

# 3. HÀM WRAPPER ĐỂ CACHE DATA (Tối ưu hiệu năng UI)
@st.cache_data(ttl=30, show_spinner=False)
def get_cached_symbol_list():
    con = get_db_session()
    return analytics.fetch_symbol_list(con)

def get_live_data(symbol):
    con = get_db_session()
    # Không cache hàm này hoặc cache ttl cực ngắn (1s) vì cần real-time
    return analytics.fetch_market_data(con, symbol, limit=200)

# 4. SIDEBAR CONTROLS
st.sidebar.markdown("### Market Data")

raw_symbols = get_cached_symbol_list()

if not raw_symbols:
    st.warning("Waiting for data pipe...")
    time.sleep(3)
    st.rerun()

# Map tên mã
symbol_map = {unquote(s): s for s in raw_symbols}
display_symbols = list(symbol_map.keys())

# Chọn mặc định
default_idx = 0
if "BINANCE:BTCUSDT" in display_symbols:
    default_idx = display_symbols.index("BINANCE:BTCUSDT")

selected_display = st.sidebar.selectbox("Select Symbol", display_symbols, index=default_idx)
selected_raw = symbol_map[selected_display]
refresh_on = st.sidebar.checkbox("Auto Refresh", value=True)

# 5. MAIN DASHBOARD VISUALIZATION
df = get_live_data(selected_raw)

if df.empty:
    st.info(f"Loading data for {selected_display}...")
    time.sleep(2)
    st.rerun()

# --- METRICS SECTION ---
curr_p = df['price'].iloc[-1]
prev_p = df['price'].iloc[0]
pct_change = ((curr_p - prev_p) / prev_p) * 100 if prev_p != 0 else 0
clean_name = selected_display.replace("BINANCE:", "")

c1, c2, c3, c4 = st.columns(4)
c1.metric("Symbol", clean_name)
c1.metric("Price", f"${curr_p:,.4f}", f"{pct_change:+.2f}%")
c2.metric("Avg Price", f"${df['price'].mean():,.4f}")
c3.metric("Volume", f"{df['volume'].sum():,.2f}")
c4.metric("Records", len(df))

st.markdown("---")

# --- CHART & TABLE SECTION ---
col_left, col_right = st.columns([3, 1])

with col_left:
    # Scale Chart logic
    p_min, p_max = df['price'].min(), df['price'].max()
    pad = (p_max - p_min) * 0.1 if p_max > p_min else p_max * 0.001

    fig = make_subplots(rows=2, cols=1, shared_xaxes=True, row_heights=[0.7, 0.3], vertical_spacing=0.05)

    fig.add_trace(go.Scatter(
        x=df['event_time'], y=df['price'], mode='lines', name='Price',
        line=dict(color='#00CC96', width=2), fill='tozeroy', fillcolor='rgba(0,204,150,0.1)'
    ), row=1, col=1)

    fig.add_trace(go.Bar(
        x=df['event_time'], y=df['volume'], name='Vol', marker_color='#555'
    ), row=2, col=1)

    fig.update_layout(
        height=500, margin=dict(t=10, b=10, l=10, r=10),
        template="plotly_dark", showlegend=False,
        yaxis=dict(range=[p_min - pad, p_max + pad], gridcolor='#333'),
        yaxis2=dict(showgrid=False),
        xaxis=dict(showgrid=False)
    )
    # Dùng timestamp làm key để ép chart vẽ lại mượt mà
    st.plotly_chart(fig, use_container_width=True, config={'displayModeBar': False}, key=f"chart_{time.time()}")

with col_right:
    tab1, tab2 = st.tabs(["Tape", "Stats (15s)"])

    with tab1:
        tape = df.sort_values('event_time', ascending=False).head(15).copy()
        tape['Time'] = tape['event_time'].dt.strftime('%H:%M:%S')
        tape['Price'] = tape['price'].map('{:,.4f}'.format)
        st.dataframe(tape[['Time', 'Price', 'volume']], height=450, hide_index=True, use_container_width=True)

    with tab2:
        try:
            stats = df.set_index('event_time').resample('15S').agg({'price': 'mean', 'volume': 'sum'})
            valid_stats = stats[stats['volume'] > 0].sort_index(ascending=False)

            if not valid_stats.empty:
                valid_stats['Time'] = valid_stats.index.strftime('%H:%M:%S')
                valid_stats['Avg'] = valid_stats['price'].map('{:,.4f}'.format)
                valid_stats['Vol'] = valid_stats['volume'].map('{:,.0f}'.format)
                st.dataframe(valid_stats[['Time', 'Avg', 'Vol']], height=450, hide_index=True, use_container_width=True)
            else:
                st.write("No data in 15s window")
        except:
            st.write("Calc Error")

# 6. AUTO REFRESH LOOP
if refresh_on:
    time.sleep(1)
    st.rerun()