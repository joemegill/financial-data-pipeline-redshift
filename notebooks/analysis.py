# notebooks/analysis.py

import os
import pandas as pd
import streamlit as st
import psycopg2
from dotenv import load_dotenv

load_dotenv()

st.set_page_config(page_title="Portfolio Analytics", page_icon="💼", layout="wide")
st.title("💼 Portfolio Performance Dashboard")

# --- Database connection ---
def get_connection():
    return psycopg2.connect(
        dbname=os.getenv("REDSHIFT_DB"),
        user=os.getenv("REDSHIFT_USER"),
        password=os.getenv("REDSHIFT_PASSWORD"),
        host=os.getenv("REDSHIFT_HOST"),
        port=os.getenv("REDSHIFT_PORT", "5439"),
    )

# --- Load data ---
@st.cache_data(ttl=600)
def load_portfolio_data():
    query = """
        SELECT symbol, total_value
        FROM fact_portfolio_performance
        ORDER BY total_value DESC;
    """
    conn = get_connection()
    df = pd.read_sql(query, conn)
    conn.close()
    return df

try:
    df = load_portfolio_data()

    df = pd.DataFrame({"symbol":["apple"], "total_value":[100]})

    st.subheader("Portfolio Holdings")
    # st.dataframe(df, use_container_width=True)

    # total_value = df["total_value"].sum()
    # st.metric(label="Total Portfolio Value", value=f"${total_value:,.2f}")

    st.bar_chart(df.set_index("symbol")["total_value"])

except Exception as e:
    st.error(f"❌ Failed to load data: {e}")
    st.info("Make sure your Redshift cluster is running and your .env is configured correctly.")
