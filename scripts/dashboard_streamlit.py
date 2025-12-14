import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

st.set_page_config(page_title="Retail Analytics", layout="wide")


st.title("🛍️ Retail Analytics Dashboard")
st.markdown("**Sales Performance vs Customer Complaints Analysis**")


@st.cache_data
def load_data():
    df1 = pd.read_parquet('/home/mhafidhrosyadi/bigdata-project/lakehouse/gold/sales_vs_complaints')
    df2 = pd.read_parquet('/home/mhafidhrosyadi/bigdata-project/lakehouse/gold/product_complaints')
    df3 = pd.read_parquet('/home/mhafidhrosyadi/bigdata-project/lakehouse/gold/regional_analysis')
    return df1, df2, df3

df_sales, df_complaints, df_regional = load_data()


col1, col2, col3 = st.columns(3)
col1.metric("Total Sales", f"${df_sales['total_sales'].sum():,.0f}")
col2.metric("Total Complaints", f"{df_sales['total_complaints'].sum():,.0f}")
#col3.metric("Avg Complaint Rate", f"{df_sales['complaint_rate'].mean():.1f}%")

# Chart 1: Sales vs Complaints
st.subheader("Sales vs Complaints by Category")
fig1 = go.Figure()
fig1.add_trace(go.Bar(name='Sales', x=df_sales['category'], y=df_sales['total_sales']))
fig1.add_trace(go.Bar(name='Complaints', x=df_sales['category'], y=df_sales['total_complaints']))
st.plotly_chart(fig1, use_container_width=True)

# Chart 2: Complaint Heatmap
st.subheader("Complaint Heatmap")
pivot = df_complaints.pivot_table(values='total_complaints', 
                                   index='product_category', 
                                   columns='complaint_category', 
                                   fill_value=0)
fig2 = px.imshow(pivot, text_auto=True, aspect="auto")
st.plotly_chart(fig2, use_container_width=True)

# Chart 3: Regional Analysis
st.subheader("Regional Performance")
fig3 = px.sunburst(df_regional, path=['region', 'category'], 
                   values='total_sales', color='total_complaints')
st.plotly_chart(fig3, use_container_width=True)

# Data Table
st.subheader("Detailed Data")
st.dataframe(df_sales)
