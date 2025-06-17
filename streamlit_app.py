import streamlit as st
import pandas as pd
import plotly.express as px
import time

if "last_run" not in st.session_state:
    st.session_state.last_run = time.time()

if time.time() - st.session_state.last_run > 10:
    st.session_state.last_run = time.time()
    st.experimental_rerun()

# Load data
df = pd.read_csv("C:\Mini_AI_project\Data Pipeline project\Output\output.csv")

# Title
st.title("NYC Taxi Fare Analysis Dashboard")

# Sidebar filter
passenger_options = df['passenger_count'].dropna().unique().tolist()
passenger_filter = st.sidebar.selectbox("Select Passenger Count", ["All"] + sorted(passenger_options))

# Filter data
if passenger_filter != "All":
    df_filtered = df[df['passenger_count'] == float(passenger_filter)]
else:
    df_filtered = df

# Metrics
st.subheader("Summary Statistics")
st.metric(label="Total Passenger Groups", value=df['passenger_count'].nunique())
st.metric(label="Overall Avg Fare per Mile", value=round(df['avg(fare_per_mile)'].mean(), 2))

# Chart
st.subheader("Average Fare per Mile by Passenger Count")
fig = px.bar(df_filtered, x='passenger_count', y='avg(fare_per_mile)', 
             labels={'avg(fare_per_mile)': 'Avg Fare ($/mile)', 'passenger_count': 'Passenger Count'},
             color='avg(fare_per_mile)', color_continuous_scale='Viridis')
st.plotly_chart(fig)

# Data preview
with st.expander("Show Raw Data"):
    st.dataframe(df_filtered)
