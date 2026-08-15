import streamlit as st
from google.cloud import bigquery

st.set_page_config(layout="wide")

st.header("Carding Fraud Detection")

client = bigquery.Client()

df = client.query(
    """
    SELECT *
    FROM `pub-sub-fraud-detection.fraud_analytics.vw_card_testing_monitor`
    ORDER BY date DESC, hour DESC, minute DESC
    """
).to_dataframe()

is_alert = st.checkbox("Is Alert")

severity_score = st.selectbox(
    "Minimum Severity Score",
    (0, 1, 2, 3, 4)
)

filtered_df = df
if st.button("Apply Filters"):
    if is_alert:
        filtered_df = filtered_df[filtered_df["is_alert"] == is_alert]
    filtered_df = filtered_df[filtered_df["severity_score"] >= severity_score]

st.dataframe(filtered_df)
