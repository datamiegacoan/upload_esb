import streamlit as st
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
import re

# --- Setup credentials ---
PROJECT_ID = 'mie-gacoan-418408'
DATASET = 'sales_data'

# --- BigQuery client ---
import json
service_account_info = st.secrets["GCP_SERVICE_ACCOUNT"]
credentials = service_account.Credentials.from_service_account_info(service_account_info)
client = bigquery.Client(credentials=credentials, project=PROJECT_ID)

st.title("📤 Upload File ESB to BigQuery")

file_type = st.selectbox("Pilih jenis file", ["Sales", "Menu", "Service Time"])

uploaded_files = st.file_uploader(
    "Choose Excel file(s) (.xlsx)",
    type="xlsx",
    accept_multiple_files=True
)

if uploaded_files:
    df_list = []

    try:
        for uploaded_file in uploaded_files:
            filename = uploaded_file.name

            if file_type == "Sales":
                TABLE = 'esb_sales_recapitulation_report'
                df = pd.read_excel(uploaded_file, header=13, dtype=str)

                # Drop rows tanpa Bill Number
                df = df[df["Bill Number"].notna() & (df["Bill Number"].str.strip() != "")]

                numeric_columns = [
                    "Pax Total", "Subtotal", "Menu Discount", "Bill Discount", "Voucher Discount",
                    "Net Sales", "Service Charge Total", "Tax Total", "VAT Total", "Delivery Cost",
                    "Order Fee", "Platform Fee", "Voucher Sales Total", "Rounding Total", "Grand Total"
                ]

                for col in numeric_columns:
                    df[col] = df[col].astype(float)

                date_columns = ["Sales Date", "Sales In Date", "Sales Out Date"]
                for col in date_columns:
                    df[col] = pd.to_datetime(df[col], errors="coerce").dt.date

                time_columns = ["Sales In Time", "Sales Out Time"]
                for col in time_columns:
                    df[col] = pd.to_datetime(df[col], format="%H:%M:%S", errors="coerce").dt.time

            elif file_type == "Menu":
                TABLE = 'esb_menu_recapitulation_report'
                df = pd.read_excel(uploaded_file, header=12, dtype=str)
                
                date_columns = ["Sales Date"]
                for col in date_columns:
                    df[col] = pd.to_datetime(df[col], errors="coerce").dt.date

                numeric_columns = [
                    Qty", "Subtotal", "Service Charge", "Tax Total", "VAT Total", "Total"]

                for col in numeric_columns:
                    df[col] = df[col].astype(float)

            elif file_type == "Service Time":
                TABLE = 'esb_menu_completion_summary_report'
                df = pd.read_excel(uploaded_file, header=10, dtype=str)

                # Convert Sales Date in ke datetime
                df["Sales Date In"] = pd.to_datetime(df["Sales Date In"], errors="coerce")

                # Convert kolom durasi
                duration_columns = ["Kitchen Process", "Checker Process", "Total Process"]
                for col in duration_columns:
                    df[col] = pd.to_timedelta(df[col], errors="coerce")
                    df[col] = df[col].apply(lambda x: str(x).split(" ")[-1] if pd.notnull(x) else None)

                numeric_columns = ["Kitchen Qty", "Checker Qty"]
                for col in numeric_columns:
                    df[col] = df[col].astype(float)

            else:
                st.error("❌ Jenis file tidak dikenali.")
                continue

            st.success(f"✅ File **{uploaded_file.name}** successfully processed!")
            st.write(df.head(5))
            df_list.append(df)

        combined_df = pd.concat(df_list, ignore_index=True)
        st.info(f"✅ All files combined! Shape: {combined_df.shape}")

        if combined_df.isnull().values.any():
            st.warning("⚠️ There are missing values in combined data.")
        else:
            st.info("✅ No missing values detected.")

        mode = st.radio("Upload mode", ["Append", "Overwrite"])

        if st.button("Upload to BigQuery"):
            table_id = f"{PROJECT_ID}.{DATASET}.{TABLE}"

            if mode == "Append":
                job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
            else:
                job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")

            with st.spinner("Uploading to BigQuery..."):
                job = client.load_table_from_dataframe(combined_df, table_id, job_config=job_config)
                job.result()

            st.success(f"✅ Upload to BigQuery successful! Mode: {mode}")

    except Exception as e:
        st.error(f"❌ Failed to process file: {e}")
