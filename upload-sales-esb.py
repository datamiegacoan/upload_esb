import streamlit as st
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
from decimal import Decimal
import re

# --- Setup credentials ---
PROJECT_ID = 'mie-gacoan-418408'      # ⬅️ Ganti dengan project ID kamu
DATASET = 'sales_data'                # ⬅️ Ganti dengan dataset BQ kamu

# --- BigQuery client ---
import json
import streamlit as st
from google.oauth2 import service_account

service_account_info = st.secrets["GCP_SERVICE_ACCOUNT"]
credentials = service_account.Credentials.from_service_account_info(service_account_info)

client = bigquery.Client(credentials=credentials, project=PROJECT_ID)

st.title("📤 Upload File ESB to BigQuery")

file_type = st.selectbox("Pilih jenis file", ["Sales", "Menu", "Service time"])

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
    
                # Convert ke float
                for col in numeric_columns:
                    df[col] = df[col].astype(float)
    
                # Convert kolom tanggal
                date_columns = ["Sales Date", "Sales In Date", "Sales Out Date"]
                for col in date_columns:
                    df[col] = pd.to_datetime(df[col], errors="coerce").dt.date
    
                # Convert kolom waktu
                time_columns = ["Sales In Time", "Sales Out Time"]
                for col in time_columns:
                    try:
                        df[col] = pd.to_datetime(df[col], format="%H:%M:%S", errors="coerce").dt.time
                    except:
                        df[col] = pd.to_datetime(df[col], errors="coerce").dt.time
                        
             elif file_type == "Menu":
                TABLE = 'esb_menu_recapitulation_report'
                df = pd.read_excel(uploaded_file, header=12, dtype=str)

                # Extract tanggal dari nama file (format DDMMYYYY)
                match = re.search(r'(\d{8})', filename)
                if match:
                    file_date_str = match.group(1)
                    date_parsed = pd.to_datetime(file_date_str, format="%d%m%Y", errors="coerce").date()
                else:
                    date_parsed = None

                df["date"] = date_parsed

                numeric_columns = [
                    "Unit Price", "Subtotal", "Menu Discount Total", "Bill Discount Total", "Net Sales Total", 
                    "Service Charge Total", "Tax Total", "VAT Total", "Grand Total"
                ]

                for col in numeric_columns:
                    df[col] = df[col].astype(float)

            elif file_type == "Service Time":
                TABLE = 'esb_menu_completion_summary_report'
                df = pd.read_excel(uploaded_file, header=11, dtype=str)

                # Convert Sales Date in ke datetime
                df["Sales Date in"] = pd.to_datetime(df["Sales Date in"], errors="coerce")

                # Convert kolom durasi
                duration_columns = ["Kitchen Process", "Checker Qty", "Checker Process", "Total Process"]
                for col in duration_columns:
                    df[col] = pd.to_timedelta(df[col], errors="coerce")

                numeric_columns = [
                    "Kitchen Qty","Checker Qty"]

                for col in numeric_columns:
                    df[col] = df[col].astype(float)

            else:
                st.error("❌ Jenis file tidak dikenali.")
                continue


            st.success(f"✅ File **{uploaded_file.name}** successfully processed!")
            st.write(df.head(5))
            df_list.append(df)

        # Gabung semua dataframe
        combined_df = pd.concat(df_list, ignore_index=True)
        st.info(f"✅ All files combined! Shape: {combined_df.shape}")

        # Check missing values
        if combined_df.isnull().values.any():
            st.warning("⚠️ There are missing values in combined data.")
        else:
            st.info("✅ No missing values detected.")

        # Pilih mode upload
        mode = st.radio("Upload mode", ["Append", "Overwrite"])

        if st.button("Upload to BigQuery"):
            client = bigquery.Client(credentials=credentials, project=PROJECT_ID)
            table_id = f"{PROJECT_ID}.{DATASET}.{TABLE}"

            # Setup config
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
