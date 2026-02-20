import os
import requests
import pandas as pd
from datetime import datetime
from google.cloud import storage

BASE_URL = "https://api-bps.gladneycenter.org"
TOKEN = os.getenv("BPS_KPI_TOKEN")
BUCKET_NAME = os.getenv("BUCKET_NAME")

ENDPOINTS = [
    "/api/kpis/em-inquiry-form-syncs",
    "/api/kpis/em-background-form-submissions"
]

def main():

    if not TOKEN:
        raise ValueError("BPS_KPI_TOKEN environment variable is not defined.")

    if not BUCKET_NAME:
        raise ValueError("BUCKET_NAME environment variable is not defined.")

    headers = {
        "Accept": "application/json",
        "x-kpi-token": TOKEN
    }

    dataframes = []

    for endpoint in ENDPOINTS:
        print(f"Calling endpoint: {endpoint}")

        response = requests.get(BASE_URL + endpoint, headers=headers)
        response.raise_for_status()

        json_response = response.json()
        data = json_response.get("data")

        if not data:
            print(f"No data returned for {endpoint}")
            continue

        # 🔥 FIX: handle both list and dict structures safely
        if isinstance(data, list):
            df = pd.DataFrame(data)

        elif isinstance(data, dict):
            df = pd.DataFrame(
                list(data.items()),
                columns=["year_month", "value"]
            )

        else:
            raise ValueError(
                f"Unexpected API format for {endpoint}: {type(data)}"
            )

        # Normalize year_month if present
        if "year_month" in df.columns:
            df["year_month"] = pd.to_datetime(df["year_month"])

        df["kpi_name"] = endpoint.split("/")[-1]
        df["ingestion_timestamp"] = datetime.utcnow()

        dataframes.append(df)

    if not dataframes:
        raise ValueError("No dataframes were created from API responses.")

    final_df = pd.concat(dataframes, ignore_index=True)

    now = datetime.utcnow()
    year = now.strftime("%Y")
    month = now.strftime("%m")
    timestamp = now.strftime("%Y%m%d_%H%M%S")

    filename = f"bps_kpis_{timestamp}.csv"
    local_path = f"/tmp/{filename}"

    final_df.to_csv(local_path, index=False)

    # Partitioned GCS structure
    gcs_path = f"bps_kpis/year={year}/month={month}/{filename}"

    print(f"Uploading to gs://{BUCKET_NAME}/{gcs_path}")

    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)
    blob = bucket.blob(gcs_path)
    blob.upload_from_filename(local_path)

    print("Upload completed successfully.")

if __name__ == "__main__":
    main()
