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
        raise ValueError("BPS_KPI_TOKEN não definido.")

    if not BUCKET_NAME:
        raise ValueError("BUCKET_NAME não definido.")

    headers = {
        "Accept": "application/json",
        "x-kpi-token": TOKEN
    }

    dfs = []

    for ep in ENDPOINTS:
        print(f"Consumindo endpoint: {ep}")

        response = requests.get(BASE_URL + ep, headers=headers)
        response.raise_for_status()

        data = response.json()["data"]

        df = (
            pd.DataFrame(list(data.items()), columns=["year_month", "value"])
            .assign(
                year_month=lambda x: pd.to_datetime(x["year_month"]),
                kpi_name=ep.split("/")[-1],
                ingestion_timestamp=datetime.utcnow()
            )
        )

        dfs.append(df)

    final_df = pd.concat(dfs)

    now = datetime.utcnow()
    year = now.strftime("%Y")
    month = now.strftime("%m")
    timestamp = now.strftime("%Y%m%d_%H%M%S")

    filename = f"bps_kpis_{timestamp}.csv"
    local_path = f"/tmp/{filename}"

    final_df.to_csv(local_path, index=False)

    # Estrutura particionada
    gcs_path = f"bps_kpis/year={year}/month={month}/{filename}"

    print(f"Fazendo upload para gs://{BUCKET_NAME}/{gcs_path}")

    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)
    blob = bucket.blob(gcs_path)
    blob.upload_from_filename(local_path)

    print("Upload concluído com sucesso.")

if __name__ == "__main__":
    main()