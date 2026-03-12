from google.cloud import storage
from helpers import load_cfg
from glob import glob
import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CFG_FILE = os.path.join(BASE_DIR, "config.yaml")
KEY_FILE = "/opt/airflow/gcp-key.json"

def main():
    cfg = load_cfg(CFG_FILE)
    datalake_cfg = cfg["datalake"]
    data_cfg = cfg["data"]

    # Create a client with the MinIO server playground, its access key
    # and secret key.
    client = storage.Client.from_service_account_json(KEY_FILE)

    bucket = client.bucket(datalake_cfg["bucket_name"])

    # Upload files.
    all_fps = glob(os.path.join(data_cfg["folder_path"], "*.parquet"))

    table_counters = {}

    for fp in all_fps:
        filename = os.path.basename(fp)
        table_name = filename.replace(".parquet", "")

        folder = datalake_cfg["folders"].get(table_name)

        if folder is None:
            print(f"Skip {filename}, no folder mapping")
            continue

        if table_name not in table_counters:
            table_counters[table_name] = 0

        part_id = table_counters[table_name]
        new_name = f"part-{part_id:05d}.parquet"

        table_counters[table_name] += 1

        object_path = f"{folder}/{new_name}"

        print(f"Uploading {fp} -> gs://{datalake_cfg['bucket_name']}/{object_path}")

        blob = bucket.blob(object_path)

        blob.upload_from_filename(fp)

if __name__ == "__main__":
    main()

# Role: Storage Object Admin