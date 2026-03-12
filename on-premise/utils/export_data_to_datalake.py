from minio import Minio
from helpers import load_cfg
from glob import glob
import os

CFG_FILE = "./utils/config.yaml"


def main():
    cfg = load_cfg(CFG_FILE)
    datalake_cfg = cfg["datalake"]
    data_cfg = cfg["data"]

    # Create a client with the MinIO server playground, its access key
    # and secret key.
    client = Minio(
        endpoint=datalake_cfg["endpoint"],
        access_key=datalake_cfg["access_key"],
        secret_key=datalake_cfg["secret_key"],
        secure=False,
    )

    # Create bucket if not exist.
    if not client.bucket_exists(bucket_name=datalake_cfg["bucket_name"]):
        client.make_bucket(bucket_name=datalake_cfg["bucket_name"])
    else:
        print(f'Bucket {datalake_cfg["bucket_name"]} already exists, skip creating!')

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

        object_path = os.path.join(folder, new_name)

        print(f"Uploading {fp} -> {object_path}")
        client.fput_object(
            bucket_name=datalake_cfg["bucket_name"],
            object_name=object_path,
            file_path=fp,
        )

if __name__ == "__main__":
    main()
