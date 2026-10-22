import re
from glob import glob

import pandas as pd


def merge_batches(folder, name):
    # Sort numerically so batch_10 comes after batch_9, not after batch_1
    files = sorted(
        glob(f"{folder}/{name}_batch_*.csv"),
        key=lambda f: int(re.search(r"_batch_(\d+)\.csv$", f).group(1)),
    )

    # Concatenate once instead of growing the DataFrame inside the loop
    df = pd.concat((pd.read_csv(f) for f in files), ignore_index=True)

    df.to_csv(f"{folder}/{name}.csv", index=False, encoding="utf-8")


merge_batches("data/hotel_ratings", "hotel_ratings")
merge_batches("data/hotel_details", "hotel_details")
