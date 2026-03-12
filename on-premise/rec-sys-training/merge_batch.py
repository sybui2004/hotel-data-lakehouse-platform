import pandas as pd

df = pd.DataFrame()

for i in range(0, 82):
    df = pd.concat([df, pd.read_csv(f"data/hotel_ratings/hotel_ratings_batch_{i}.csv")], ignore_index=True)

df.to_csv("data/hotel_ratings/hotel_ratings.csv", index=False, encoding="utf-8")

df = pd.DataFrame()

for i in range(0, 9):
    df = pd.concat([df, pd.read_csv(f"data/hotel_details/hotel_details_batch_{i}.csv")], ignore_index=True)

df.to_csv("data/hotel_details/hotel_details.csv", index=False, encoding="utf-8")
