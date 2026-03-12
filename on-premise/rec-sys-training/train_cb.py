import pandas as pd
import re
import os
import joblib

from sklearn.feature_extraction.text import TfidfVectorizer

def load_stopwords(folder):

    stopwords = set()

    for f in os.listdir(folder):

        with open(os.path.join(folder, f), encoding="utf8") as file:

            for line in file:
                stopwords.add(line.strip().lower())

    return stopwords


stopwords = load_stopwords("data/stopwords")

def preprocess_text(text):

    if not isinstance(text, str):
        return ""

    text = text.lower()

    text = re.sub(r"[^\w\s]", " ", text)

    words = [w for w in text.split() if w not in stopwords]

    return " ".join(words)

def train():

    print("Loading data...")

    ratings = pd.read_parquet('data/data_merge.parquet')

    ratings["hotel_description"] = ratings["hotel_description"].apply(preprocess_text)

    hotels = ratings[
        [
            "HotelID",
            "hotel_name",
            "hotel_location",
            "hotel_description",
            "hotel_address",
            "hotel_url",
            "hotel_avg_rating"
        ]
    ]

    hotels = hotels.drop_duplicates().reset_index(drop=True)

    print("Training TF-IDF...")

    vectorizer = TfidfVectorizer(
        max_features=8000,
        ngram_range=(1, 2),
        min_df=2,
    )

    desc_matrix = vectorizer.fit_transform(hotels["hotel_description"])

    print("Saving models...")

    os.makedirs('models', exist_ok=True)

    joblib.dump(vectorizer, "models/vectorizer.pkl")
    joblib.dump(desc_matrix, "models/desc_matrix.pkl")
    joblib.dump(hotels, "models/hotels.pkl")

    print("Training completed!")

if __name__ == "__main__":
    train()