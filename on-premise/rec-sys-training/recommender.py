import joblib
import numpy as np
import pandas as pd
import re
import os
from sqlalchemy import create_engine

engine = create_engine(
    "postgresql://warehouse:warehouse@warehouse-db:5432/hotel_dw"
)

hotels_merge = pd.read_sql(
    "SELECT * FROM data_cb_merge",
    engine
)

from sklearn.metrics.pairwise import linear_kernel

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

vectorizer = joblib.load("models/vectorizer.pkl")
desc_matrix = joblib.load("models/desc_matrix.pkl")
hotels = joblib.load("models/hotels.pkl")

# load user profile
def get_recommendation(user_id, location, k=10):

    user_rows = hotels_merge.loc[hotels_merge["user_id"] == user_id]

    if user_rows.empty:
        return []

    user_description = user_rows.iloc[0]["descriptions"]

    user_description = preprocess_text(user_description)

    # vectorize user
    user_vec = vectorizer.transform([user_description])

    # cosine similarity
    sim_scores = linear_kernel(user_vec, desc_matrix)[0]

    # sort hotels
    hotel_indices = np.argsort(sim_scores)[::-1]

    rec = hotels.iloc[hotel_indices]

    rec = rec[rec["hotel_location"] == location]

    rec = rec.head(k)

    return rec.to_dict("records")