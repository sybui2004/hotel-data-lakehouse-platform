import joblib
import numpy as np
import pandas as pd
import re
import os
from sqlalchemy import create_engine
from sklearn.metrics.pairwise import linear_kernel

engine = create_engine(
    "postgresql://{user}:{password}@{host}:{port}/{name}".format(
        user=os.getenv("DB_USER", "warehouse"),
        password=os.getenv("DB_PASSWORD", "warehouse"),
        host=os.getenv("DB_HOST", "warehouse-db"),
        port=os.getenv("DB_PORT", "5432"),
        name=os.getenv("DB_NAME", "hotel_dw"),
    )
)

# user_id -> descriptions, loaded on first request so the app can start
# before the warehouse has been populated by the Spark job.
_user_descriptions = None


def get_user_descriptions():
    global _user_descriptions

    if _user_descriptions is None:
        df = pd.read_sql("SELECT user_id, descriptions FROM data_cb_merge", engine)
        _user_descriptions = (
            df.drop_duplicates("user_id").set_index("user_id")["descriptions"]
        )

    return _user_descriptions

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

    user_description = get_user_descriptions().get(user_id)

    # Missing users and NULL/NaN descriptions would otherwise score every
    # hotel 0 and return arbitrary results.
    if not isinstance(user_description, str) or not user_description:
        return []

    user_description = preprocess_text(user_description)

    # vectorize user
    user_vec = vectorizer.transform([user_description])

    # cosine similarity
    sim_scores = linear_kernel(user_vec, desc_matrix)[0]

    # keep only hotels in the requested location, then sort
    location_mask = (hotels["hotel_location"] == location).to_numpy()

    filtered_hotels = hotels[location_mask]
    filtered_scores = sim_scores[location_mask]

    top_indices = np.argsort(filtered_scores)[::-1][:k]

    rec = filtered_hotels.iloc[top_indices]

    return rec.to_dict("records")