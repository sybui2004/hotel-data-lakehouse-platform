import joblib
import numpy as np
import pandas as pd
import re
import os
from sklearn.metrics.pairwise import linear_kernel
from google.cloud import bigquery
from google.oauth2 import service_account

credentials = service_account.Credentials.from_service_account_file("/app/gcp-key.json")

PROJECT_ID = "dataengineering-489105"
TABLE_ID = "dataengineering-489105.hotel_dw.data_cb_merge"

bq_client = bigquery.Client(project=PROJECT_ID,credentials=credentials)

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

_user_cache = {}

def get_user_description(user_id):

    try:
        user_id = int(user_id)
    except:
        return None
    
    if user_id in _user_cache:
        return _user_cache[user_id]

    query = f"""
        SELECT descriptions
        FROM `{TABLE_ID}`
        WHERE user_id = @user_id
        LIMIT 1
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("user_id", "INT64", user_id)
        ]
    )

    print("USER_ID:", user_id, type(user_id))
    
    try:
        df = bq_client.query(query, job_config=job_config).to_dataframe()

        if df.empty:
            return None

        description = df.iloc[0]["descriptions"]

        _user_cache[user_id] = description

        return description

    except Exception as e:

        print("BigQuery error:", e)

        return None

def get_recommendation(user_id, location, k=10):

    user_description = get_user_description(user_id)

    if not user_description:
        return []

    user_description = preprocess_text(user_description)

    user_vec = vectorizer.transform([user_description])

    sim_scores = linear_kernel(user_vec, desc_matrix).flatten()

    location_mask = hotels["hotel_location"] == location

    filtered_hotels = hotels[location_mask]
    filtered_scores = sim_scores[location_mask]

    top_indices = np.argsort(filtered_scores)[::-1][:k]

    rec = filtered_hotels.iloc[top_indices]

    return rec.to_dict("records")