import pandas as pd
import numpy as np
import re
import os
import nltk

from sklearn.metrics.pairwise import cosine_similarity, linear_kernel
from sklearn.metrics import mean_squared_error, mean_absolute_error
from sklearn.feature_extraction.text import TfidfVectorizer

nltk.download('stopwords')

hotel_details = pd.read_csv("data/hotel_details/hotel_details.csv")
ratings = pd.read_csv("data/hotel_ratings/hotel_ratings.csv")

ratings = ratings.merge(
    hotel_details[['hotel_id','hotel_url','hotel_location', 'hotel_name','hotel_description','hotel_address']],
    left_on='HotelID',
    right_on='hotel_id',
    how='left'
)

ratings_user = pd.read_parquet('data/train_test/total_by_user.parquet')
ratings_test_user = pd.read_parquet('data/train_test/test_by_user.parquet')

ratings_test_item = pd.read_parquet('data/train_test/test_by_item.parquet')
ratings_test_cb = pd.read_parquet('data/train_test/test_cb.parquet')

hotels_merge = pd.read_parquet('data/train_test/data_cb_merge.parquet')

ratings_user_df = ratings_user.pivot_table(
    index="UserID",
    columns="HotelID",
    values="Rating"
)

global_mean = ratings_user["Rating"].mean()

# USER BASED COLLABORATIVE FILTERING

user_mean = ratings_user_df.mean(axis=1)

ratings_centered = ratings_user_df.sub(user_mean, axis=0)
ratings_centered = ratings_centered.fillna(0)

user_similarity = cosine_similarity(ratings_centered)

user_similarity_df = pd.DataFrame(
    user_similarity,
    index=ratings_user_df.index,
    columns=ratings_user_df.index
)

def most_similar_user(user_id, k):

    if user_id not in user_similarity_df.index:
        return None

    return (
        user_similarity_df.loc[user_id]
        .drop(user_id)
        .nlargest(k)
    )


def get_recommendation_based_on_user(user_id, k, location):

    similar_users = most_similar_user(user_id, k)

    if similar_users is None:
        return None

    similar_users = similar_users[similar_users > 0]

    if len(similar_users) == 0:
        return None

    neighbor_ratings = ratings_user_df.loc[similar_users.index]

    neighbor_mean = user_mean.loc[similar_users.index]

    neighbor_centered = neighbor_ratings.sub(neighbor_mean, axis=0)

    sim_scores = similar_users.values.reshape(-1,1)

    weighted = neighbor_centered.mul(sim_scores)

    score = weighted.sum(axis=0)

    sim_sum = np.abs(sim_scores).sum()

    predicted = user_mean[user_id] + (score / sim_sum)

    user_seen = ratings_user_df.loc[user_id]

    predicted = predicted[user_seen.isna()]

    hotels_loc = set(
        ratings_user[ratings_user.Location == location]["HotelID"]
    )

    predicted = predicted[predicted.index.isin(hotels_loc)]

    return predicted.sort_values(ascending=False)


def predict_for_test_user(test_df, k):

    y_pred = []
    cache = {}

    print(f"Predicting {len(test_df)} samples")

    for _, row in test_df.iterrows():

        user = row["UserID"]
        hotel = row["HotelID"]
        location = row["Location"]

        key = (user, location)

        if key not in cache:
            cache[key] = get_recommendation_based_on_user(user, k, location)

        rec = cache[key]

        if rec is None:
            y_pred.append(global_mean)

        elif hotel in rec.index:
            y_pred.append(rec[hotel])

        else:
            y_pred.append(global_mean)

    return y_pred


y_pred = predict_for_test_user(ratings_test_user, k=20)

y_true = ratings_test_user["Rating"]

mae = mean_absolute_error(y_true, y_pred)
rmse = np.sqrt(mean_squared_error(y_true, y_pred))
nmae = mae / 10

print("USER BASED CF")
print("MAE:", mae)
print("RMSE:", rmse)
print("NMAE:", nmae)

# ITEM BASED COLLABORATIVE FILTERING

ratings_item_df = ratings_centered.T

item_similarity = cosine_similarity(ratings_item_df)

item_similarity_df = pd.DataFrame(
    item_similarity,
    index=ratings_item_df.index,
    columns=ratings_item_df.index
)

item_topk = {
    item: item_similarity_df.loc[item].drop(item).nlargest(20)
    for item in item_similarity_df.index
}

def get_recommendation_based_on_item(user_id, k, location):

    if user_id not in ratings_user_df.index:
        return None

    user_ratings = ratings_user_df.loc[user_id]

    rated_items = user_ratings.dropna()

    if len(rated_items) == 0:
        return None

    scores = {}
    sim_sums = {}

    for hotel_id, rating in rated_items.items():

        similar_items = item_topk.get(hotel_id)

        if similar_items is None:
            continue

        for sim_hotel, sim_score in similar_items.items():

            if sim_hotel in rated_items.index:
                continue

            scores.setdefault(sim_hotel,0)
            sim_sums.setdefault(sim_hotel,0)

            scores[sim_hotel] += sim_score * rating
            sim_sums[sim_hotel] += abs(sim_score)

    predicted = {
        h: scores[h]/sim_sums[h]
        for h in scores if sim_sums[h] != 0
    }

    predicted = pd.Series(predicted)

    hotels_loc = set(
        ratings_user[ratings_user.Location == location]["HotelID"]
    )

    predicted = predicted[predicted.index.isin(hotels_loc)]

    return predicted.sort_values(ascending=False)


def predict_for_test_item(test_df, k):

    y_pred = []
    cache = {}

    print(f"Predicting {len(test_df)} samples")

    for _, row in test_df.iterrows():

        user = row["UserID"]
        hotel = row["HotelID"]
        location = row["Location"]

        key = (user, location)

        if key not in cache:
            cache[key] = get_recommendation_based_on_item(user,k,location)

        rec = cache[key]

        if rec is None:
            y_pred.append(global_mean)

        elif hotel in rec.index:
            y_pred.append(rec[hotel])

        else:
            y_pred.append(global_mean)

    return y_pred


y_pred = predict_for_test_item(ratings_test_item,20)

y_true = ratings_test_item["Rating"]

mae = mean_absolute_error(y_true, y_pred)
rmse = np.sqrt(mean_squared_error(y_true, y_pred))
nmae = mae / 10

print("ITEM BASED CF")
print("MAE:", mae)
print("RMSE:", rmse)
print("NMAE:", nmae)

# CONTENT BASED

def load_stopwords(folder):

    stopwords=set()

    for f in os.listdir(folder):

        with open(os.path.join(folder,f),encoding="utf8") as file:

            for line in file:
                stopwords.add(line.strip().lower())

    return stopwords


stopwords = load_stopwords("data/stopwords")


def preprocess_text(text):

    if not isinstance(text,str):
        return ""

    text = text.lower()

    text = re.sub(r"[^\w\s]"," ",text)

    words = [w for w in text.split() if w not in stopwords]

    return " ".join(words)


ratings["hotel_description"] = ratings["hotel_description"].apply(preprocess_text)

hotels = ratings[['HotelID','hotel_name','hotel_location','hotel_description']]
hotels = hotels.drop_duplicates().reset_index(drop=True)

hotels_merge["Descriptions"] = hotels_merge["Descriptions"].apply(preprocess_text)

vectorizer = TfidfVectorizer(
    max_features=8000,
    ngram_range=(1,2),
    min_df=2
)

desc_matrix = vectorizer.fit_transform(hotels["hotel_description"])

user_desc_matrix = vectorizer.transform(hotels_merge["Descriptions"])

cosine_sim = linear_kernel(user_desc_matrix, desc_matrix)

def get_cb_recommendation(user_id, location, k):

    idx_list = hotels_merge.index[hotels_merge["UserID"]==user_id]

    if len(idx_list)==0:
        return []

    idx = idx_list[0]

    sim_scores = list(enumerate(cosine_sim[idx]))

    sim_scores = sorted(sim_scores,key=lambda x:x[1],reverse=True)

    hotel_indices = [i[0] for i in sim_scores]

    rec = hotels.iloc[hotel_indices]

    rec = rec[rec["hotel_location"]==location]

    return rec["hotel_name"].head(k).tolist()


list_user = hotels_merge["UserID"].unique()

num_hotels_cb = 25
  
list_content_based = []

for user in list_user:

    location = hotels_merge[hotels_merge['UserID']==user]['Location'].iloc[0]

    rec = get_cb_recommendation(user, location, num_hotels_cb)

    list_content_based.append(rec)

df_content_based = pd.DataFrame({
    'UserID': list_user,
    'List Recommendation': list_content_based
})

user_rec_dict = dict(zip(df_content_based['UserID'], df_content_based['List Recommendation']))

check_userid = set()
count = 0

for _, row in ratings_test_cb.iterrows():

    user = row['UserID']
    hotel = row['hotel_name']

    if user in user_rec_dict and user not in check_userid:

        if hotel in user_rec_dict[user]:

            print(f'user = {user}, hotel = {hotel}, list hotels = {user_rec_dict[user]}')

            count += 1
            check_userid.add(user)

accuracy = round((count/ratings_test_cb["UserID"].nunique())*100,2)

print("CONTENT BASED")
print("Accuracy:",accuracy,"%")

df_content_based.to_csv(f'data/result_cb_{num_hotels_cb}.csv',index=False)