import pandas as pd

hotel_details = pd.read_csv("data/hotel_details/hotel_details.csv")
ratings = pd.read_csv("data/hotel_ratings/hotel_ratings.csv")

ratings = ratings.merge(
    hotel_details[['hotel_id','hotel_location','hotel_name','hotel_description']],
    left_on='HotelID',
    right_on='hotel_id',
    how='left'
)

ratings = ratings.drop(columns=['hotel_id'])

ratings = ratings.rename(columns={'hotel_location': 'Location'})

ratings = ratings.drop_duplicates(subset=['HotelID','UserID'])

ratings['UserID'] = ratings['UserID'].astype('int32')
ratings['HotelID'] = ratings['HotelID'].astype('int32')
ratings['Rating'] = ratings['Rating'].astype('float32')
ratings['Location'] = ratings['Location'].astype('category')

# USER BASED DATASET FOR User-based Collaborative Filtering: similar users -> recommend hotel

# Count ratings user by location
ratings_user_count = (
    ratings
    .groupby(['Location', 'UserID'])
    .size()
    .reset_index(name='counts')
)

# Filter user >=30 ratings
ratings_user_greater30 = ratings_user_count.query("counts >= 30")[['Location','UserID']]

result_user = ratings.merge(
    ratings_user_greater30,
    on=['Location','UserID'],
    how='inner'
)

result_user.to_parquet("data/train_test/total_by_user.parquet", index=False)

test_user = (
    result_user
    .sort_values(['Location','UserID'])
    .groupby(['Location','UserID'])
    .sample(n=5, random_state=42)
    .reset_index(drop=True)
)

test_user.to_parquet("data/train_test/test_by_user.parquet", index=False)

train_user = result_user.merge(
    test_user[['HotelID','UserID']],
    on=['HotelID','UserID'],
    how='left',
    indicator=True
)

train_user = train_user[train_user['_merge'] == 'left_only']
train_user = train_user.drop(columns='_merge').reset_index(drop=True)

train_user.to_parquet("data/train_test/train_by_user.parquet", index=False)

# ITEM BASED DATASET FOR Item-based Collaborative Filtering: similar hotels -> recommend hotel
rating_item = ratings.drop_duplicates(subset=['HotelID','UserID'])

# Count ratings for hotel
ratings_item_count = (
    rating_item
    .groupby('HotelID')
    .size()
    .reset_index(name='counts')
)

# Filter hotel >=10 ratings
ratings_item_greater10 = ratings_item_count.query("counts >= 10")[['HotelID']]

result_item = rating_item.merge(
    ratings_item_greater10,
    on='HotelID',
    how='inner'
)

result_item.to_parquet("data/train_test/total_by_item.parquet", index=False)

test_item = (
    result_item
    .sort_values('HotelID')
    .groupby('HotelID')
    .head(3)
    .reset_index(drop=True)
)

test_item.to_parquet("data/train_test/test_by_item.parquet", index=False)

train_item = ratings.merge(
    test_item[['HotelID','UserID']],
    on=['HotelID','UserID'],
    how='left',
    indicator=True
)

train_item = train_item[train_item['_merge']=='left_only']
train_item = train_item.drop(columns='_merge').reset_index(drop=True)

train_item.to_parquet("data/train_test/train_total_by_item.parquet", index=False)

training_item = result_item.merge(
    test_item[['HotelID','UserID']],
    on=['HotelID','UserID'],
    how='left',
    indicator=True
)

training_item = training_item[training_item['_merge']=='left_only']
training_item = training_item.drop(columns='_merge').reset_index(drop=True)

training_item.to_parquet("data/train_test/train_by_item.parquet", index=False)

# CONTENT BASED DATASE FOR Content-based: build user profile from hotel descriptions

# Count ratings by user + location
ratings_cb_count = (
    ratings
    .groupby(['Location','UserID'])
    .size()
    .reset_index(name='counts')
)

# Filter user >=10 ratings
ratings_cb_greater10 = ratings_cb_count.query("counts >= 10")[['Location','UserID']]

result_cb = ratings.merge(
    ratings_cb_greater10,
    on=['Location','UserID'],
    how='inner'
)

result_cb.to_parquet("data/train_test/test_total_cb.parquet", index=False)

test_cb = (
    result_cb
    .sort_values(['Location','UserID'])
    .groupby(['Location','UserID'])
    .head(5)
    .reset_index(drop=True)
)

test_cb.to_parquet("data/train_test/test_cb.parquet", index=False)

train_cb = ratings.merge(
    test_cb[['HotelID','UserID']],
    on=['HotelID','UserID'],
    how='left',
    indicator=True
)

train_cb = train_cb[train_cb['_merge']=='left_only']
train_cb = train_cb.drop(columns='_merge').reset_index(drop=True)

train_cb.to_parquet("data/train_test/train_total_cb.parquet", index=False)

training_cb = result_cb.merge(
    test_cb[['HotelID','UserID']],
    on=['HotelID','UserID'],
    how='left',
    indicator=True
)

training_cb = training_cb[training_cb['_merge']=='left_only']
training_cb = training_cb.drop(columns='_merge').reset_index(drop=True)
training_cb['hotel_description'] = training_cb['hotel_description'].fillna('').astype(str)
training_cb['hotel_name'] = training_cb['hotel_name'].fillna('').astype(str)
training_cb.to_parquet("data/train_test/train_cb.parquet", index=False)

# BUILD USER PROFILE (CONTENT BASED)

df_merge = (
    training_cb
    .groupby(['UserID','Location'])
    .agg({
        'hotel_name': lambda x: ','.join(x),
        'hotel_description': lambda x: ' '.join(x)
    })
    .reset_index()
)

df_merge = df_merge.rename(columns={
    'hotel_name':'Name Hotel',
    'hotel_description':'Descriptions'
})

df_merge.to_parquet("data/train_test/data_cb_merge.parquet", index=False)