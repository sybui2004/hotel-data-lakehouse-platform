from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def main():
    spark = (
        SparkSession.builder
        .appName("Lake to Warehouse Transform")
        .config("spark.executor.memory", "4g")
        .config("spark.driver.memory", "2g")
        .getOrCreate()
    )

    df = spark.read.parquet(
        "gs://hotel-data-lake/raw/data_merge/*.parquet"
    ).cache()

    dim_hotel = df.select(
        "hotel_id",
        "hotel_name",
        "hotel_location",
        "hotel_address",
        "hotel_description",
        "hotel_url"
    ).dropDuplicates(["hotel_id"])

    dim_user = df.select(
        "user_id",
        col("user").alias("user_name"),
        "country"
    ).dropDuplicates(["user_id"])

    fact_review = df.select(
        "hotel_id",
        "user_id",
        "rating",
        "review"
    )

    df_cb = spark.read.parquet(
        "gs://hotel-data-lake/raw/data_cb_merge/*.parquet"
    ).repartition(10)

    df_cb = df_cb.select(
        "user_id",
        col("descriptions")
    )

    common_options = {
        "temporaryGcsBucket": "hotel-data-lake",
        "intermediateFormat": "parquet"
    }

    project_id = "dataengineering-489105"

    dim_hotel.write.format("bigquery") \
        .options(**common_options) \
        .option("table", f"{project_id}.hotel_dw.dim_hotel") \
        .mode("overwrite").save()

    dim_user.write.format("bigquery") \
        .options(**common_options) \
        .option("table", f"{project_id}.hotel_dw.dim_user") \
        .mode("overwrite").save()

    fact_review.write.format("bigquery") \
        .options(**common_options) \
        .option("table", f"{project_id}.hotel_dw.fact_review") \
        .mode("overwrite").save()

    df_cb.write.format("bigquery") \
        .options(**common_options) \
        .option("table", f"{project_id}.hotel_dw.data_cb_merge") \
        .mode("overwrite").save()

    spark.stop()
if __name__ == "__main__":
    main()


# Role: Dataproc Administrator