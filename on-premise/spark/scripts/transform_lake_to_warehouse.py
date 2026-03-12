from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def main():
    spark = (
        SparkSession.builder.master("local[*]")
        .appName("Lake to Warehouse Transform")
        .config("spark.sql.parquet.enableVectorizedReader", "false")
        .config("spark.sql.parquet.columnarReaderBatchSize", "512")
        .getOrCreate()
    )
    
    df = spark.read.parquet(
        "s3a://hotel-data-lake/raw/data_merge/*.parquet"
    ).cache()

    df.printSchema()

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
        "s3a://hotel-data-lake/raw/data_cb_merge/*.parquet"
    ).repartition(10)

    df_cb = df_cb.select(
        "user_id",
        col("descriptions")
    )

    jdbc_url = "jdbc:postgresql://warehouse-db:5432/hotel_dw"

    properties = {
        "user": "warehouse",
        "password": "warehouse",
        "driver": "org.postgresql.Driver"
    }

    dim_hotel.write.jdbc(
        url=jdbc_url,
        table="dim_hotel",
        mode="overwrite",
        properties=properties
    )

    dim_user.write.jdbc(
        url=jdbc_url,
        table="dim_user",
        mode="overwrite",
        properties=properties
    )

    fact_review.write.jdbc(
        url=jdbc_url,
        table="fact_review",
        mode="overwrite",
        properties=properties
    )

    df_cb.write.jdbc(
        url=jdbc_url,
        table="data_cb_merge",
        mode="overwrite",
        properties=properties
    )
    spark.stop()
if __name__ == "__main__":
    main()

# docker exec -it datalake-spark /opt/spark/bin/spark-submit --jars /opt/spark/extra-jars/postgresql-42.6.0.jar,/opt/spark/extra-jars/hadoop-aws-3.3.4.jar,/opt/spark/extra-jars/aws-java-sdk-bundle-1.12.262.jar --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 --conf spark.hadoop.fs.s3a.access.key=minio_access_key --conf spark.hadoop.fs.s3a.secret.key=minio_secret_key --conf spark.hadoop.fs.s3a.path.style.access=true --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem --conf spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider /opt/spark/scripts/transform_lake_to_warehouse.py