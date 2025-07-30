import math
from datetime import datetime
from time import sleep

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as f
from pyspark.sql.types import StructType, StructField, DoubleType, StringType, TimestampType, IntegerType

TOPIC_NAME_91 = 'student.topic.cohort5.heorhi.out'  # Это топик, в который Ваше приложение должно отправлять сообщения. Укажите здесь название Вашего топика student.topic.cohort<номер когорты>.<username>.out
TOPIC_NAME_IN = 'student.topic.cohort5.heorhi' # Это топик, из которого Ваше приложение должно читать сообщения. Укажите здесь название Вашего топика student.topic.cohort<номер когорты>.<username>

# При первом запуске ваш топик student.topic.cohort<номер когорты>.<username>.out может не существовать в Kafka и вы можете увидеть такие сообщения:
# ERROR: Topic student.topic.cohort<номер когорты>.<username>.out error: Broker: Unknown topic or partition
# Это сообщение говорит о том, что тест начал проверять работу Вашего приложение, но так как Ваше приложение ещё не отправило туда сообщения, то топик ещё не создан. Нужно подождать несколько минут.

def calculate_distance(lat_1, lon_1, lat_2, lon_2):
    R = 6371

    lat_1 = math.radians(lat_1)
    lon_1 = math.radians(lon_1)

    distance_lat = lat_2 - lat_1
    distance_lon = lon_2 - lon_1
    c = math.sin(distance_lat / 2)**2 + math.cos(lat_1) * math.cos(lat_2) * math.sin(distance_lon / 2)**2
    d = 2 * R * math.asin(math.sqrt(c))
    return d

def spark_init(test_name) -> SparkSession:
    return (
        SparkSession.builder
        .master("local")
        .appName(test_name)
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,org.postgresql:postgresql:42.4.2")
        .getOrCreate()
    )


postgresql_settings = {
    'user': 'student',
    'password': 'de-student'
}


def read_marketing(spark: SparkSession) -> DataFrame:
    return (spark.read
        .format("jdbc")
        .option("url", "jdbc:postgresql://rc1a-fswjkpli01zafgjm.mdb.yandexcloud.net:6432/de")
        .option("driver", "org.postgresql.Driver")
        .option("user", postgresql_settings['user'])
        .option("password", postgresql_settings['password'])
        .option("dbtable", "public.marketing_companies")
        .load())


kafka_security_options = {
    'kafka.security.protocol': 'SASL_SSL',
    'kafka.sasl.mechanism': 'SCRAM-SHA-512',
    'kafka.sasl.jaas.config': 'org.apache.kafka.common.security.scram.ScramLoginModule required username=\"de-student\" password=\"ltcneltyn\";'
}


def read_client_stream(spark: SparkSession) -> DataFrame:
    raw_df = (spark.readStream
              .format('kafka')
              .option('kafka.bootstrap.servers', 'rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091')
              .options(**kafka_security_options)
              .option("subscribe", TOPIC_NAME_IN)
              .load())
    
    # Парсим JSON и преобразуем данные
    value_schema = StructType([
        StructField("client_id", StringType(), True),
        StructField("timestamp", DoubleType(), True),
        StructField("lat", DoubleType(), True),
        StructField("lon", DoubleType(), True)
    ])
    
    parsed = raw_df.withColumn("value", f.col("value").cast("string"))
    parsed = parsed.withColumn("json", f.from_json(f.col("value"), value_schema))
    
    result = parsed.select(
        f.col("json.client_id"),
        f.from_unixtime(f.col("json.timestamp")).cast(TimestampType()).alias("timestamp"),
        f.col("json.lat"),
        f.col("json.lon"),
        f.col("offset")
    )
    
    # Дедупликация по client_id и timestamp с ватермаркой 10 минут
    result = result.withWatermark("timestamp", "10 minutes").dropDuplicates(["client_id", "timestamp"])
    
    return result


def join(user_df, marketing_df) -> DataFrame:
    df = (user_df
            .crossJoin(marketing_df)
            .withColumn("adv_campaign_id", marketing_df.id)
            .withColumn("adv_campaign_name", marketing_df.name)
            .withColumn('distance', 1)
            .withColumn("adv_campaign_description", marketing_df.description)
            .withColumn("adv_campaign_start_time", marketing_df.start_time)
            .withColumn("adv_campaign_end_time", marketing_df.end_time)
            .withColumn("adv_campaign_point_lat", marketing_df.point_lat)
            .withColumn("adv_campaign_point_lon", marketing_df.point_lon)
            .withColumn("client_id", f.substring("client_id", 0, 6))
            .withColumn("created_at", f.lit(datetime.now()))
            .select(
                "client_id",
                "distance",
                "adv_campaign_id",
                "adv_campaign_name",
                "adv_campaign_description",
                "adv_campaign_start_time",
                "adv_campaign_end_time",
                "adv_campaign_point_lat",
                "adv_campaign_point_lon",
                "created_at",
                "offset"
        )
    )
    return df.withColumn(
        'value', f.to_json(
            f.struct(
                f.col('client_id'), f.col("distance"), f.col("adv_campaign_id"), f.col("adv_campaign_name"), f.col("adv_campaign_description"), f.col("adv_campaign_start_time"),
                f.col("adv_campaign_end_time"), f.col("adv_campaign_point_lat"), f.col("adv_campaign_point_lon"), f.col("created_at"), f.col("offset")
            )
        )
    )


def run_query(df):
    return (df
            .writeStream
            .outputMode("append")
            .format("kafka")
            .option('kafka.bootstrap.servers', 'rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091')
            .options(**kafka_security_options)
            .option("topic", TOPIC_NAME_91)
            .option("checkpointLocation", "test_query")
            .trigger(processingTime="60 seconds")
            .start())


if __name__ == "__main__":
    spark = spark_init('join stream')
    client_stream = read_client_stream(spark)
    marketing_df = read_marketing(spark)
    output = join(client_stream, marketing_df)
    query = run_query(output)

    while query.isActive:
        print(f"query information: runId={query.runId}, "
              f"status is {query.status}, "
              f"recent progress={query.recentProgress}")
        sleep(30)

    query.awaitTermination()
