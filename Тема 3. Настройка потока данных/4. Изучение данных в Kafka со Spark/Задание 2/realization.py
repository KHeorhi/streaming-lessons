from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql import functions as f, DataFrame
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, StringType, BinaryType, LongType, DataType, TimestampType
from settings import TOPIC_NAME

# необходимая библиотека с идентификатором в maven
# вы можете использовать ее с помощью метода .config и опции "spark.jars.packages"
spark_jars_packages = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0"

# настройки security для кафки
# вы можете использовать из с помощью метода .options(**kafka_security_options)
kafka_security_options = {
    'kafka.bootstrap.servers': 'rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091',
    'subscribe': TOPIC_NAME,
    'kafka.security.protocol': 'SASL_SSL',
    'kafka.sasl.mechanism': 'SCRAM-SHA-512',
    'kafka.sasl.jaas.config': 'org.apache.kafka.common.security.scram.ScramLoginModule required username=\"de-student\" password=\"ltcneltyn\";',
}

def spark_init() -> SparkSession:
    return SparkSession.builder.master("local").appName('test connect to kafka').config(
    "spark.jars.packages", spark_jars_packages
    ).getOrCreate()


# def load_df(spark: SparkSession) -> DataFrame:
#     df = spark.read.format('kafka').options(**kafka_security_options)
#     return df

incomming_message_schema = StructType(
    [   
        StructField("subscription_id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("description", StringType(), True),
        StructField("price", DoubleType(), True),
        StructField("currency", StringType(), True),
        StructField("key", StringType(), True),
        StructField("value" , StringType(), True),
        StructField("topic" , StringType(), True),
        StructField("partition" , IntegerType(), True),
        StructField("offset" , LongType(), True),
        StructField("timestamp" , TimestampType(), True),
        StructField("timestampType" , IntegerType(), True),
    ]
)

# def transform(df: DataFrame) -> DataFrame:
#     return df.withColumn('value', from_json(col("value").cast("string"), incomming_message_schema))
 
spark = spark_init()

df = (
    spark.read.format('kafka')
    .option('kafka.bootstrap.servers','rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091')
    .option('subscribe','persist_topic')
    .option('kafka.security.protocol','SASL_SSL')
    .option('kafka.sasl.mechanism', 'SCRAM-SHA-512')
    .option('kafka.sasl.jaas.config', 'org.apache.kafka.common.security.scram.ScramLoginModule required username=\"de-student\" password=\"ltcneltyn\";')
    .load()
)
df = (
    df.withColumn("parsed_key_value",from_json(col("value").cast("string"), incomming_message_schema))
    .select("parsed_key_value.*")
)
# source_df = load_df(spark)
# df = transform(source_df)


df.printSchema()
df.show(truncate=False)
