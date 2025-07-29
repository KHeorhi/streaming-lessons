from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as f
from pyspark.sql.types import StructType, StructField, DoubleType, StringType, IntegerType, LongType, TimestampType
from pyspark.sql.functions import from_json, col

# необходимая библиотека с идентификатором в maven
# вы можете использовать ее с помощью метода .config и опции "spark.jars.packages"
spark_jars_packages = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0"

# настройки security для кафки
# вы можете использовать из с помощью метода .options(**kafka_security_options)
kafka_security_options = {
    'kafka.bootstrap.servers': 'rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091',
    'subscribe': 'student.topic.cohort5.heorhi',
    'kafka.security.protocol': 'SASL_SSL',
    'kafka.sasl.mechanism': 'SCRAM-SHA-512',
    'kafka.sasl.jaas.config': 'org.apache.kafka.common.security.scram.ScramLoginModule required username=\"de-student\" password=\"ltcneltyn\";', 
}

incomming_message_schema = StructType(
    [   
        StructField("client_id", StringType(), True),
        StructField("timestamp" , TimestampType(), True),
        StructField("lat" , DoubleType(), True),
        StructField("lon" , DoubleType(), True),
    ]
)


def spark_init() -> SparkSession:
    return SparkSession.builder.master("local").appName('test connect to kafka').config(
    "spark.jars.packages", spark_jars_packages
    ).getOrCreate()


def load_df(spark: SparkSession) -> DataFrame:
    return spark.readStream.format('kafka').options(**kafka_security_options).load()


def transform(df: DataFrame) -> DataFrame:
    df = df.withColumn("parsed_key_value",from_json(col("value").cast("string"), incomming_message_schema)).select("parsed_key_value.*").dropDuplicates(['client_id', 'timestamp']).withWatermark('timestamp', '10 minute') 
    return df

spark = spark_init()

source_df = load_df(spark)
output_df = transform(source_df)
output_df.printSchema()

query = (output_df
         .writeStream
         .outputMode("append")
         .format("console")
         .option("truncate", False)
         .trigger(once=True)
         .start())
try:
    query.awaitTermination()
finally:
    query.stop()