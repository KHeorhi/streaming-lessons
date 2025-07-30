from pyspark.sql import SparkSession


spark = (
        SparkSession.builder.appName('test_name')
        .config("spark.jars.packages", "org.postgresql:postgresql:42.4.0")
        .getOrCreate()
        )

jdbc_option = {
    'url': 'jdbc:postgresql://rc1a-fswjkpli01zafgjm.mdb.yandexcloud.net:6432/de',
    'driver': 'org.postgresql.Driver',
    'dbtable': 'marketing_companies',
    'user': 'student',
    'password': 'de-student',
}


df = spark.read.format('jdbc').options(**jdbc_option).load()
df.count()


df.printSchema()
print(df.head(10))