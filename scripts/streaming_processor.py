from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_json
from pyspark.sql.types import StructType, StringType, IntegerType, ArrayType


json_schema = StructType() \
    .add("application_id", StringType()) \
    .add("customer", StructType()
         .add("customer_id", StringType())
         .add("region", StringType())) \
    .add("loan", StructType()
         .add("amount", IntegerType())
         .add("term_months", IntegerType())) \
    .add("scoring", StructType()
         .add("score", IntegerType())
         .add("risk_level", StringType())) \
    .add("documents", ArrayType(StructType()
         .add("type", StringType())
         .add("status", StringType()))) \
    .add("decision_status", StringType()) \
    .add("submitted_at", StringType())

# Подключение к Kafka (замени FQDN и пароль!)
kafka_brokers = "rc1a-13m8vuuiie3pfbse.mdb.yandexcloud.net:9091"
kafka_topic = "loan-json-topic"
username = "producer"
password = "Ekaterina2003!"
ca_path = "s3a://dp-scripts-pavlova/YandexInternalRootCA.crt"

spark = SparkSession.builder \
    .appName("KafkaStreamProcessor") \
    .getOrCreate()

raw_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_brokers) \
    .option("subscribe", kafka_topic) \
    .option("startingOffsets", "latest") \
    .option("kafka.security.protocol", "SASL_SSL") \
    .option("kafka.ssl.truststore.location", ca_path) \
    .option("kafka.ssl.truststore.type", "PEM") \
    .option("kafka.sasl.mechanism", "SCRAM-SHA-512") \
    .option("kafka.sasl.jaas.config",
            f'org.apache.kafka.common.security.scram.ScramLoginModule required username="{username}" password="{password}";') \
    .load()

        
# Парсим JSON и разворачиваем (flatten)
parsed_df = raw_stream \
    .select(from_json(col("value").cast("string"), json_schema).alias("data")) \
    .select(
        col("data.application_id"),
        col("data.customer.customer_id").alias("customer_id"),
        col("data.customer.region").alias("region_code"),
        col("data.loan.amount").alias("loan_amount"),
        col("data.loan.term_months").alias("term_months"),
        col("data.scoring.score").alias("credit_score"),
        col("data.scoring.risk_level").alias("risk_level"),
        col("data.decision_status"),
        col("data.submitted_at"),
        to_json(col("data.documents")).alias("documents_json")
    )

# Вывод в консоль (для отладки) + запись в Parquet в бакет
query = parsed_df.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", "s3a://dp-data-pavlova/streaming_output/") \
    .option("checkpointLocation", "s3a://dp-data-pavlova/checkpoints/") \
    .start()

query.awaitTermination()