from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, current_timestamp
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType

# Ініціалізація SparkSession
spark = SparkSession.builder \
    .appName("Olympic Streaming Pipeline") \
    .config("spark.jars", "/mnt/c/Users/user/Documents/GitHub/goit-de-fp/task_1/mysql-connector-j-8.0.32.jar") \
    .getOrCreate()

# Читання даних з MySQL таблиці athlete_bio
print("Читання даних з MySQL таблиці athlete_bio...")
athlete_bio = spark.read.format("jdbc").options(
    url="jdbc:mysql://217.61.57.46:3306/olympic_dataset",
    driver="com.mysql.cj.jdbc.Driver",
    dbtable="athlete_bio",
    user="neo_data_admin",
    password="Proyahaxuqithab9oplp"
).load()
print("Дані з MySQL таблиці athlete_bio прочитані.")

# Фільтрація некоректних значень
athlete_bio = athlete_bio.filter(
    (col("height").isNotNull()) & (col("weight").isNotNull()) &
    (col("height").cast(FloatType()).isNotNull()) & (col("weight").cast(FloatType()).isNotNull())
)

# Читання даних з MySQL таблиці athlete_event_results
print("Читання даних з MySQL таблиці athlete_event_results...")
athlete_event_results = spark.read.format("jdbc").options(
    url="jdbc:mysql://217.61.57.46:3306/olympic_dataset",
    driver="com.mysql.cj.jdbc.Driver",
    dbtable="athlete_event_results",
    user="neo_data_admin",
    password="Proyahaxuqithab9oplp"
).load()
print("Дані з MySQL таблиці athlete_event_results прочитані.")

# Запис у Kafka топік athlete_event_results
print("Запис у Kafka топік athlete_event_results...")
athlete_event_results.selectExpr("to_json(struct(*)) AS value") \
    .write \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "athlete_event_results") \
    .save()
print("Дані записані в Kafka топік athlete_event_results.")

# Читання даних з Kafka
kafka_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "athlete_event_results") \
    .load()

# Декодування JSON
schema = StructType([
    StructField("athlete_id", IntegerType(), True),
    StructField("sport", StringType(), True),
    StructField("medal", StringType(), True),
    StructField("country_noc", StringType(), True)
])

event_results = kafka_stream.selectExpr("CAST(value AS STRING) as json") \
    .selectExpr("from_json(json, 'athlete_id INT, sport STRING, medal STRING, country_noc STRING') AS data") \
    .select("data.*")

# Об'єднання з біологічними даними
joined_data = event_results.join(athlete_bio, "athlete_id")

# Обчислення середніх значень
aggregated_data = joined_data.groupBy("sport", "medal", "sex", "country_noc") \
    .agg(
        avg("height").alias("avg_height"),
        avg("weight").alias("avg_weight")
    ) \
    .withColumn("timestamp", current_timestamp())

# Функція запису в Kafka та базу даних
def write_to_sinks(df, epoch_id):
    print("Запис у Kafka та базу даних...")
    df.selectExpr("to_json(struct(*)) AS value") \
        .write \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("topic", "aggregated_athlete_stats") \
        .save()
    
    df.write \
        .format("jdbc") \
        .option("url", "jdbc:mysql://217.61.57.46:3306/olympic_dataset") \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .option("dbtable", "aggregated_athlete_stats") \
        .option("user", "neo_data_admin") \
        .option("password", "Proyahaxuqithab9oplp") \
        .mode("append") \
        .save()
    print("Дані записані в Kafka та базу даних.")

# Налаштування стриму
query = aggregated_data.writeStream \
    .outputMode("update") \
    .foreachBatch(write_to_sinks) \
    .start()

query.awaitTermination()
