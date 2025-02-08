from pyspark.sql import SparkSession
from pyspark.sql.functions import window, count, from_json, col, to_timestamp, expr
from pyspark.sql.types import StructType, StructField, StringType, LongType
import time
import os
from main_publisher import publish_data_to_kafka  # Import the main publisher
import signal

# Set JAVA_HOME if required by your system
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64"

# Global variable to store the total number of emojis received
total_emojis_received = 0

def create_spark_session():
    """Create and configure Spark session for micro-batch streaming"""
    return SparkSession.builder \
        .appName("EmojiStreamProcessor") \
        .config("spark.streaming.stopGracefullyOnShutdown", "true") \
        .config("spark.sql.shuffle.partitions", "10") \
        .config("spark.streaming.kafka.maxRatePerPartition", "10000") \
        .config("spark.streaming.kafka.minBatchesToRetain", "2") \
        .config("spark.streaming.backpressure.enabled", "true") \
        .config("spark.streaming.kafka.consumer.cache.enabled", "false") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3") \
        .getOrCreate()

def create_kafka_read_stream(spark):
    """Create input stream from Kafka with micro-batch settings"""
    return spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "emoji_topic") \
        .option("startingOffsets", "latest") \
        .option("maxOffsetsPerTrigger", 5000) \
        .option("fetchOffset.numRetries", "3") \
        .load()

def process_emoji_stream():
    """Process emoji streaming data in micro-batches"""
    global total_emojis_received  # Use global variable to track total emojis
    spark = create_spark_session()
    
    # Define schema for JSON data
    schema = StructType([ 
        StructField("user_id", StringType(), True),
        StructField("emoji_type", StringType(), True),
        StructField("timestamp", LongType(), True)
    ])
    
    # Read from Kafka
    kafka_df = create_kafka_read_stream(spark)
    
    # Parse JSON data and handle timestamps
    parsed_df = kafka_df.select(
        from_json(col("value").cast("string"), schema).alias("data")
    ).select("data.*")
    
    # Convert millisecond timestamp to proper timestamp type
    df_with_timestamp = parsed_df.withColumn(
        "event_time",
        to_timestamp(col("timestamp") / 1000)
    )
    
    # Process in 2-second windows with micro-batching
    windowed_counts = df_with_timestamp \
        .withWatermark("event_time", "2 seconds") \
        .groupBy(
            window("event_time", "2 seconds"),
            "emoji_type"
        ) \
        .agg(count("*").alias("emoji_count"))
    
    # Calculate the scaled count
    scaled_counts = windowed_counts \
        .withColumn(
            "scaled_count",
            expr("CASE WHEN emoji_count >= 100 THEN emoji_count/100 ELSE 0 END")
        )
    
    # Filter out records with scaled_count < 1
    filtered_counts = scaled_counts.filter(col("scaled_count") >= 1)
    
    # Prepare output with window information
    output_df = filtered_counts.select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        "emoji_type",
        "emoji_count",
        "scaled_count"
    )
    
    # Update the total emojis received for each batch
    def update_total_emojis(batch_df, batch_id):
        global total_emojis_received
        batch_count = batch_df.agg({"emoji_count": "sum"}).collect()[0][0]
        if batch_count is not None:
            total_emojis_received += batch_count

    # Write to console for monitoring
    console_query = output_df \
        .writeStream \
        .format("console") \
        .outputMode("update") \
        .trigger(processingTime="2 seconds") \
        .option("truncate", False) \
        .start()
    
    # Write to Kafka topic "processed_emojis"
    kafka_query = output_df \
        .selectExpr("to_json(struct(*)) as value") \
        .writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("topic", "processed_emojis") \
        .option("checkpointLocation", "/tmp/checkpoint/") \
        .outputMode("update") \
        .trigger(processingTime="2 seconds") \
        .start()

    # Publish the aggregated data to the main publisher in each batch
    def process_and_publish_batch(batch_df, batch_id):
        aggregated_data = batch_df.collect()  # Collect the data from the current batch
        # Publish each record to Kafka using the main publisher
        for row in aggregated_data:
            data = {
                "window_start": str(row.window_start),
                "window_end": str(row.window_end),
                "emoji_type": row.emoji_type,
                "emoji_count": row.emoji_count,
                "scaled_count": row.scaled_count
            }
            publish_data_to_kafka(data)

    # Stream processing with batch publishing
    monitoring_query = output_df \
        .writeStream \
        .foreachBatch(lambda df, id: (update_total_emojis(df, id), process_and_publish_batch(df, id))) \
        .trigger(processingTime="2 seconds") \
        .start()

    def handle_exit(sig, frame):
        """Handle termination signal and print the total emojis received"""
        global total_emojis_received
        print(f"\n[INFO] Total emojis received: {total_emojis_received}")
        console_query.stop()
        kafka_query.stop()
        monitoring_query.stop()
        spark.stop()
        exit(0)

    # Attach signal handlers for graceful termination
    signal.signal(signal.SIGINT, handle_exit)
    signal.signal(signal.SIGTERM, handle_exit)

    try:
        spark.streams.awaitAnyTermination()
    except Exception as e:
        print(f"[ERROR] {e}")
        handle_exit(None, None)

if __name__ == "__main__":
    process_emoji_stream()
