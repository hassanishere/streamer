from time import sleep
from openai import OpenAI
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, when, udf, lit
from pyspark.sql.types import StructType, StructField, StringType
from config.config import config

# Load API key once
openai_api_key = config['openai']['api_key']

# Define UDF-compatible function
def sentiment_analysis(comment: str) -> str:
    if comment:
        try:
            # Client inside the function to avoid serialization issues
            client = OpenAI(api_key=openai_api_key)
            response = client.chat.completions.create(
                model='gpt-3.5-turbo',
                messages=[
                    {
                        "role": "system",
                        "content": f"""
                        You're a machine learning model with a task of classifying comments into POSITIVE, NEGATIVE, NEUTRAL.
                        You are to respond with one word from the option specified above, do not add anything else.
                        Here is the comment: {comment}
                        """
                    }
                ]
            )
            return response.choices[0].message.content.strip()
        except Exception as e:
            print(f"OpenAI API error: {e}")
            return "ERROR"
    return "Empty"

def start_streaming(spark):
    topic = 'customer_reviews'

    while True:
        try:
            # Step 1: Read from socket
            stream_df = (
                spark.readStream
                    .format("socket")
                    .option("host", "host.docker.internal")
                    .option("port", 9999)
                    .load()
            )

            # Step 2: Define minimal schema for the incoming JSON
            schema = StructType([
                StructField("text", StringType())
            ])

            # Step 3: Parse the JSON line into a DataFrame
            parsed_df = stream_df.select(from_json(col("value"), schema).alias("data")).select("data.*")

            # Step 4: Register UDF
            sentiment_udf = udf(sentiment_analysis, StringType())

            # Step 5: Apply sentiment classification
            enriched_df = parsed_df.withColumn(
                "feedback",
                when(col("text").isNotNull(), sentiment_udf(col("text"))).otherwise(None)
            )

            # Step 6: Convert to Kafka-compatible format (optional key, can be dummy)
            kafka_df = enriched_df.withColumn("key", lit("review")).selectExpr(
                "CAST(key AS STRING)", "to_json(struct(*)) AS value"
            )

            # Step 7: Write to Kafka
            query = (
                kafka_df.writeStream
                    .format("kafka")
                    .option("kafka.bootstrap.servers", config['kafka']['bootstrap.servers'])
                    .option("kafka.security.protocol", config['kafka']['security.protocol'])
                    .option("kafka.sasl.mechanism", config['kafka']['sasl.mechanisms'])
                    .option("kafka.sasl.jaas.config",
                            f'org.apache.kafka.common.security.plain.PlainLoginModule required '
                            f'username="{config["kafka"]["sasl.username"]}" '
                            f'password="{config["kafka"]["sasl.password"]}";')
                    .option("checkpointLocation", "/tmp/checkpoint")
                    .option("topic", topic)
                    .start()
                    .awaitTermination()
            )

        except Exception as e:
            print(f"Exception encountered: {e}. Retrying in 10 seconds...")
            sleep(10)

if __name__ == "__main__":
    spark = SparkSession.builder.appName("SocketStreamConsumer").getOrCreate()
    start_streaming(spark)
