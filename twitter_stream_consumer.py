import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.1 pyspark-shell'


if __name__ == "__main__":

    bootstrapServers = "localhost:9092"
    topics = "tweets"

    spark = SparkSession\
            .builder\
            .appName("StructuredKafkaTweetsCount")\
            .getOrCreate()

    # Disable log info
    sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    # Create DataSet representing the stream of input lines from kafka
    lines = spark\
        .readStream\
        .format("kafka")\
        .option("kafka.bootstrap.servers", bootstrapServers)\
        .option("subscribe", topics)\
        .load()\
        .selectExpr("CAST(value AS STRING)")

    # Split the lines into words
    words = lines.select(
        # explode turns each item in an array into a separate row
        explode(
            split(lines.value, ' ')
        ).alias('hashtag')
    )

    # filter the words to get only hashtags
    hashtags = words.filter("hashtag LIKE '#%'")

    # adding the count of each hashtag to its last count
    wordCounts = hashtags.groupBy('hashtag').count()


    # Start running the query that prints the running counts to the console
    query = wordCounts \
        .writeStream \
        .outputMode('complete') \
        .format('console') \
        .start()

    # wait for the streaming to finish
    query.awaitTermination()