### Getting Started
* Install Python and Pip
* Install dependecies: pip install psutil && pip install tweepy && pip install websockets
* Make sure you have Apache Spark installed. 
* Get your API keys from [https://dev.twitter.com/](Twitter Developers) and put them in ``twitter_config.py``.
* Set docker in your etc/hosts to point to your machine


### How to Run the Example?

First, run the Kafka server with the following command:

```bash
docker-compose up -d 

```
Then, fire up the stream source:

```bash
twitter_stream_producer.py
```

Now submit the ```twitter_stream_consumer.py``` to ```spark-submit```:

```bash
$SPARK_HOME/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.1 twitter_stream_consumer.py
```

You will start to see the most frequently used words in the tweets from your opened stream like this:
```python
+------------------+-----+
|           hashtag|count|
+------------------+-----+
|#programming\u2026|    1|
|     #Spark\u2026"|    1|
|     #RunWayv_CHEN|    1|
|          #Jupyter|    1|
|            #Learn|    1|
|       #opensource|    1|
|           #Apache|    1|
+------------------+-----+

```
