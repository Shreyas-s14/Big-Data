#the first attempt to run a basic spark program:
# goal is to Read a stream from the localhost
#Reference: Learning spark- Databricks/O-Reily

from pyspark import SparkContext
from pyspark.sql import SparkSession

from pyspark.sql.functions import explode, split
sc = SparkContext('local')
spark = SparkSession(sc)
lines = (spark
.readStream
.format("socket")
.option("host", "localhost")
.option("port", 9999)
.load())

#Now, perform the required transformation on the collected dataset:
words=lines.select(explode(split(lines.value," ")).alias("word"))

#Word-count
word_count=words.groupBy("word").count()


#Write the out to stream to kafka:
query=(word_count
.writeStream
.format("kafka")
.option("topic","output"))