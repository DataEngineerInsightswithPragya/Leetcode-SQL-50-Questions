from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,IntegerType,StringType
from pyspark.sql.functions import length

spark = SparkSession.builder.appName('test').getOrCreate()

schema = StructType(
    [
        StructField('tweet_id',IntegerType(),True),
        StructField('content',StringType(),True)
    ]
)

data = [
    (1, 'Let us Code'),
    (2,'More than fifteen chars are here!')
]

tweets = spark.createDataFrame(data,schema)
tweets.show(truncate = False)

# +--------+---------------------------------+
# |tweet_id|content                          |
# +--------+---------------------------------+
# |1       |Let us Code                      |
# |2       |More than fifteen chars are here!|
# +--------+---------------------------------+

filter_df = tweets.where(length(tweets['content']) > 15)
filter_df.show()
# +--------+--------------------+
# |tweet_id|             content|
# +--------+--------------------+
# |       2|More than fifteen...|
# +--------+--------------------+

result_df = filter_df.select('tweet_id')
result_df.show()
# +--------+
# |tweet_id|
# +--------+
# |       2|
# +--------+