# Input:
# Customer table:
# +----+------+------------+
# | id | name | referee_id |
# +----+------+------------+
# | 1  | Will | null       |
# | 2  | Jane | null       |
# | 3  | Alex | 2          |
# | 4  | Bill | null       |
# | 5  | Zack | 1          |
# | 6  | Mark | 2          |
# +----+------+------------+
# Output:
# +------+
# | name |
# +------+
# | Will |
# | Jane |
# | Bill |
# | Zack |
# +------+

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,IntegerType,StringType

spark = SparkSession.builder.appName('test').getOrCreate()

schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("referee_id", IntegerType(), True)
])

data = [
    (1, "Will", None),
    (2, "Jane", None),
    (3, "Alex", 2),
    (4, "Bill", None),
    (5, "Zack", 1),
    (6, "Mark", 2)
]

Customer = spark.createDataFrame(data,schema = schema)
Customer.show()

# +---+----+----------+
# | id|name|referee_id|
# +---+----+----------+
# |  1|Will|      NULL|
# |  2|Jane|      NULL|
# |  3|Alex|         2|
# |  4|Bill|      NULL|
# |  5|Zack|         1|
# |  6|Mark|         2|
# +---+----+----------+

filter_df = Customer.where((Customer['referee_id'] != 2) | (Customer['referee_id'].isNull()))
filter_df.show()
#
# +---+----+----------+
# | id|name|referee_id|
# +---+----+----------+
# |  1|Will|      NULL|
# |  2|Jane|      NULL|
# |  4|Bill|      NULL|
# |  5|Zack|         1|
# +---+----+----------+

result_df = filter_df.select('name')
result_df.show()

# +----+
# |name|
# +----+
# |Will|
# |Jane|
# |Bill|
# |Zack|
# +----+