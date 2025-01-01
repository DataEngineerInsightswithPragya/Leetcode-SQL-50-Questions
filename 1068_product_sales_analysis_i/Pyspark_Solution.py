from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,IntegerType,StringType

spark = SparkSession.builder.appName('test').getOrCreate()

sales_schema = StructType(
    [
    StructField('sales_id',IntegerType(),True),
    StructField('product_id',IntegerType(),True),
    StructField('year',IntegerType(),True),
    StructField('quantity',IntegerType(),True),
    StructField('price',IntegerType(),True)
    ]
)

sales_data = [
    (1, 100, 2008, 10, 5000),
    (2, 100, 2009, 12, 5000),
    (7, 200, 2011, 15, 9000)
]

product_data = [
    (100, "Nokia"),
    (200, "Apple"),
    (300, "Samsung")
]

product_schema = StructType([
    StructField("product_id", IntegerType(), True),
    StructField("product_name", StringType(), True)
])


sales = spark.createDataFrame(data = sales_data,schema = sales_schema)
product = spark.createDataFrame(data = product_data,schema = product_schema)

sales.show()
product.show()

#+--------+----------+----+--------+-----+
# |sales_id|product_id|year|quantity|price|
# +--------+----------+----+--------+-----+
# |       1|       100|2008|      10| 5000|
# |       2|       100|2009|      12| 5000|
# |       7|       200|2011|      15| 9000|
# +--------+----------+----+--------+-----+
#
# +----------+------------+
# |product_id|product_name|
# +----------+------------+
# |       100|       Nokia|
# |       200|       Apple|
# |       300|     Samsung|
# +----------+------------+

joined_df = sales.join(product, on = 'product_id', how = 'inner')
joined_df.show()
# +----------+--------+----+--------+-----+------------+
# |product_id|sales_id|year|quantity|price|product_name|
# +----------+--------+----+--------+-----+------------+
# |       100|       1|2008|      10| 5000|       Nokia|
# |       100|       2|2009|      12| 5000|       Nokia|
# |       200|       7|2011|      15| 9000|       Apple|
# +----------+--------+----+--------+-----+------------+

# joined_df = sales.join(product, sales['product_id'] == product['product_id'], how = 'inner')
# joined_df.show()
# +--------+----------+----+--------+-----+----------+------------+
# |sales_id|product_id|year|quantity|price|product_id|product_name|
# +--------+----------+----+--------+-----+----------+------------+
# |       1|       100|2008|      10| 5000|       100|       Nokia|
# |       2|       100|2009|      12| 5000|       100|       Nokia|
# |       7|       200|2011|      15| 9000|       200|       Apple|
# +--------+----------+----+--------+-----+----------+------------+

result_df = joined_df.select('product_name','year','price')
result_df.show()
# +------------+----+-----+
# |product_name|year|price|
# +------------+----+-----+
# |       Nokia|2008| 5000|
# |       Nokia|2009| 5000|
# |       Apple|2011| 9000|
# +------------+----+-----+