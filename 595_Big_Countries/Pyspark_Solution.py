# --Input:
# --World table:
# --+-------------+-----------+---------+------------+--------------+
# --| name        | continent | area    | population | gdp          |
# --+-------------+-----------+---------+------------+--------------+
# --| Afghanistan | Asia      | 652230  | 25500100   | 20343000000  |
# --| Albania     | Europe    | 28748   | 2831741    | 12960000000  |
# --| Algeria     | Africa    | 2381741 | 37100000   | 188681000000 |
# --| Andorra     | Europe    | 468     | 78115      | 3712000000   |
# --| Angola      | Africa    | 1246700 | 20609294   | 100990000000 |
# --+-------------+-----------+---------+------------+--------------+
# --Output:
# --+-------------+------------+---------+
# --| name        | population | area    |
# --+-------------+------------+---------+
# --| Afghanistan | 25500100   | 652230  |
# --| Algeria     | 37100000   | 2381741 |
# --+-------------+------------+---------+

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,IntegerType,StringType,LongType

spark = SparkSession.builder.appName('test').getOrCreate()

schema = StructType(
    [
    StructField('name',StringType(),True),
    StructField('continent',StringType(),True),
    StructField('area',LongType(),True),
    StructField('population',LongType(),True),
    StructField('gdp',LongType(),True)
    ]
)

data = [
    ("Afghanistan", "Asia", 652230, 25500100, 20343000000),
    ("Albania", "Europe", 28748, 2831741, 12960000000),
    ("Algeria", "Africa", 2381741, 37100000, 188681000000),
    ("Andorra", "Europe", 468, 78115, 3712000000),
    ("Angola", "Africa", 1246700, 20609294, 100990000000)
]


world = spark.createDataFrame(data,schema)
world.show()
# +-----------+---------+-------+----------+------------+
# |       name|continent|   area|population|         gdp|
# +-----------+---------+-------+----------+------------+
# |Afghanistan|     Asia| 652230|  25500100| 20343000000|
# |    Albania|   Europe|  28748|   2831741| 12960000000|
# |    Algeria|   Africa|2381741|  37100000|188681000000|
# |    Andorra|   Europe|    468|     78115|  3712000000|
# |     Angola|   Africa|1246700|  20609294|100990000000|
# +-----------+---------+-------+----------+------------+

filter_df = world.where((world['area'] >= 3000000) | (world['population'] >= 25000000))
filter_df.show()
# +-----------+---------+-------+----------+------------+
# |       name|continent|   area|population|         gdp|
# +-----------+---------+-------+----------+------------+
# |Afghanistan|     Asia| 652230|  25500100| 20343000000|
# |    Algeria|   Africa|2381741|  37100000|188681000000|
# +-----------+---------+-------+----------+------------+

result_df = filter_df.select('name','population','area')
result_df.show()
# +-----------+----------+-------+
# |       name|population|   area|
# +-----------+----------+-------+
# |Afghanistan|  25500100| 652230|
# |    Algeria|  37100000|2381741|
# +-----------+----------+-------+

