from pyspark.sql import SparkSession
from pyspark.sql.types import StringType,IntegerType,DateType

spark = SparkSession.builder.appName('test').getOrCreate()

schema = StructType(
    [
        StructField('article_id',IntegerType(),True),
        StructField('author_id',IntegerType(),True),
        StructField('viewer_id',IntegerType(),True),
        StructField('view_date',StringType(),True)
     ]
)

data = [
    (1, 3, 5, "2019-08-01"),
    (1, 3, 6, "2019-08-02"),
    (2, 7, 7, "2019-08-01"),
    (2, 7, 6, "2019-08-02"),
    (4, 7, 1, "2019-07-22"),
    (3, 4, 4, "2019-07-21"),
    (3, 4, 4, "2019-07-21")
]

views = spark.createDataFrame(data,schema)
views.show()
#
# +----------+---------+---------+----------+
# |article_id|author_id|viewer_id| view_date|
# +----------+---------+---------+----------+
# |         1|        3|        5|2019-08-01|
# |         1|        3|        6|2019-08-02|
# |         2|        7|        7|2019-08-01|
# |         2|        7|        6|2019-08-02|
# |         4|        7|        1|2019-07-22|
# |         3|        4|        4|2019-07-21|
# |         3|        4|        4|2019-07-21|
# +----------+---------+---------+----------+

filter_df = views.where(views['author_id'] == views['viewer_id'])
filter_df.show()
# +----------+---------+---------+----------+
# |article_id|author_id|viewer_id| view_date|
# +----------+---------+---------+----------+
# |         2|        7|        7|2019-08-01|
# |         3|        4|        4|2019-07-21|
# |         3|        4|        4|2019-07-21|
# +----------+---------+---------+----------+

drop_dup_df = filter_df.select('author_id').drop_duplicates()
drop_dup_df.show()

# +---------+
# |author_id|
# +---------+
# |        7|
# |        4|
# +---------+

# Step 3: Sort the values
asc_df = drop_dup_df.orderBy("author_id")

# Step 4: Rename column using withColumnRenamed
renamed_df = asc_df.withColumnRenamed("author_id", "id")

# Show the final DataFrame
print("\nFinal DataFrame:")
renamed_df.show()

# Final DataFrame:
# +---+
# | id|
# +---+
# |  4|
# |  7|
# +---+