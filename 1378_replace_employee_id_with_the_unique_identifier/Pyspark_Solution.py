from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,IntegerType,StringType

spark = SparkSession.builder.appName('test').getOrCreate()

employeeuni_schema = StructType(
    [
        StructField('id',IntegerType(),True),
        StructField('unique_id',IntegerType(),True)
    ]
)

employee_schema = StructType(
    [
        StructField('id',IntegerType(),True),
        StructField('name',StringType(),True)
    ]
)


employees_data = [(1, 'Alice'), (7, 'Bob'), (11, 'Meir'), (90, 'Winston'), (3, 'Jonathan')]

employeeuni_data = [(3, 1), (11, 2), (90, 3)]


employeeuni = spark.createDataFrame(employeeuni_data,employeeuni_schema)

employee = spark.createDataFrame(employees_data,employee_schema)

employeeuni.show()
employee.show()

# +---+---------+
# | id|unique_id|
# +---+---------+
# |  3|        1|
# | 11|        2|
# | 90|        3|
# +---+---------+

# +---+--------+
# | id|    name|
# +---+--------+
# |  1|   Alice|
# |  7|     Bob|
# | 11|    Meir|
# | 90| Winston|
# |  3|Jonathan|
# +---+--------+


joined_df = employee.join(employeeuni,employee['id'] == employeeuni['id'],how = "left").drop(employeeuni['id'])
joined_df.show()
# +---+--------+---------+
# | id|    name|unique_id|
# +---+--------+---------+
# |  1|   Alice|     NULL|
# |  7|     Bob|     NULL|
# |  3|Jonathan|        1|
# | 90| Winston|        3|
# | 11|    Meir|        2|
# +---+--------+---------+

result_df = joined_df.select('unique_id','name')
result_df.show()
# +---------+--------+
# |unique_id|    name|
# +---------+--------+
# |     NULL|   Alice|
# |     NULL|     Bob|
# |        1|Jonathan|
# |        3| Winston|
# |        2|    Meir|
# +---------+--------+