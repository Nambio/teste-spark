from pyspark.sql import SparkSession
from datetime import datetime, date
from pyspark.sql import Row

# spark = SparkSession.builder.master("spark://10.206.0.2:7077").getOrCreate()

# spark = SparkSession.builder.remote("sc://10.206.0.2:34753").getOrCreate()

spark = SparkSession.builder.getOrCreate()

# Create DataFrame 
columns = ["id", "name","age","gender"]
data = [(1, "James",30,"M"), (2, "Ann",40,"F"),
    (3, "Jeff",41,"M"),(4, "Jennifer",20,"F")]

sampleDF = spark.sparkContext.parallelize(data).toDF(columns)

# sampleDF.write \
#   .format("jdbc") \
#   .option("driver","com.mysql.cj.jdbc.Driver") \
#   .option("url", "jdbc:mysql://10.182.0.3:3306/dataspark") \
#   .option("dbtable", "personas") \
#   .option("user", "root") \
#   .option("password", "admin123") \
#   .save()

sampleDF.write \
  .format("jdbc") \
  .option("driver","com.mysql.cj.jdbc.Driver") \
  .option("url", "jdbc:mysql://34.125.46.6:3306/dataspark") \
  .option("dbtable", "personas") \
  .option("user", "root") \
  .option("password", "admin123") \
  .save()

  