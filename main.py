from pyspark.sql import SparkSession
builder = SparkSession.builder.appName("etl-yelp-pandas-json")
builder = builder.config("spark.sql.execution.arrow.pyspark.enabled", "true")
builder.getOrCreate()
print(builder)
