# Imported necessary functions

from pyspark.sql import SparkSession
from pyspark.sql.functions import col,isnan, when, count,sum
import os
import sys
spark = SparkSession.builder \
.config('spark.executer.memory','4g') \
.config('spark.driver.memory','4g') \
.config('spark.offHeap.size','50g') \
.config('spark.executer.enabled','true') \
.appName("bigdata")\
.getOrCreate();

#1.Read a CSV and print output to console
# I took the data from kaggle website
# https://www.kaggle.com/datasets/ravindrasinghrana/job-description-dataset

df_csv = spark.read.csv("F:/data/job_descriptions.csv")
df_csv.show(5)
# Only showing 5 rows here

# 2. Read a JSON file and print output to console
# I took the data from kaggle website
#https://www.kaggle.com/datasets/prathamsharma123/farmers-protest-tweets-dataset-raw-json

df_json = spark.read.json("F:/data/farmers-protest-tweets-2021-03-5.json")
df_json.show(5)
# Same printing only top 5 rows here

# 3 Read parquet file and print output to console
# I took the data from kaggle website
# https://www.kaggle.com/datasets/uadithyan/sleep-states-dataset

df_parquet = spark.read.parquet("F:/data/CMI_sleep_data_updated.parquet")
df_parquet.show(5)

# 4.Read a avro file and print output to console 
# I am getting error for this one
# this is the error I am getting
"""---------------------------------------------------------------------------
AnalysisException                         Traceback (most recent call last)
Cell In[44], line 1
----> 1 df = spark.read.format('avro').load("F:/data/userdata1.avro")

File ~\AppData\Roaming\Python\Python310\site-packages\pyspark\sql\readwriter.py:307, in DataFrameReader.load(self, path, format, schema, **options)
    305 self.options(**options)
    306 if isinstance(path, str):
--> 307     return self._df(self._jreader.load(path))
    308 elif path is not None:
    309     if type(path) != list:

File ~\AppData\Roaming\Python\Python310\site-packages\py4j\java_gateway.py:1322, in JavaMember.__call__(self, *args)
   1316 command = proto.CALL_COMMAND_NAME +\
   1317     self.command_header +\
   1318     args_command +\
   1319     proto.END_COMMAND_PART
   1321 answer = self.gateway_client.send_command(command)
-> 1322 return_value = get_return_value(
   1323     answer, self.gateway_client, self.target_id, self.name)
   1325 for temp_arg in temp_args:
   1326     if hasattr(temp_arg, "_detach"):

File ~\AppData\Roaming\Python\Python310\site-packages\pyspark\errors\exceptions\captured.py:185, in capture_sql_exception.<locals>.deco(*a, **kw)
    181 converted = convert_exception(e.java_exception)
    182 if not isinstance(converted, UnknownException):
    183     # Hide where the exception came from that shows a non-Pythonic
    184     # JVM exception message.
--> 185     raise converted from None
    186 else:
    187     raise

AnalysisException: Failed to find data source: avro. Avro is built-in but external data source module since Spark 2.4.
Please deploy the application as per the deployment section of Apache Avro Data Source Guide."""

# 5 Example for broadcast join (Inner join 2 dataframes)
# https://github.com/MainakRepositor/Datasets/tree/master/Elon%20Tweets

from pyspark.sql.functions import broadcast
# First reading the 2022.csv and 2021.csv into a dataframe

df_2020Tweets = spark.read.csv("F:/data/2020.csv", header=True, inferSchema=True)
df_2021Tweets = spark.read.csv("F:/data/2021.csv", header=True, inferSchema=True)
df_2021Tweets.show(5)

joined_df = df_2020Tweets.join(broadcast(df_2021Tweets), df_2020Tweets["id"] == df_2021Tweets["id"], "inner")

result_df = joined_df.select(df_2020Tweets["*"], df_2021Tweets["tweet"])
result_df.show(5)

# 6. Example for Filtering the data 
filtered_df = df_parquet.filter(df_parquet["enmo"] > 0.09)
filtered_df.show()
filtered_df1 = df_parquet.filter(df_parquet["step"] > 50)
filtered_df1.show()

# 7. Example for applying aggregate functions like  max, min, avg 
from pyspark.sql.functions import max, min,avg
df_parquet.agg(max("enmo"), min("enmo"), avg("enmo")).show()

# 8. Example for Read json file with typed schema (without infering the schema) with Structtype, StructField .....
from pyspark.sql.types import StructType, StructField, StringType,IntegerType,DateType

schema = StructType([
    StructField("summary", StringType(), True),
    StructField("content", StringType(), True),
    StructField("conversationId", StringType(), True),
    StructField("date", StringType(), True),
    StructField("id", StringType(), True),
    StructField("lang", StringType(), True),
    StructField("likeCount", StringType(), True),
    StructField("quoteCount", StringType(), True),
    StructField("renderedContent", StringType(), True),
    StructField("replyCount", StringType(), True),
    StructField("retweetCount", StringType(), True),
    StructField("retweetedTweet", StringType(), True),
    StructField("source", StringType(), True),
    StructField("sourceLabel", StringType(), True),
    StructField("sourceUrl", StringType(), True),
    StructField("url", StringType(), True)
])

df_json_defined_schema = spark.read.schema(schema).json("F:/data/farmers-protest-tweets-2021-03-5.json")
df_json_defined_schema.show(5)

# 9. Example for increase and decrease number of dataframe partitions 
df_parquet_increase = df_parquet.repartition(15)
df_parquet_increase.show() # for increase

df_parquet_decrease = df_parquet.coalesce(2)
df_parquet_decrease.show() # for decrease

# 10. Example for renaming the column of the dataframe 
df_parquet_rename = df_parquet.withColumnRenamed("step", "step_renamed")
df_parquet_rename.show()

# 11. Example for adding a new column to the dataframe 
df_parquet_addcolumn = df_parquet
df_parquet_addcolumn = df_parquet.withColumn("new_enmo_column_percent", col("enmo") * 100) 
df_parquet_addcolumn.show()

# 12. Changing the structure of the dataframe
df_parquet.printSchema()

df_parquet_modified = df_parquet.drop("timestamp").drop("series_id")
df_parquet_modified = df_parquet_modified.withColumn("enmo", col("enmo").cast(StringType()))
df_parquet_modified.show()
df_parquet_modified.printSchema()

# Json file structure should look like 
json_data = [
    '{"name" : "john doe", "dob" : "01-01-1970", "phone" : "+234567890", "salary" : 57000, "location" : "New York", "sex" : "male"}',
    '{"name" : "john adam", "dob" : "02-01-1990", "phone" : "+6634567890", "salary" : 69000, "location" : "Los Angeles", "sex" : "male"}',
    '{"name" : "jane frank", "dob" : "01-11-1999", "phone" : "+9876543210", "salary" : 70000, "location" : "Chicago", "sex" : "female"}',
    '{"name" : "alice hillary", "dob" : "01-12-1975","phone" :"+4534567890", "salary" : 80000, "location" : "Houston", "sex" : "female"}',
    '{"name" : "bob kim", "dob" : "01-01-1985", "phone" : "+5675555555", "salary" : 10000, "location" : "Phoenix", "sex" : "male"}',
    '{"name" : "felister malito", "dob" : "01-01-1995", "phone" : "+9234567890", "salary" : 65000, "location" : "San Francisco", "sex" : "male"}',
    '{"name" : "davison steve", "dob" : "05-01-1970", "phone" : "+299834444", "salary" : 75000, "location" : "Miami", "sex" : "male"}',
    '{"name" : "emmanuel pinner", "dob" : "06-01-1982", "phone" : "+541293333", "salary" : 82000, "location" : "Boston", "sex" : "female"}',
    '{"name" : "frank omega", "dob" : "01-08-1978", "phone" : "+5673885674", "salary" : 78000, "location" : "Denver", "sex" : "male"}',
    '{"name" : "gaddiom tom", "dob" : "01-09-1992", "phone" : "+6754377779", "salary" : 97000, "location" : "Seattle", "sex" : "female"}'
]

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_json, struct, lit

spark = SparkSession.builder \
    .appName("JSON") \
    .getOrCreate()

# Converting list to RDD format
rdd = spark.sparkContext.parallelize(json_data)

from pyspark.sql.types import StructType, StructField, StringType,IntegerType,DateType
schema = StructType([
    StructField("name", StringType(), True),
    StructField("dob", StringType(), True),
    StructField("phone", StringType(), True),
    StructField("salary", IntegerType(), True),
    StructField("location", StringType(), True),
    StructField("sex", StringType(), True)
])

# Convert RDD to DataFrame
rdddf = spark.read.json(rdd, schema=schema)

from pyspark.sql.functions import struct
df_restructured = rdddf.select(to_json(struct(
    struct(col("name"), col("dob"), col("phone")).alias("personal_data")
)).alias("json_string"))

df_restructured.show(truncate=False)
output_path = "personal_data.json"
df_restructured.write.json(output_path, mode="overwrite")