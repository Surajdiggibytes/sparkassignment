from pyspark.sql import SparkSession
from pyspark.sql.functions import split,col,desc
from util import *
spark = SparkSession.builder.master("local[5]").appName("Spark Assignment").getOrCreate()
logsDf = spark.read.text("C:\\Users\\Suraj\\Desktop\\sparkfiles\\ghtorrent-logs.txt")
# print( logs.count()) # count the number of lines DataFrame contains
# # logs.show(n= 30,truncate=False)
client(logsDf)













