from pyspark.sql import SparkSession
from pyspark.sql.functions import split,col,desc
from util import *
spark = SparkSession.builder.master("local[5]").appName("Spark Assignment").getOrCreate()
logsDf = spark.read.text("C:\\Users\\SurajJ\\PycharmProjects\\sparkassignmentnew\\files\\ghtorrent-logs.txt")
# print( logs.count()) # count the number of lines DataFrame contains
# # logs.show(n= 30,truncate=False)
fun1 = client(logsDf)
fun2 = counting(fun1)
fun3 = warning(fun1)
fun4 = repo(fun3)
fun5 = request(fun4)
# fun6 = failRequest(fun5)














