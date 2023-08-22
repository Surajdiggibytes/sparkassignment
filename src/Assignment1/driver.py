from pyspark.sql import SparkSession
from src.Assignment1 import util
spark = SparkSession.builder.getOrCreate()

userDf = spark.read.csv("C:\\Users\\SurajJ\\PycharmProjects\\sparkassignmentnew\\files\\user.csv",inferSchema=True,header=True)
transactionDf = spark.read.csv("C:\\Users\\SurajJ\\PycharmProjects\\sparkassignmentnew\\files\\transaction.csv",inferSchema=True,header=True)
#util.user_data(userDf,transactionDf)
#util.products(transactionDf)
util.spending(transactionDf)





