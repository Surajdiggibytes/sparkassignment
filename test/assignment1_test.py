import unittest
from src.Assignment1 import util
from pyspark.sql import SparkSession


class MyTestCase(unittest.TestCase):
    def testcase1(self):
        spark = SparkSession.builder.getOrCreate()
        test_userDf1 = spark.read.csv("C:\\Users\\SurajJ\\PycharmProjects\\sparkassignmentnew\\files\\user.csv",inferSchema=True, header=True)
        test_transactionDf1 = spark.read.csv("C:\\Users\\SurajJ\\PycharmProjects\\sparkassignmentnew\\files\\transaction.csv",inferSchema=True, header=True)
        expected_output = 3
        actual_output = util.user_data(test_userDf1, test_transactionDf1)
        self.assertEqual(actual_output,expected_output)

    def testcase2(self):

        spark = SparkSession.builder.getOrCreate()
        test_transactionDf1 = spark.read.csv("C:\\Users\\SurajJ\\PycharmProjects\\sparkassignmentnew\\files\\transaction.csv", inferSchema=True,header=True)
        data = [(101,'mouse'),(101,'fridge'),(101,'speaker'),(102,'keyboard'),(102,'chair'),(103,'tv'),(105,'sofa'),(105,'laptop'),(106,'bed'),(108,'phone')]
        schema = ['user_id','product_description']
        expected_output = spark.createDataFrame(data,schema)
        actual_output = util.products(test_transactionDf1)
        self.assertEqual(actual_output.collect(),expected_output.collect())

    def testcase3(self):

        spark = SparkSession.builder.getOrCreate()
        test_transactionDf1 = spark.read.csv("C:\\Users\\SurajJ\\PycharmProjects\\sparkassignmentnew\\files\\transaction.csv", inferSchema=True,header=True)
        data = [(101,'mouse',700),(101,'fridge',35000),(101,'speaker',500),(102,'keyboard',900),(102,'chair',1000),(103,'tv',34000),(105,'sofa',55000),(105,'laptop',66000),(106,'bed',100),(108,'phone',20000)]
        schema = ['user_id','product_description','price']
        expected_output = spark.createDataFrame(data,schema)
        actual_output = util.spending(test_transactionDf1)
        self.assertEqual(actual_output.collect(), expected_output.collect())



if __name__ == '__main__':
    unittest.main()
