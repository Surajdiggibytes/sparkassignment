import unittest
from pyspark.sql import SparkSession
from src.Assignment1 import util
def test_user_data(userDf,transactionDf):
    # Count of unique locations where each product is sold
    left_join = transactionDf.join(userDf, userDf.user_id == transactionDf.user_id, how="left")
    final_df = left_join.select(userDf['location ']).distinct()
    fin = final_df.count()
    # print(fin)
    return fin
    # products bought by each user.
    second = left_join.select(userDf['user_id'], transactionDf['product_description']).orderBy(transactionDf["user_id"])
    # second.show()
    return second
    # Total spending done by each user on each product
    third = transactionDf.select("user_id", "product_description", "price").orderBy(transactionDf['user_id'])
    # third.show()
    return third

class MyTestCase(unittest.TestCase):
    def test_case1(self):
        spark = SparkSession.builder.getOrCreate()
        test_userDf1 = spark.read.csv("C:\\Users\\Suraj\\Desktop\\sparkfiles\\test_user1.csv",
                                inferSchema=True, header=True)
        test_transactionDf1 = spark.read.csv("C:\\Users\\Suraj\\Desktop\\sparkfiles\\test_transaction1.csv",
                                       inferSchema=True, header=True)

        expected_output = test_user_data(test_userDf1,test_transactionDf1)
        x = util.user_data(test_userDf1,test_transactionDf1)
        self.assertEqual(x,expected_output)

    def test_case2(self):
        spark = SparkSession.builder.getOrCreate()
        test_userDf2 = spark.read.csv("C:\\Users\\Suraj\\Desktop\\sparkfiles\\test_user2.csv",
                                inferSchema=True, header=True)
        test_transactionDf2 = spark.read.csv("C:\\Users\\Suraj\\Desktop\\sparkfiles\\test_transaction2.csv",
                                       inferSchema=True, header=True)

        expected_output = test_user_data(test_userDf2,test_transactionDf2)
        x = util.user_data(test_userDf2,test_transactionDf2)
        self.assertEqual(x,expected_output)

    def test_case3(self):
        spark = SparkSession.builder.getOrCreate()
        test_userDf2 = spark.read.csv("C:\\Users\\Suraj\\Desktop\\sparkfiles\\test_user3.csv",
                                inferSchema=True, header=True)
        test_transactionDf2 = spark.read.csv("C:\\Users\\Suraj\\Desktop\\sparkfiles\\test_transaction3.csv",
                                       inferSchema=True, header=True)

        expected_output = test_user_data(test_userDf2,test_transactionDf2)
        x = util.user_data(test_userDf2,test_transactionDf2)
        self.assertEqual(x,expected_output)
if __name__ == '__main__':
    unittest.main()
