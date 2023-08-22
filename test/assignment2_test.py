import unittest
from src.Assignment2 import util
from pyspark.sql import SparkSession
from pyspark.sql.functions import split,col,desc


def client_test(logsDf_test):
    logsDf1 = logsDf_test.withColumn('logLevel', split(col('value'), ',')[0]) \
        .withColumn('timeStamp', split(col('value'), ',')[1]) \
        .withColumn("DownloaderId", split(col('value'), ',')[2]) \
        .withColumn("Downloader_ID", split(col('DownloaderId'), '--')[0]).drop('DownloaderID') \
        .withColumn("Ruby_Class", split(col('value'), '--')[1]).drop('value') \
        .withColumn("RubyClass", split(col('Ruby_Class'), ' ')[1]) \
        .withColumn("Comments", split(col('Ruby_Class'), ":")[1]).drop('Ruby_Class')
    return logsDf1

def counting_test(logsDf1):
    count1 = logsDf1.count()
    return count1

def warning_test(logsDf1):
    warn = logsDf1.filter(logsDf1.logLevel == 'WARN').count()
    #print(warn)
    return warn

def repo_test(logsDf1):
    repo = logsDf1.filter(logsDf1.RubyClass == 'api_client.rb:').count()
    return repo

def request_test(logsDf1):
    req = logsDf1.filter(col("Comments").contains("https")).groupBy('Downloader_ID').count()
    httpReq = req.select('Downloader_ID', 'count').orderBy(desc('count')).first()

    # print(httpReq['Downloader_ID'], httpReq['count'])
    return (httpReq['Downloader_ID'], httpReq['count'])

def failRequest_test(logsDf1):
    failedReq = logsDf1.filter(col("Comments").contains("Failed request")).groupBy('Downloader_ID', 'Comments').count()
    # print(failedReq)
    df5 = failedReq.select('Downloader_ID', 'count').orderBy(desc('count')).first()
    # print(df5['Downloader_ID'], df5['count'])
    return (df5['Downloader_ID'], df5['count'])
spark = SparkSession.builder.master("local[5]").appName("Spark Assignment").getOrCreate()
test_logsDf = spark.read.text("C:\\Users\\SurajJ\\PycharmProjects\\sparkassignmentnew\\files\\ghtorrent-logs.txt")
fun1 = client_test(test_logsDf)
fun2 = counting_test(fun1)
fun3 = warning_test(fun2)
fun4 = repo_test(fun3)
fun5 = request_test(fun4)
fun6 = failRequest_test(fun5)

class MyTestCase(unittest.TestCase):
    def test_case1(self):
        expected_output = client_test(test_logsDf)
        actual_output = util.client(test_logsDf)
        self.assertEqual(actual_output.collect(),expected_output.collect())

    def test_case2(self):
        expected_output = counting_test(fun1)
        actual_output = util.counting(fun1)
        self.assertEqual(actual_output,expected_output)

    def test_case3(self):
        expected_output = warning_test(fun1)
        actual_output = util.warning(fun1)
        self.assertEqual(actual_output,expected_output)

    def test_case4(self):
        expected_output = repo_test()(fun3)
        actual_output = util.repo(fun3)
        self.assertEqual(actual_output,expected_output)
    #
    def test_case5(self):
        expected_output = request_test(fun4)
        actual_output = util.request(fun4)
        self.assertEqual(actual_output,expected_output)
    #
    def test_case6(self):
        expected_output = failRequest_test(fun5)
        actual_output = util.failRequest(fun5)
        self.assertEqual(actual_output,expected_output)


if __name__ == '__main__':
    unittest.main()
