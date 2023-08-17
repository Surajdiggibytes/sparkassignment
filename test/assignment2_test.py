import unittest
from src.Assignment2 import util
from pyspark.sql import SparkSession
from pyspark.sql.functions import split, col, desc
def test_client(logsDf):
    logsDf1 = logsDf.withColumn('logLevel', split(col('value'), ',')[0]) \
        .withColumn('timeStamp', split(col('value'), ',')[1]) \
        .withColumn("DownloaderId", split(col('value'), ',')[2]) \
        .withColumn("Downloader_ID", split(col('DownloaderId'), '--')[0]).drop('DownloaderID') \
        .withColumn("Ruby_Class", split(col('value'), '--')[1]).drop('value') \
        .withColumn("RubyClass", split(col('Ruby_Class'), ' ')[1]) \
        .withColumn("Comments", split(col('Ruby_Class'), ":")[1]).drop('Ruby_Class')
    # logsDf1.show(truncate=False)

    count1 = logsDf1.count()
    return count1

    warn = logsDf1.filter(logsDf1.logLevel == 'WARN').count()
    return warn


    repo = logsDf1.filter(logsDf1.RubyClass == 'api_client.rb:').count()
    return repo

    req = logsDf1.filter(col("Comments").contains("https")).groupBy('Downloader_ID').count()
    httpReq = req.select('Downloader_ID', 'count').orderBy(desc('count')).first()
    return (httpReq['Downloader_ID'], httpReq['count'])

    failedReq = logsDf1.filter(col("Comments").contains("Failed request")).groupBy('Downloader_ID', 'Comments').count()
    df5 = failedReq.select('Downloader_ID', 'count').orderBy(desc('count')).first()
    return (df5['Downloader_ID'], df5['count'])

class MyTestCase(unittest.TestCase):
    def test_case1(self):
        spark = SparkSession.builder.master("local[5]").appName("Spark Assignment").getOrCreate()
        test_logsDf = spark.read.text("C:\\Users\\Suraj\\Desktop\\sparkfiles\\test1_ghtorrent-logs.txt")
        expected_output = test_client(test_logsDf)
        x = util.client(test_logsDf)
        self.assertEqual(x,expected_output)

    def test_case2(self):
        spark = SparkSession.builder.master("local[5]").appName("Spark Assignment").getOrCreate()
        test_logsDf = spark.read.text("C:\\Users\\Suraj\\Desktop\\sparkfiles\\test2_ghtorrent-logs.txt")
        expected_output = test_client(test_logsDf)
        x = util.client(test_logsDf)
        self.assertEqual(x,expected_output)

    def test_case3(self):
        spark = SparkSession.builder.master("local[5]").appName("Spark Assignment").getOrCreate()
        test_logsDf = spark.read.text("C:\\Users\\Suraj\\Desktop\\sparkfiles\\test3_ghtorrent-logs.txt")
        expected_output = test_client(test_logsDf)
        x = util.client(test_logsDf)
        self.assertEqual(x, expected_output)

if __name__ == '__main__':
    unittest.main()
