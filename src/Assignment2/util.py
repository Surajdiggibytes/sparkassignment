
from pyspark.sql.functions import split, col, desc


def client(logsDf):
    logsDf1 = logsDf.withColumn('logLevel', split(col('value'), ',')[0]) \
        .withColumn('timeStamp', split(col('value'), ',')[1]) \
        .withColumn("DownloaderId", split(col('value'), ',')[2]) \
        .withColumn("Downloader_ID", split(col('DownloaderId'), '--')[0]).drop('DownloaderID') \
        .withColumn("Ruby_Class", split(col('value'), '--')[1]).drop('value') \
        .withColumn("RubyClass", split(col('Ruby_Class'), ' ')[1]) \
        .withColumn("Comments", split(col('Ruby_Class'), ":")[1]).drop('Ruby_Class')
    # logsDf1.show(truncate=False)
    """
    count the number of lines DataFrame contains
    """
    count1 = logsDf1.count()
    return count1
    # print( logsDf1.count())

    """
    count of number of warning messages 
    """
    warn = logsDf1.filter(logsDf1.logLevel == 'WARN').count()
    return warn
    # print(warn)

    """
    Get the count of api_client repositories
    """
    repo = logsDf1.filter(logsDf1.RubyClass == 'api_client.rb:').count()
    return repo
    # print(repo)

    # logsDf1.filter("logsDf1.Comments == '%Successful request%' ").show()
    # logsDf1.filter(logsDf1.Comments == '%Successful request%').show()

    """
    which client did most HTTPS request
    """
    req = logsDf1.filter(col("Comments").contains("https")).groupBy('Downloader_ID').count()
    httpReq = req.select('Downloader_ID', 'count').orderBy(desc('count')).first()

    # print(httpReq['Downloader_ID'], httpReq['count'])
    return (httpReq['Downloader_ID'], httpReq['count'])

    """
    which client did most failed HTTPS request
    """
    failedReq = logsDf1.filter(col("Comments").contains("Failed request")).groupBy('Downloader_ID', 'Comments').count()
    # print(failedReq)
    df5 = failedReq.select('Downloader_ID', 'count').orderBy(desc('count')).first()
    # print(df5['Downloader_ID'], df5['count'])
    return (df5['Downloader_ID'], df5['count'])
