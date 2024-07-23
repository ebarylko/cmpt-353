from pyspark.sql import SparkSession, types, DataFrame, functions, Row
from os import getenv
from re import match
from sys import argv

spark = SparkSession.builder.getOrCreate()

LogInfo = Row("host", "bytes")


def extract_hostname_and_bytes(log: str) -> LogInfo:
    """
    @param log: a log detailing an HTTP request sent to the NASA Kennedy space center
    @return: a Row containing the hostname and the number of bytes sent in the request
    """
    hostname_and_bytes = r"^(\S+) - - \[\S+ [+-]\d+\] \"[A-Z]+ \S+ HTTP/\d\.\d\" \d+ (\d+)$"
    search = match(hostname_and_bytes, log)
    return None if search is None else LogInfo(search.group(1), int(search.group(2)))


if not getenv('TESTING'):
    rows = spark.sparkContext.textFile("nasa-logs-1")
    print(rows.take(4))

