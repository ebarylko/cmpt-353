from pyspark.sql import SparkSession, types, DataFrame, functions, Row
from os import getenv
from re import match
from sys import argv

spark = SparkSession.builder.getOrCreate()

LogInfo = Row("host", "bytes")


def extract_hostname_and_bytes(log: str) -> LogInfo:
    """
    @param log: a log detailing an HTTP request sent to the NASA Kennedy space center
    @return: a Row containing the hostname and the number of bytes sent in the request if the log is valid, false otherwise
    """
    hostname_and_bytes = r"^(\S+) - - \[\S+ [+-]\d+\] \"[A-Z]+ \S+ HTTP/\d\.\d\" \d+ (\d+)$"
    search = match(hostname_and_bytes, log)
    return None if search is None else LogInfo(search.group(1), int(search.group(2)))


def is_valid_log(log: LogInfo) -> bool:
    """
    @param log: the log to validate
    @return: true if the log is not None, false otherwise
    """
    return log is not None


if not getenv('TESTING'):
    rows = spark.sparkContext.textFile("nasa-logs-1")

    logs = rows.map(extract_hostname_and_bytes).filter(is_valid_log)
    print(logs.take(10))


