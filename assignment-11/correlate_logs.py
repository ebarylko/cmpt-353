from pyspark.sql import SparkSession, DataFrame, functions, Row
from os import getenv
from re import match
from math import sqrt
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


def calc_correlation_coefficient(logs: DataFrame):
    """
    @param logs: a DataFrame where each row contains a hostname and the number of bytes transferred
    @return: the correlation coefficient for the number of times a request is made and the number of bytes transferred
    """
    total_requests_and_bytes = logs.groupBy('host').agg(functions.count('host').alias('total_requests'), functions.sum('bytes').alias('total_bytes')).drop('host')
    covariance = total_requests_and_bytes.cov('total_requests', 'total_bytes')
    bytes_and_requests_std = total_requests_and_bytes.select(functions.std('total_requests') * functions.std('total_bytes')).collect()[0][0]
    return covariance / bytes_and_requests_std


if not getenv('TESTING'):
    rows = spark.sparkContext.textFile("nasa-logs-1")

    valid_logs = rows.map(extract_hostname_and_bytes).filter(is_valid_log).toDF(['host', 'bytes'])

    corr_coef = calc_correlation_coefficient(valid_logs)

    print(f"r = {corr_coef}\nr^2 = {corr_coef ** 2}")
