from pyspark.sql import SparkSession, DataFrame, functions, Row
from os import getenv
from re import match
from sys import argv
from math import sqrt


LogInfo = Row("host", "bytes")


def extract_hostname_and_bytes(log: str) -> LogInfo:
    """
    @param log: a log detailing an HTTP request sent to the NASA Kennedy space center
    @return: a Row containing the hostname and the number of bytes sent in the request if the log is valid,
     None otherwise
    """
    hostname_and_bytes = r"^(\S+) - - \[\S+ [+-]\d+\] \"[A-Z]+ \S+ HTTP/\d\.\d\" \d+ (\d+)$"
    matches = match(hostname_and_bytes, log)
    return None if matches is None else LogInfo(matches.group(1), int(matches.group(2)))


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
    total_requests_and_bytes = (logs.groupBy('host').
                                agg(functions.count('host').alias('total_requests'),
                                    functions.sum('bytes').alias('total_bytes'))
                                .drop('host'))

    with_more_info = total_requests_and_bytes.withColumns({"requests_bytes_prod": total_requests_and_bytes.total_bytes * total_requests_and_bytes.total_requests,
                                                           "bytes_squared": total_requests_and_bytes.total_bytes ** 2,
                                                           "requests_squared": total_requests_and_bytes.total_requests ** 2})

    vals = with_more_info.select(functions.count("total_requests").alias("num_of_requests"),
                                 functions.sum("requests_bytes_prod").alias("bytes_times_requests"),
                                 functions.sum("total_requests").alias("requests_sum"),
                                 functions.sum("total_bytes").alias("bytes_sum"),
                                 functions.sum("bytes_squared").alias("bytes_squared_sum"),
                                 functions.sum("requests_squared").alias("requests_squared_sum")
                                 ).first()

    covariance = vals.num_of_requests * vals.bytes_times_requests - vals.requests_sum * vals.bytes_sum
    standard_dev = (sqrt(vals.num_of_requests * vals.requests_squared_sum - vals.requests_sum ** 2) *
                    sqrt(vals.num_of_requests * vals.bytes_squared_sum - vals.bytes_sum ** 2))

    return covariance / standard_dev


if not getenv('TESTING'):
    spark = SparkSession.builder.getOrCreate()
    assert len(argv) == 2, "Intended use of correlate_logs.py: spark-submit correlate_logs.py input_directory"

    input_directory = argv[1]

    rows = spark.sparkContext.textFile(input_directory)

    valid_logs = rows.map(extract_hostname_and_bytes).filter(is_valid_log).toDF(['host', 'bytes'])

    corr_coef = calc_correlation_coefficient(valid_logs)

    print(f"r = {corr_coef}\nr^2 = {corr_coef ** 2}")
