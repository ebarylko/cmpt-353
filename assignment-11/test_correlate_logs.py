from pyspark.rdd import RDD
from pyspark.sql import SparkSession, Row
import correlate_logs as cl

spark = SparkSession.builder.getOrCreate()


def test_extract_hostname_and_bytes():
    assert cl.LogInfo('199.72.81.55', 6245) == cl.extract_hostname_and_bytes('199.72.81.55 - - [01/Jul/1995:00:00:01 -0400] "GET /history/apollo/ HTTP/1.0" 200 6245')
    assert cl.extract_hostname_and_bytes('invalid text') is None
    assert cl.LogInfo('unicomp6.unicomp.net', 3985) == cl.extract_hostname_and_bytes('unicomp6.unicomp.net - - [01/Jul/1995:00:00:06 -0400] "GET /shuttle/countdown/ HTTP/1.0" 200 3985')


def is_valid_log(log):
    return log is not None


sample_data = spark.sparkContext.parallelize([cl.LogInfo('199.72.81.55', 6245),
                                              None,
                                              None,
                                              cl.LogInfo('199.120.110.21', 4085)])

actual_logs = sample_data.filter(is_valid_log)

expected_logs = spark.sparkContext.parallelize([cl.LogInfo('199.72.81.55', 6245),
                                                cl.LogInfo('199.120.110.21', 4085)])


def logs_match(log1: cl.LogInfo, log2: cl.LogInfo):
    return log1['host'] == log2['host'] and log1['bytes'] == log2['bytes']


def assert_logs_equal(expected, actual):
    return all(map(logs_match, expected.collect(), actual.collect()))


def test_filter_valid_logs():
    assert assert_logs_equal(expected_logs, actual_logs)