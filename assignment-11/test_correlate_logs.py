from pyspark.sql import SparkSession, Row
import correlate_logs as cl

spark = SparkSession.builder.getOrCreate()


def test_extract_hostname_and_bytes():
    assert cl.LogInfo('199.72.81.55', 6245) == cl.extract_hostname_and_bytes('199.72.81.55 - - [01/Jul/1995:00:00:01 -0400] "GET /history/apollo/ HTTP/1.0" 200 6245')
    assert cl.extract_hostname_and_bytes('invalid text') is None
    assert cl.LogInfo('unicomp6.unicomp.net', 3985) == cl.extract_hostname_and_bytes('unicomp6.unicomp.net - - [01/Jul/1995:00:00:06 -0400] "GET /shuttle/countdown/ HTTP/1.0" 200 3985')


def is_valid_log(log):
    return log is not None