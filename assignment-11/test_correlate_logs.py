from pyspark.sql import SparkSession, Row
from chispa.dataframe_comparer import  assert_df_equality
import correlate_logs as cl

spark = SparkSession.builder.getOrCreate()

# sample_logs = spark.sparkContext.parallelize(['199.72.81.55 - - [01/Jul/1995:00:00:01 -0400] "GET /history/apollo/ HTTP/1.0" 200 6245',
#                                               'invalid text',
#                                              'unicomp6.unicomp.net - - [01/Jul/1995:00:00:06 -0400] "GET /shuttle/countdown/ HTTP/1.0" 200 3985',
#                                               '199.120.110.21 - - [01/Jul/1995:00:00:09 -0400] "GET /shuttle/missions/sts-73/mission-sts-73.html HTTP/1.0" 200 4085'
#                                               ])

# LogInfo = Row("host", "bytes")
#
# expected_hosts_and_bytes = spark.createDataFrame([LogInfo('199.72.81.55', 6245),
#                                                   LogInfo(None, None),
#                                                   LogInfo(None, None),
#                                                   LogInfo('199.120.110.21', 4085)])


def test_extract_hostname_and_bytes():
    assert cl.LogInfo('199.72.81.55', 6245) == cl.extract_hostname_and_bytes('199.72.81.55 - - [01/Jul/1995:00:00:01 -0400] "GET /history/apollo/ HTTP/1.0" 200 6245')
    assert cl.extract_hostname_and_bytes('invalid text') is None
    assert cl.LogInfo('unicomp6.unicomp.net', 3985) == cl.extract_hostname_and_bytes('unicomp6.unicomp.net - - [01/Jul/1995:00:00:06 -0400] "GET /shuttle/countdown/ HTTP/1.0" 200 3985')