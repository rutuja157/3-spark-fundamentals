
## this gives us spark ,anywhere the spark is referenced in the test code it will use this spark session
#eg. "in test_monthly_user_site_hits.py under def test_monthly_user_site_hits(spark): " we have spark here 

import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope='session')
def spark():
    return SparkSession.builder \
      .master("local") \
      .appName("chispa") \
      .getOrCreate()

# import pytest
# from pyspark.sql import SparkSession


# @pytest.fixture(scope='session')
# def spark():
#     return SparkSession.builder \
#         .master("local[*]") \
#         .appName("chispa") \
#         .config("spark.driver.host", "127.0.0.1") \
#         .config("spark.driver.bindAddress", "127.0.0.1") \
#         .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse") \
#         .config("spark.sql.adaptive.enabled", "false") \
#         .config("spark.sql.adaptive.coalescePartitions.enabled", "false") \
#         .getOrCreate()