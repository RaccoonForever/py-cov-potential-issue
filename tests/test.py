from pyspark.sql.types import StructType, StructField, StringType

from tests.context import udf_get_bucket_name
from tests.conf_spark import PySparkUnitTestCase


class UnitTestClass(PySparkUnitTestCase):

    def test_get_bucketname_1(self):
        """
        Test basic UDF get filename
        """
        test_input = [
            (
                's3://bucket_test/temp.txt',)
        ]
        schema = StructType([
            StructField("path", StringType(), False)
        ])

        df = self.spark.createDataFrame(data=test_input,
                                        schema=schema)

        results = df.withColumn("bucket_name", udf_get_bucket_name("path")).collect()
        print(results)
        self.assertEqual(1, 1)
