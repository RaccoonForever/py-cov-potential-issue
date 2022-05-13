"""
Module to use for pyspark unit tests
"""
import unittest

from pyspark.sql import SparkSession


class PySparkUnitTestCase(unittest.TestCase):
    """
    Class to inherit for pyspark unit test
    """

    @classmethod
    def setUpClass(cls):
        """
        Set up a SparkSession as a local config that will be reuse for all pyspark tests
        """
        cls.spark = SparkSession\
            .builder\
            .master("local")\
            .appName("test-pyspark-session")\
            .getOrCreate()
        cls.sc = cls.spark.sparkContext

    @classmethod
    def tearDownClass(cls):
        """
        Tear down the SparkSession
        """
        cls.sc.stop()
