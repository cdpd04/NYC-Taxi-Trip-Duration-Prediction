import unittest
from pyspark.sql import SparkSession


class TestPipeline(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder \
            .appName("TestPipeline") \
            .master("local[2]") \
            .getOrCreate()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_spark_session(self):
        self.assertIsNotNone(self.spark)

    def test_basic_dataframe(self):
        df = self.spark.createDataFrame(
            [(1, 2), (3, 4)],
            ["a", "b"]
        )
        self.assertEqual(df.count(), 2)


if __name__ == "__main__":
    unittest.main()
