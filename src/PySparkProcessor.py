from pyspark.sql import SparkSession

class PySparkProcessor:
    def __init__(self):
        self.spark = SparkSession.builder \
            .appName("data_eng_poc") \
            .getOrCreate()

    def process_csv(self, file_path):
        # read file
        df = self.spark.read.csv(file_path, header=False, inferSchema=True)
        # TODO: replace with actual processing
        df.show()
        df.printSchema()