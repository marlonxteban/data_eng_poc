from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType

from datetime import datetime

class PySparkProcessor:
    def __init__(self):
        self.url = "jdbc:postgresql://host.docker.internal:5432/postgres"
        self.properties = {
            "user": "postgres",
            "password": "postgres",
            "driver": "org.postgresql.Driver"
        }
        self.spark = SparkSession.builder \
            .appName("data_eng_poc") \
            .config("spark.jars", "./drivers/postgresql-42.7.1.jar") \
            .getOrCreate()

    def get_schema(self, name):
        schemas = {
            "job": StructType([
                StructField("id", IntegerType(), False),
                StructField("job", StringType(), False)
            ]),
            "department": StructType([
                StructField("id", IntegerType(), False),
                StructField("department", StringType(), False)
            ]),
            "employee": StructType([
                StructField("id", IntegerType(), False),
                StructField("name", StringType(), False),
                StructField("datetime", DateType(), False),
                StructField("department_id", IntegerType(), True),
                StructField("job_id", IntegerType(), True)
            ])
        }
        return schemas[name]

    def get_table_name(self, name):
        table_names = {
            "job": "jobs",
            "department": "departments",
            "employee": "employees"
        }
        return table_names[name]

    def process_csv(self, file_path, file_type):
        # read file
        schema = self.get_schema(file_type)
        df = self.spark.read.csv(file_path, schema=schema, header=False)
        df.write.jdbc(url=self.url, table=self.get_table_name(file_type), mode="append", properties=self.properties)
        return df.count()

    def preprocess_employees_file(self, file_path, blocks):
        # read file
        schema = self.get_schema("employee")
        df = self.spark.read.csv(file_path, schema=schema, header=False)
        # preprocess
        invalid_records_df = df.filter(df["id"].isNull() | df["name"].isNull() | df["datetime"].isNull())
        invalid_records_df.write.csv(f"rejected_files/invalid_records_{datetime.now()}", header=True, mode="overwrite")
        valid_records_df = df.filter(df["id"].isNotNull() & df["name"].isNotNull() & df["datetime"].isNotNull())
        valid_date_records_df = valid_records_df.withColumn("datetime", df["datetime"].cast("date"))
        return valid_date_records_df

    def calculate_partitions(self, count, blocks):
        return count // blocks if count % blocks == 0 else (count // blocks) + 1

    def process_employees_df(self, df, blocks):
        valid_employees = df.count()
        partitions = self.calculate_partitions(valid_employees, blocks)
        df = df.repartition(partitions)
        df.write.jdbc(url=self.url, table=self.get_table_name("employee"), mode="append", properties=self.properties)
        return valid_employees