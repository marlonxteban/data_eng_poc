from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType
from pyspark.sql.functions import col, year, quarter, sum, when, avg, desc, count

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

    def get_employees_job_2021(self):
        employees_df = self.spark.read.jdbc(url=self.url, table="employees", properties=self.properties)
        departments_df = self.spark.read.jdbc(url=self.url, table="departments", properties=self.properties)
        jobs_df = self.spark.read.jdbc(url=self.url, table="jobs", properties=self.properties)
        df = employees_df.join(departments_df, employees_df.department_id == departments_df.id, "inner") \
            .join(jobs_df, employees_df.job_id == jobs_df.id, "inner") \
            .select(employees_df.id, employees_df.name, employees_df.datetime, departments_df.department, jobs_df.job)
        df_2021 = df.filter(year(col("datetime")) == 2021)
        df_2021_quarters = df_2021.withColumn("Q1", when(quarter("datetime") == 1, 1).otherwise(0)) \
            .withColumn("Q2", when(quarter("datetime") == 2, 1).otherwise(0)) \
            .withColumn("Q3", when(quarter("datetime") == 3, 1).otherwise(0)) \
            .withColumn("Q4", when(quarter("datetime") == 4, 1).otherwise(0))

        df_grouped = df_2021_quarters.groupBy("department", "job").agg(
            sum("Q2").alias("sum_Q2"),
            sum("Q1").alias("sum_Q1"),
            sum("Q3").alias("sum_Q3"),
            sum("Q4").alias("sum_Q4")
        )

        df_result = df_grouped.orderBy("department", "job")

        results = df_result.collect()
        data_for_template = [
            {"department": row["department"], "job": row["job"], "Q1": row["sum_Q1"], "Q2": row["sum_Q2"], "Q3": row["sum_Q3"], "Q4": row["sum_Q4"]} for row in results
        ]

        return data_for_template

    def get_hired_by_department_2021(self):
        employees_df = self.spark.read.jdbc(url=self.url, table="employees", properties=self.properties)
        departments_df = self.spark.read.jdbc(url=self.url, table="departments", properties=self.properties)

        df_employees_2021 = employees_df.filter(year(col("datetime")) == 2021)
        df_department_counts = df_employees_2021.groupBy("department_id").agg(count("*").alias("num_employees"))

        print(f"**************** df_department_counts ****************")

        print(df_department_counts.limit(10).show())

        print(f"**************** end df_department_counts ****************")

        average_hires = df_department_counts.agg(avg("num_employees").alias("average_hires")).collect()[0]["average_hires"]

        print(f"**************** average hires ****************")

        print(average_hires)

        print(f"**************** end avg hires ****************")

        df_departments_with_counts = departments_df.join(df_department_counts, departments_df["id"] == df_department_counts["department_id"])

        print(f"**************** df_departments_with_counts ****************")

        print(df_departments_with_counts.limit(10).show())

        print(f"**************** end df_departments_with_counts ****************")

        df_above_average = df_departments_with_counts.filter(df_departments_with_counts["num_employees"] > average_hires)

        df_result = df_above_average.orderBy(desc("num_employees"))
        df_result_clean = df_result.select("id", "department", "num_employees").withColumnRenamed("num_employees", "hired")

        results = df_result_clean.collect()
        data_for_template = [
            {"id": row["id"], "department": row["department"], "hired": row["hired"]} for row in results
        ]

        return data_for_template
