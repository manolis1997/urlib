from databricks.connect.session import DatabricksSession as SparkSession

class SparkClass:
    spark = SparkSession.builder.getOrCreate()