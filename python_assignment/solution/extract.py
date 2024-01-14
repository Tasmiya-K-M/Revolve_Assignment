from pyspark.sql import SparkSession
from sys import exc_info
import logging
logging.basicConfig(filename='C:/Users/Asus/Downloads/Revolve Solutions - Python Assignment/python-assignment/logs/shopping_patterns.log')

class Extract:
    def extract_data(self, spark:SparkSession, file_type:str="", file_name:str=""):
        try:
            if file_type == "json":
                df = spark.read.option("recursiveFileLookup", "true").json("C:/Users/Asus/Downloads/Revolve Solutions - Python Assignment/python-assignment/input_data/starter/transactions")
            elif file_type == "csv":
                df = spark.read.format("csv").option("inferSchema", True).option("header", True).option("path",f"C:/Users/Asus/Downloads/Revolve Solutions - Python Assignment/python-assignmentinput_data/starter/{file_name}.csv").load()
            return df
        except Exception as err:
            logging.error(msg=str(err), exc_info=True)
            raise err
