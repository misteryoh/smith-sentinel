import pyspark.sql
from pyspark.sql import SparkSession
from file_handler import FileHandler

class SparkHandler:

    def __init__(self):
        # Cria um SparkSession
        self.spark = SparkSession.builder.getOrCreate()

    def get_spark_session(self):
        return self.spark

    def stop_spark_session(self):
        self.spark.stop()

    def load_data_from_file(self, filepath):

        # Obtém a extensão do arquivo
        extension = FileHandler.get_file_extension(filepath)

        if extension == 'csv':
            delimiter = FileHandler.get_file_delimiter(filepath)
            
            df = self.spark.read.load(path=filepath, format=extension, sep=delimiter, inferSchema="true", header="true")
        elif extension == 'json':

            df = self.spark.read.load(path=filepath, format=extension)

        return df

