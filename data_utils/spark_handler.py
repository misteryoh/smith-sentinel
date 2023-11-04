import pyspark.sql
from pyspark.sql import SparkSession
from file_handler import FileHandler

class SparkHandler:

    def __init__(self, spark_session):
        
        """
        Inicializa a classe SparkHandler.

        Args:
            spark_session: Uma SparkSession opcional que pode ser usada para inicializar a classe.

        Returns:
            None.
        """

        if spark_session is None:
            self.spark = SparkSession.builder.getOrCreate()
        else:
            self.spark = spark_session

    def get_spark_session(self):

        """
        Obtém a SparkSession da classe.

        Returns:
            A SparkSession.
        """

        return self.spark

    def stop_spark_session(self):

        """
        Para a SparkSession da classe.

        Returns:
            None.
        """

        self.spark.stop()

    def load_data_from_file(self, filepath):

        """
        Carrega dados de um arquivo para um DataFrame Spark.

        Args:
            filepath: O caminho do arquivo a ser carregado.

        Returns:
            Um DataFrame Spark com os dados do arquivo.
        """

        # Obtém a extensão do arquivo
        extension = FileHandler.get_file_extension(filepath)

        if extension == 'csv':
            delimiter = FileHandler.get_file_delimiter(filepath)
            
            df = self.spark.read.load(path=filepath, format=extension, sep=delimiter, inferSchema="true", header="true")
        elif extension == 'json':

            df = self.spark.read.load(path=filepath, format=extension)

        return df

