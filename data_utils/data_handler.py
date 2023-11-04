# Importa o pacote PySpark.
import pyspark
import csv
import random
import numpy as np
import json
import faker
from spark_handler import SparkHandler

#####################################################################
# Data Generation
#####################################################################

def generate_data(nro_rows, path, type, file_name=None):

    """
    Gera massa de dados utilizando a biblioteca Faker

    Args:
        nro_rows: Número de linhas de massa de dados a serem geradas
        path: Local onde o arquivo com a massa de dados será gerado
        type: Tipo de arquivo a ser gerado

    Returns:
        None
    """

    # Cria um gerador de dados aleatórios
    fake = faker.Faker("pt_BR")

    # Gera os cabeçalhos do arquivo
    headers = ["name", "job", "cpf", "country"]

    # Cria um objeto para armazenar os dados
    data = []

    # Gera os dados
    for _ in range(nro_rows):
        data.append([
            fake.name(),
            fake.job(),
            fake.cpf(),
            fake.country(),
        ])

    # Salva o arquivo
    if type == "csv":
        with open(path, "w", newline="") as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(headers)
            writer.writerows(data)
    elif type == "json":
        with open(path, "w") as jsonfile:
            json.dump(data, jsonfile)

def check_column_data_types(df):
    """
    Verifica os tipos de dados das colunas de um arquivo CSV importados no dataframe.

    Args:
      df: O dataframe PySpark.

    Returns:
      Um dicionário com os tipos de dados das colunas.
    """

    # Obtém os nomes das colunas.
    column_names = df.columns

    # Obtém os tipos de dados das colunas.
    column_dtypes = df.dtypes

    # Cria um dicionário com os tipos de dados das colunas.
    column_data_types = {}
    for column_name, column_dtype in zip(column_names, column_dtypes):
        column_data_types[column_name] = column_dtype

    return column_data_types

def convert_column_data_types(df):
    """
    Analisa e tenta converter para o formato mais adequado, os dados das colunas de um DataFrame.

    Args:
      df: O dataframe PySpark.

    Returns:
      O dataframe PySpark com os dados convertidos.
    """

    # Obtém os nomes das colunas.
    column_names = df.columns

    # Itera sobre as colunas.
    for column_name in column_names:

        # Tenta converter os dados da coluna para todos os tipos de dados.
        column_data_types = []
        for data_type in [
            "int",
            "float",
            "date",
            "time",
            "string",
            "object",
        ]:
            # Substitui a linha que causava o erro pelo código abaixo
            df = df.withColumn(
                column_name,
                df[column_name].cast(data_type),
            )
            column_data_types.append(data_type)

        # Identifica o tipo de dados mais adequado.
        best_data_type = column_data_types[0]
        for data_type in column_data_types:
            if column_data_types.count(data_type) > column_data_types.count(best_data_type):
                best_data_type = data_type

        # Usa um método estatístico para averiguar o melhor formato.
        if best_data_type == "string":
            column_values = df[column_name].tolist()
            unique_values = set(column_values)
            if len(unique_values) <= 10:
                best_data_type = "category"

        # Converte os dados da coluna para o tipo de dados mais adequado.
        try:
            df = df.withColumn(
                column_name,
                df[column_name].cast(best_data_type),
            )
        except ValueError:
            # Descarta o tipo de dado que der erro.
            best_data_type = None

    return df