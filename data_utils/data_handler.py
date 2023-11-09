import csv
import numpy as np
import json
import os
from faker import Faker
from data_utils.spark_handler import SparkHandler

#####################################################################
# Data Generation
#####################################################################

def generate_faker_data(nro_rows: int, 
                        folder_path: str, 
                        file_name: str,
                        format_type: str
                        ) -> str:

    """
    Gera massa de dados utilizando a biblioteca Faker

    Args:
        nro_rows: Número de linhas de massa de dados a serem geradas
        folder_path: Local onde o arquivo com a massa de dados será gerado
        file_name: Nome do arquivo que será gerado com a massa de dados
        type: Tipo de arquivo a ser gerado

    Returns:
        filepath: Path completo do arquivo gerado
    """

    # Cria um gerador de dados aleatórios
    fake = Faker("pt_BR")

    # Gera os cabeçalhos do arquivo
    headers = ["name", "job", "cpf", "country", "date"]

    # Cria um objeto para armazenar os dados
    data = []

    # Gera os dados
    for _ in range(nro_rows):
        data.append([
            fake.name(),
            fake.job(),
            fake.cpf(),
            fake.country(),
            fake.date('%d-%m-%Y')
        ])

    if folder_path[-1] == "/":
        # Se for, retorna o caminho concatenado com o nome do arquivo.
        filepath = folder_path + file_name + '.' + format_type
    else:
        # Se não for, retorna o caminho concatenado com o caractere "/" e o nome do arquivo.
        filepath = folder_path + "/" + file_name + '.' + format_type

    # Salva o arquivo
    if format_type == "csv":
        with open(filepath, "w", newline="", encoding='UTF-8') as csvfile:
            writer = csv.writer(csvfile, delimiter=';')
            writer.writerow(headers)
            writer.writerows(data)
    elif format_type == "json":
        with open(filepath, "w") as jsonfile:
            json.dump(data, jsonfile)

    return filepath

def import_data(folder_path, file_name):

    return True
#####################################################################
# Data Validation
#####################################################################

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
            # TODO: Ajustar etapa de cast, pois não está funcionando corretamente
            
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


# Faça uma função python a partir das seguintes especificações:
# 1. A função deverá eleger o melhor data type de cada coluna de um spark dataframe
# 2. Para eleger o melhor data type, a função deverá tentar converter cada coluna para todos
# os data types que estiverem contidos em uma lista de data types.
# 3. A lista de data types deverá conter os seguintes valores: "int", "float", "date", "time", "string", "object"
# 4. Ao tentar realizar a conversão da coluna, em caso de sucesso, a função deverá armazenar data type em um dicionario, onde a chave será o nome da coluna, e o valor será uma lista com os data types possíveis de conversãol.
# 5. Ao tentar realizar a conversão da coluna, podem ocorrer Exceptions, a função deverá identificar a exception e ignora-la. Caso uma conversão retorne uma Expection, o data type não deverá ser armazenado no dicionario especificado no item 4.