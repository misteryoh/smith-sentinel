import os

class FileHandler:

    def get_file_extension(filepath):
        # Obtém o nome do arquivo
        filename = os.path.basename(filepath)

        # Obtém o ponto que separa o nome do arquivo da extensão
        dot_index = filename.rfind(".")

        # Retorna a extensão do arquivo
        if dot_index != -1:
            return filename[dot_index + 1:]
        else:
            return None

    def get_file_delimiter(filepath):

        delimiters = [";", ",", "/t", "|"]

        # Abre o arquivo CSV
        with open(filepath, "r") as f:
            # Lê as primeiras 100 linhas do arquivo
            lines = f.readlines()[:100]

        # Inicializa um dicionário para armazenar a contagem de ocorrências de cada delimitador
        delimiter_counts = {}
        for delimiter in delimiters:
            delimiter_counts[delimiter] = 0

        # Conta as ocorrências de cada delimitador nas primeiras 100 linhas do arquivo
        for line in lines:
            for delimiter in delimiters:
                delimiter_counts[delimiter] += line.count(delimiter)

        # Obtém o delimitador de maior ocorrência
        max_delimiter = max(delimiter_counts, key=delimiter_counts.get)

        return max_delimiter