import faker
import csv
import json

def generate_data(nro_rows, path, type):

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


if __name__ == "__main__":
  # Exemplo de uso do método
  generate_data(10, "data.csv", "csv")
  generate_data(10, "data.json", "json")
