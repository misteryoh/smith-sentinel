from data_utils.spark_handler import SparkHandler
import data_utils.data_handler as dt_hand

def main():

    spark_session = SparkHandler()

    # Cria o caminho para o arquivo CSV.
    path_to_file = "/home/misteryoh/Coding/fake_data"

    # Gera dados aleatórios para o arquivo CSV.
    dt_hand.generate_data(path_to_file, 100, 'json')

    # Carrega o arquivo CSV.
    df = spark_session.load_data_from_file(path_to_file)

    df = dt_hand.convert_column_data_types(df)

    # Verifica os tipos de dados das colunas.
    column_data_types = dt_hand.check_column_data_types(df)

    # Imprime os tipos de dados das colunas.
    print(column_data_types)

    # Imprime o DataFrame.
    df.show()

if __name__ == "__main__":
    main()
