import platform
from data_utils.spark_handler import SparkHandler
import data_utils.data_handler as dt_hand

def main():

    spark_session = SparkHandler()

    if platform.system() == 'Windows': 
        folder_path = 'C:/Users/andre/Desktop/coding/git/smith-sentinel/datasets/'
    elif platform.system() == 'Linux':
        folder_path = "/home/misteryoh/Coding/git/smith-sentinel/datasets/"

    # Gera dados aleatórios para o arquivo CSV.
    filepath = dt_hand.generate_faker_data(nro_rows=100,
                                           folder_path=folder_path, 
                                           file_name="fake_data",
                                           format_type='csv'
                                           )

    # Carrega o arquivo CSV.
    df = spark_session.load_data_from_file(filepath)

    df = dt_hand.find_best_dtypes(df)

    # Verifica os tipos de dados das colunas.
    column_data_types = dt_hand.check_column_data_types(df)

    # Imprime os tipos de dados das colunas.
    print(column_data_types)

    # Imprime o DataFrame.
    df.show()

if __name__ == "__main__":
    main()
