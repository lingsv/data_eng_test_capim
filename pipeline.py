import pandas as pd
import psycopg2
from psycopg2 import extras
from unidecode import unidecode

def log(message) -> None:
    """Logs the operations

    Args:
        message (_type_): The messages on the functions.
    """

    print(f'\n {message}')

def get_data(path:str) -> pd.DataFrame:
    """Extracts the data from json

    Args:
        path (str): Path to the data.

    Returns:
        pd.DataFrame
    """

    # Extracts the data from json
    data = pd.read_json(path)

    log("Dados carregados com sucesso.")
    return data

def process_data(data: pd.DataFrame) -> pd.DataFrame:
    """Processes the data.

    Args:
        data (pd.DataFrame): The dataframe created in the first task.

    Raises:
        error: Raises an error if something goes wrong.

    Returns:
        pd.DataFrame: Treated data dataframe.
    """
    try:
        df = pd.DataFrame(data)

        # Split the address into separate columns
        df[['logradouro', 'numero', 'bairro', 'cidade', 'estado', 'cep']] = df['endereco'].apply(pd.Series)
        
        # Removes unnecessary column
        df = df.drop('endereco', axis=1)

        # Remove special characters from the name and address
        df['nome'] = df['nome'].apply(unidecode)
        df['logradouro'] = df['logradouro'].apply(unidecode)
        df['bairro'] = df['bairro'].apply(unidecode)
        df['cidade'] = df['cidade'].apply(unidecode)

        # Remove special characters from the phone number and cep   
        df['telefone'] = df['telefone'].str.replace('-','')
        df['telefone'] = df['telefone'].str.replace('(00)','')
        df['cep'] = df['cep'].str.replace('-','')
    
    except Exception as error:
        raise error
    log('Dados transformados com sucesso.')
    return df

def load_data_to_postgres(data: pd.DataFrame) -> None:
    """Loads data to a postgres table.

    Args:
        data (pd.DataFrame): Target dataframe.
    """
    # connection string
    connection_string = psycopg2.connect(
        host="localhost",  
        port="5432",
        database="postgres",
        user="postgres",
        password="postgres",
    )

    with connection_string.cursor() as cursor:
        # creates the table
        create_table = """
            CREATE TABLE IF NOT EXISTS address_data(
                id SERIAL PRIMARY KEY,
                nome VARCHAR,
                idade INT,
                email VARCHAR,
                telefone VARCHAR,
                logradouro VARCHAR,
                numero VARCHAR,
                bairro VARCHAR,
                cidade VARCHAR,
                estado VARCHAR,
                cep VARCHAR
            )
            """
        cursor.execute(create_table)

        # insert non duplicated data
        insert_query = """
                INSERT INTO address_data (id, nome, idade, email, telefone, logradouro, numero, bairro, cidade, estado, cep)
                VALUES %s
                ON CONFLICT (id) DO UPDATE SET
                    nome = EXCLUDED.nome,
                    idade = EXCLUDED.idade,
                    telefone = EXCLUDED.telefone,
                    logradouro = EXCLUDED.logradouro,
                    numero = EXCLUDED.numero,
                    bairro = EXCLUDED.bairro,
                    cidade = EXCLUDED.cidade,
                    estado = EXCLUDED.estado,
                    cep = EXCLUDED.cep
                    """
        extras.execute_values(cursor, insert_query, data.values)

        # Checa se os dados foram inseridos no banco
        cursor.execute("SELECT COUNT(*) FROM address_data")
        count = cursor.fetchone()[0]
        if count == len(data):
            log("Dados inseridos com sucesso.")
            
            # Consulta para selecionar os dados inseridos
            cursor.execute("SELECT * FROM address_data")
            rows = cursor.fetchall()  # Recupera todos os registros retornados pela consulta

            # Exibe os dados recuperados
            for row in rows:
                print(row)
        else:
            log("Erro ao inserir dados.")


def run_pipeline(path: str) -> None:
    """Pipeline executor.

    Args:
        path (str): Where the data is stored.
    """
    data = get_data(path)
    dataframe_address_data = process_data(data)
    load_data_to_postgres(dataframe_address_data)

if __name__ == "__main__":
    path = '/home/carol/Documentos/data_eng_test_capim/data.json'
    run_pipeline(path)
