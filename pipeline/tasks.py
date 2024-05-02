import datetime
import json
import os
import time

import pandas as pd
import psycopg2
import requests
from prefect import task
from psycopg2 import extras
from utils import log
from unidecode import unidecode


@task
def get_data(path:str) -> str:

    data = pd.read_json(path)

    return data

@task
def process_data(data:str) -> pd.DataFrame:
    """
    Transform data from the API
    :param data:
    :return:
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
    log('Data transformed with success!')

    return df

@task
def load_data_to_postgres(data: pd.DataFrame) -> None:
    """Loads data to a postgres table.

    Args:
        data (pd.DataFrame): Target dataframe.
    """

    with open('credentials.json', 'r') as jsonfile:

        postgres_credentials = json.load(jsonfile)

    log('Loaded postgres credentials.')

    connection_string = psycopg2.connect(
        host=postgres_credentials['host'],
        database=postgres_credentials['database'],
        user=postgres_credentials['user'],
        password=postgres_credentials['password'],
    )

    with connection_string.cursor() as cursor:
        create_table = """
            CREATE TABLE IF NOT EXISTS address_data(
                id INT,
                nome VARCHAR,
                idade INT,
                email VARCHAR,
                telefone VARCHAR,
                logradouro VARCHAR,
                numero VARCHAR,
                bairro VARCHAR,
                cidade VARCHAR,
                estado VARCHAR,
                cep VARCHAR,
           
            )
            """

        cursor.execute(create_table)

        insert_query = "INSERT INTO IF NOT EXISTS address_data VALUES %s"
        extras.execute_values(cursor, insert_query, data.values)

    connection_string.commit()
    connection_string.close()

    log('Loaded data to postgres.')
