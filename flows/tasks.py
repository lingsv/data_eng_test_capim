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
    # Conexão ao banco de dados
    connection_string = psycopg2.connect(
        host="0.0.0.0",  # Ou o endereço IP do seu host, se diferente
        port="5432",
        database="postgres",
        user="postgres",
        password="postgres",
    )

    with connection_string.cursor() as cursor:
        # Criação da tabela se não existir
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

        # Upsert dos dados
        insert_query = """
            INSERT INTO address_data (id, nome, idade, email, telefone, logradouro, numero, bairro, cidade, estado, cep)
            VALUES %s
            ON CONFLICT (id) DO UPDATE SET
                nome = EXCLUDED.nome,
                idade = EXCLUDED.idade,
                email = EXCLUDED.email,
                telefone = EXCLUDED.telefone,
                logradouro = EXCLUDED.logradouro,
                numero = EXCLUDED.numero,
                bairro = EXCLUDED.bairro,
                cidade = EXCLUDED.cidade,
                estado = EXCLUDED.estado,
                cep = EXCLUDED.cep
            """
        extras.execute_values(cursor, insert_query, data.values)
