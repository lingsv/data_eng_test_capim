from prefect import Flow, Parameter
from tasks import (get_data, load_data_to_postgres, process_data,
                   )

with Flow('Extracting Fictional data data') as adress_data:

    # Parameter
    path = Parameter('path', default='/home/carol/Documentos/Repos/pessoal/data_eng_test_capim/pipeline/data/data.json')

    # Tasks

    data = get_data(path)
    dataframe_address_data = process_data(data)
    load_data_to_postgres(dataframe_address_data)
