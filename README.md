# Teste para a posição de Engenheira de Dados na Capim

## Árvore de diretórios

```bash
├── data.json
├── docs
│   └── README.md
├── LICENSE
├── Notebook
│   └── Open data.ipynb
├── pipeline.py
├── README.md
└── requirements.txt
```

## Arquivos


## Como utilizar este repositório

1. Crie um ambiente virtual e instale o arquivo `requirementx.txt`;

2. Instale e ative o serviço Docker;

3. Ative o ambiente virtual Python;

4. Crie a rede docker:

```docker
docker network create net
```

5. Crie o banco PostGreSQL:

```docker
docker run --name postgres -e POSTGRES_PASSWORD=postgres -p 5432:5432 --network net -d postgres
```

6. Execute o pipeline:

```python
python pipeline.py
```