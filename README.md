# data_eng_test_capim
Teste para a posição de Engenheira de Dados na Capim


1. Cria a rede:

```docker
docker network create net
```

2. Cria o banco:

```docker
docker run --name postgres -e POSTGRES_PASSWORD=postgres -p 5432:5432 --network net -d postgres
```

3. Executa o pipeline:

```python
python pipeline.py
```