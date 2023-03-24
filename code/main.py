# Esse script efetivamente instancia as funções da classe postgres. Criando dumps
# das tabelas em em dvdrental e as copiando para analytics

from postgres import Postgres

ANALYTICS_CREDENTIALS = {

    "host":"refera-challenge-analytics-1", # Como estão num mesmo ambiente docker, estão na mesma rede e dessa forma posso utilizar o nome do container ao invés de 'localhost'
    "port": "5432",
    "user": "postgres",
    "password": "password",
    "database": "analytics"

}


TRANSACTIONAL_CREDENTIALS = {

    "host":"refera-challenge-transactional-1", # Como estão num mesmo ambiente docker, estão na mesma rede e dessa forma posso utilizar o nome do container ao invés de 'localhost'
    "port": "5432",
    "user": "postgres",
    "password": "password",
    "database": "dvdrental"

}


# Conexão com o banco analytics
analytics_db = Postgres(credentials=ANALYTICS_CREDENTIALS)

# Conexão com o banco transactional
transactional_db = Postgres(credentials=TRANSACTIONAL_CREDENTIALS)

# Separando os nomes das tabelas no banco transacional
tabelas_transactional = transactional_db.get_table_names()

# Exportando tabelas para arquivos csvs
for tabela in tabelas_transactional:

    transactional_db.export_tables_to_csv(table=tabela)

for tabela in tabelas_transactional:
    analytics_db.copy_csv_files_to_db(tabela=tabela)