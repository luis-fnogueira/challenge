import logging
import psycopg2

# Criando um objeto logger e definindo o nível
logger = logging.getLogger()
logger.setLevel(logging.INFO)


class Postgres:

    def __init__(self, credentials: dict) -> None:

        """
        Esse construtor inicia a conexão com banco de dados
        Args:
            Credentials: dicionário contendo as credenciais
        """
        self.__CREDENTIALS = credentials
        

    def __get_conn(self):

        """
        Essa função cria uma conexão com o banco de dados com as credenciais
        passadas no construtor
        """
        try:

            return psycopg2.connect(
                host = self.__CREDENTIALS['host'],
                port = self.__CREDENTIALS['port'],
                user = self.__CREDENTIALS['user'],
                password = self.__CREDENTIALS['password'],
                database = self.__CREDENTIALS['database']
                )

        except Exception as error:

            logger.error(error)


    def get_table_names(self) -> list:

        """
        Essa função lista nomes das tabelas em uma lista
        """

        try:
        
            # Criando a conexão com o banco
            conn = self.__get_conn()
            cur = conn.cursor()

            # Query para pegar os os nomes das tabelas
            cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='public' AND table_type='BASE TABLE';")

            # Obtendo os resultados da consulta
            tabelas = cur.fetchall()

            # Salvando os nomes das tabelas em uma lista e retornando a função
            return [tabela[0] for tabela in tabelas]    

        except Exception as error:

            logger.error(error)


    def export_tables_to_csv(self, table: str):

        """
        Essa função exporta as tabelas para a pasta /code/sql_dumps no formato csv
        
        """
        try:
        
            # Criando a conexão com o banco
            conn = self.__get_conn()
            cur = conn.cursor()

            # exporando para csv
            arquivo = open(f'/code/sql_dumps/export_{table}.csv', 'w')
            sql = f"COPY (SELECT * FROM {table}) TO STDOUT WITH CSV HEADER"


            cur.copy_expert(sql, arquivo)
            arquivo.close()

            logger.info(f"{table} exported to /code/sql_dumps/")

        except Exception as error:

            logger.error(error)


    def copy_csv_files_to_db(self, tabela: str) -> None:

        """
        Essa função copia os dados de arquivos CSV para uma tabela no Postgres
        """

        try:
        
            conn = self.__get_conn()
            cur = conn.cursor()

            # Abrir arquivo csv
            with open(f'/code/sql_dumps/export_{tabela}.csv', 'r') as arquivo:

                # Pulando a linha de cabeçalho
                next(arquivo)
    
                # Copiando dados do CSV para a tabela
                cur.copy_from(arquivo, tabela, sep=',')

            # Commitando as mudanças no banco
            conn.commit()
            logger.info(f"{tabela}.csv copiada para {tabela}")


            # Fechando cursor e conexão
            cur.close()
            conn.close()
        
        except Exception as error:

            logger.error(error)