from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import sys
import os
import logging
import psycopg2

sys.path.append('/opt/airflow/dags')

from src.anual.processos.processar_cotahist import processar_e_inserir_stage
from src.anual.processos.criar_dim_data import criar_dim_data_table
from src.anual.processos.criar_dim_empresa import criar_dim_empresa_table
from src.anual.processos.criar_fato_cotacao import criar_fato_cotacao_table
from src.anual.processos.criar_tabelas_stage import criar_tabelas_stage
from src.anual.processos.transformar_e_carregar import transformar_e_carregar

default_args = {
    'owner': 'thiago',
    'start_date': datetime(2024, 1, 1),
}

dag = DAG('base_anual', default_args=default_args, schedule_interval='@once', catchup=False)

def processar_arquivos():
    logging.info("Iniciando o processamento dos arquivos COTAHIST")
    input_dir = '/opt/airflow/dags/src/anual/data'

    try:
        conn = psycopg2.connect(
            host='postgres',
            database='airflow',
            user='airflow',
            password='airflow'
        )

        for file_name in os.listdir(input_dir):
            if file_name.startswith('COTAHIST_A') and file_name.endswith('.TXT'):
                caminho_arquivo = os.path.join(input_dir, file_name)
                logging.info(f"Processando o arquivo: {file_name}")
                processar_e_inserir_stage(caminho_arquivo, conn)

        logging.info("Processamento dos arquivos concluído e dados inseridos na stage")
    except Exception as e:
        logging.error(f"Erro ao processar arquivos: {e}")
        raise
    finally:
        if 'conn' in locals() and conn:
            conn.close()
            logging.info("Conexão com o banco de dados fechada.")

def criar_tabelas():
    logging.info("Iniciando a criação das tabelas no banco de dados")
    try:
        conn = psycopg2.connect(
            host='postgres',
            database='airflow',
            user='airflow',
            password='airflow'
        )
        criar_tabelas_stage(conn)
        criar_dim_data_table(conn)
        criar_dim_empresa_table(conn)
        criar_fato_cotacao_table(conn)
        logging.info("Tabelas criadas com sucesso")
    except Exception as e:
        logging.error(f"Erro ao criar tabelas: {e}")
        raise
    finally:
        if 'conn' in locals() and conn:
            conn.close()
            logging.info("Conexão com o banco de dados fechada.")

def transformar_carregar():
    logging.info("Iniciando a transformação e carga dos dados")
    try:
        conn = psycopg2.connect(
            host='postgres',
            database='airflow',
            user='airflow',
            password='airflow'
        )
        transformar_e_carregar(conn)
        logging.info("Transformação e carga dos dados concluída com sucesso")
    except Exception as e:
        logging.error(f"Erro ao transformar e carregar dados: {e}")
        raise
    finally:
        if 'conn' in locals() and conn:
            conn.close()
            logging.info("Conexão com o banco de dados fechada.")

def limpar_stage():
    logging.info("Iniciando a limpeza da tabela stage_cotacao")
    try:
        conn = psycopg2.connect(
            host='postgres',
            database='airflow',
            user='airflow',
            password='airflow'
        )

        cursor = conn.cursor()

        sql = "DELETE FROM stage_cotacao;"
        cursor.execute(sql)
        logging.info("Todos os registros da tabela stage_cotacao foram removidos.")

        conn.commit()

    except Exception as e:
        logging.error(f"Erro ao limpar a tabela stage_cotacao: {e}")
        if 'conn' in locals() and conn:
            conn.rollback()
        raise

    finally:
        if 'cursor' in locals() and cursor:
            cursor.close()
            logging.info("Cursor fechado.")
        
        if 'conn' in locals() and conn:
            conn.close()
            logging.info("Conexão com o banco de dados fechada.")

processar_arquivos_task = PythonOperator(
    task_id='processar_arquivos',
    python_callable=processar_arquivos,
    dag=dag,
)

criar_tabelas_task = PythonOperator(
    task_id='criar_tabelas',
    python_callable=criar_tabelas,
    dag=dag,
)

transformar_carregar_task = PythonOperator(
    task_id='transformar_carregar',
    python_callable=transformar_carregar,
    dag=dag,
)

limpar_stage_task = PythonOperator(
    task_id='limpar_stage',
    python_callable=limpar_stage,
    dag=dag,
)

criar_tabelas_task >> processar_arquivos_task >> transformar_carregar_task >> limpar_stage_task
