import pandas as pd
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import sys
import os
import logging
import psycopg2

sys.path.append('/opt/airflow/dags')

from src.diario.processos.processar_cotahist_diario import processar_e_inserir_stage_diario
from src.diario.processos.inserir_dados_diario import transformar_e_carregar_diario

default_args = {
    'owner': 'thiago',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'base_diaria',
    default_args=default_args,
    description='Processamento diário dos arquivos COTAHIST',
    schedule_interval='0 12 * * *',
    catchup=False
)

def processar_arquivo_diario(**context):
    logging.info("Iniciando o processamento do arquivo diário COTAHIST")
    
    data_execucao = datetime.now() - timedelta(days=2)
    while data_execucao.weekday() >= 5:
        data_execucao -= timedelta(days=1)
    
    data_formatada = data_execucao.strftime('%d%m%Y')
    nome_arquivo = f'COTAHIST_D{data_formatada}.TXT'
    input_dir = '/opt/airflow/dags/src/diario/data'
    caminho_arquivo = os.path.join(input_dir, nome_arquivo)

    logging.info(f"Procurando pelo arquivo {nome_arquivo} no diretório {input_dir}")

    if not os.path.exists(caminho_arquivo):
        logging.error(f"Arquivo {nome_arquivo} não encontrado em {input_dir}")
        raise FileNotFoundError(f"Arquivo {nome_arquivo} não encontrado")

    try:
        conn = psycopg2.connect(
            host='postgres',
            database='airflow',
            user='airflow',
            password='airflow'
        )
        processar_e_inserir_stage_diario(caminho_arquivo, conn)
        logging.info("Processamento do arquivo diário concluído e dados inseridos na stage")
    except Exception as e:
        logging.error(f"Erro ao processar o arquivo diário: {e}")
        raise
    finally:
        if 'conn' in locals() and conn:
            conn.close()
            logging.info("Conexão com o banco de dados fechada.")

def inserir_dados_diarios_func():
    logging.info("Iniciando a transformação e carga dos dados diários no banco de dados")
    try:
        conn = psycopg2.connect(
            host='postgres',
            database='airflow',
            user='airflow',
            password='airflow'
        )
        transformar_e_carregar_diario(conn)
        logging.info("Transformação e carga dos dados diários concluída com sucesso")
    except Exception as e:
        logging.error(f"Erro ao transformar e carregar dados diários: {e}")
        raise
    finally:
        if 'conn' in locals() and conn:
            conn.close()
            logging.info("Conexão com o banco de dados fechada.")

processar_diario_task = PythonOperator(
    task_id='processar_arquivo_diario',
    python_callable=processar_arquivo_diario,
    provide_context=True,
    dag=dag,
)

inserir_diario_task = PythonOperator(
    task_id='inserir_dados_diarios',
    python_callable=inserir_dados_diarios_func,
    dag=dag,
)

processar_diario_task >> inserir_diario_task
