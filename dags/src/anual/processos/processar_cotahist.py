import psycopg2
import logging
from datetime import datetime

def processar_e_inserir_stage(caminho_arquivo, conn):
    """
    Função para processar o arquivo COTAHIST e inserir os dados na tabela de stage.
    
    Args:
    - caminho_arquivo (str): Caminho para o arquivo COTAHIST.
    - conn (psycopg2.connection): Conexão ativa com o banco de dados.
    """
    data_rows = []

    try:
        with open(caminho_arquivo, 'r', encoding='latin1') as f:
            lines = f.readlines()
            for line in lines:
                tipo_registro = line[0:2]
                if tipo_registro == '01':
                    data_pregao = line[2:10]
                    cod_negociacao = line[12:24].strip()
                    nome_empresa = line[27:39].strip()

                    try:
                        preco_abertura = float(line[56:69].strip()) / 100
                        preco_maximo = float(line[69:82].strip()) / 100
                        preco_minimo = float(line[82:95].strip()) / 100
                        preco_medio = float(line[95:108].strip()) / 100
                        preco_fechamento = float(line[108:121].strip()) / 100
                        qtd_negocios = int(line[147:152].strip())
                        volume_total = float(line[170:188].strip()) / 100

                    except ValueError as e:
                        logging.warning(f"Erro ao processar a linha: {line}")
                        logging.warning(f"Detalhes do erro: {e}")
                        continue

                    data_formatada = datetime.strptime(data_pregao, '%Y%m%d').date()

                    data_rows.append((
                        data_formatada, cod_negociacao, nome_empresa, preco_abertura,
                        preco_maximo, preco_minimo, preco_medio, preco_fechamento,
                        qtd_negocios, volume_total
                    ))

        if not data_rows:
            logging.error("Nenhuma linha válida foi encontrada no arquivo para processar.")
            raise ValueError("Nenhuma linha válida foi encontrada no arquivo para processar.")

        with conn.cursor() as cursor:
            insert_query = """
                INSERT INTO stage_cotacao (
                    data_pregao, cod_negociacao, nome_empresa, preco_abertura, preco_maximo,
                    preco_minimo, preco_medio, preco_fechamento, qtd_negocios, volume_total
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
            """
            cursor.executemany(insert_query, data_rows)

        conn.commit()
        logging.info(f"{len(data_rows)} registros inseridos na tabela stage_cotacao com sucesso.")

    except FileNotFoundError as fnf_error:
        logging.error(f"Arquivo não encontrado: {fnf_error}")
        raise
    except psycopg2.DatabaseError as db_error:
        logging.error(f"Erro ao conectar ao banco de dados ou inserir dados: {db_error}")
        conn.rollback()
        raise
    except Exception as e:
        logging.error(f"Erro desconhecido: {e}")
        raise
