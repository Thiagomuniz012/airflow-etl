import psycopg2

def transformar_e_carregar_diario(conn):
    """
    Função para transformar os dados da tabela de stage e carregá-los nas tabelas finais.

    Args:
    - conn (psycopg2.connection): Conexão ativa com o banco de dados.
    """
    with conn.cursor() as cursor:
        cursor.execute("""
            INSERT INTO dim_data (data_pregao, ano, mes, trimestre, dia, nome_mes, dia_da_semana)
            SELECT DISTINCT
                data_pregao,
                EXTRACT(YEAR FROM data_pregao) AS ano,
                EXTRACT(MONTH FROM data_pregao) AS mes,
                (EXTRACT(MONTH FROM data_pregao) - 1) / 3 + 1 AS trimestre,
                EXTRACT(DAY FROM data_pregao) AS dia,
                TO_CHAR(data_pregao, 'Month') AS nome_mes,
                TO_CHAR(data_pregao, 'Day') AS dia_da_semana
            FROM stage_cotacao
            ON CONFLICT (data_pregao) DO NOTHING;
        """)

        cursor.execute("""
            INSERT INTO dim_empresa (cod_negociacao, nome_empresa)
            SELECT DISTINCT
                cod_negociacao, nome_empresa
            FROM stage_cotacao
            ON CONFLICT (cod_negociacao) DO NOTHING;
        """)

        cursor.execute("""
            INSERT INTO fato_cotacao (
                data_pregao, cod_negociacao, preco_abertura, preco_maximo,
                preco_minimo, preco_medio, preco_fechamento, qtd_negocios, volume_total
            )
            SELECT
                data_pregao, cod_negociacao, preco_abertura, preco_maximo,
                preco_minimo, preco_medio, preco_fechamento, qtd_negocios, volume_total
            FROM stage_cotacao
            ON CONFLICT (data_pregao, cod_negociacao) DO NOTHING;
        """)
        
    conn.commit()
