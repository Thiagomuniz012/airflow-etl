import psycopg2

def criar_tabelas_stage(conn):
    with conn.cursor() as cursor:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS stage_cotacao (
                data_pregao DATE,
                cod_negociacao VARCHAR(20),
                nome_empresa VARCHAR(100),
                preco_abertura NUMERIC,
                preco_maximo NUMERIC,
                preco_minimo NUMERIC,
                preco_medio NUMERIC,
                preco_fechamento NUMERIC,
                qtd_negocios INTEGER,
                volume_total NUMERIC
            );
        """)
    conn.commit()
