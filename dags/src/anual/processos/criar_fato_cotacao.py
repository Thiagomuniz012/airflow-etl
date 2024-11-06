import psycopg2

def criar_fato_cotacao_table(conn):
    with conn.cursor() as cursor:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS fato_cotacao (
                data_pregao DATE,
                cod_negociacao VARCHAR(12),
                preco_abertura DECIMAL,
                preco_maximo DECIMAL,
                preco_minimo DECIMAL,
                preco_medio DECIMAL,
                preco_fechamento DECIMAL,
                qtd_negocios INTEGER,
                volume_total DECIMAL,
                PRIMARY KEY (data_pregao, cod_negociacao),
                FOREIGN KEY (data_pregao) REFERENCES dim_data (data_pregao),
                FOREIGN KEY (cod_negociacao) REFERENCES dim_empresa (cod_negociacao)
            );
        """)
    conn.commit()
