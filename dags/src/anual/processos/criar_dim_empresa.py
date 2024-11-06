import psycopg2

def criar_dim_empresa_table(conn):
    with conn.cursor() as cursor:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS dim_empresa (
                cod_negociacao VARCHAR(12) PRIMARY KEY,
                nome_empresa VARCHAR(50)
            );
        """)
    conn.commit()
