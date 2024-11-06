import psycopg2

def criar_dim_data_table(conn):
    with conn.cursor() as cursor:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS dim_data (
                data_pregao DATE PRIMARY KEY,
                ano INTEGER,
                mes INTEGER,
                trimestre INTEGER,
                dia INTEGER,
                nome_mes VARCHAR(20),
                dia_da_semana VARCHAR(20)
            );
        """)
    conn.commit()
