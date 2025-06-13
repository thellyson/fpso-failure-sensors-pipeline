import os

# ─── Parâmetros de conexão PostgreSQL ───
PG_HOST       = os.getenv("POSTGRES_HOST", "postgres")
PG_PORT       = os.getenv("POSTGRES_PORT", "5432")
PG_DB         = os.getenv("POSTGRES_DB",   "fpso")
PG_USER       = os.getenv("POSTGRES_USER", "shape")
PG_PASSWORD   = os.getenv("POSTGRES_PASSWORD", "shape")
PG_STRINGTYPE = os.getenv("POSTGRES_STRINGTYPE", "unspecified")

def get_jdbc_url():
    return f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}?stringtype={PG_STRINGTYPE}"

def get_jdbc_opts():
    return {
        "user":   PG_USER,
        "password": PG_PASSWORD,
        "driver": "org.postgresql.Driver"
    }
