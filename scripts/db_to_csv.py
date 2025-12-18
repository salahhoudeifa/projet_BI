from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL
import pandas as pd

#pour creer le dashboard, il necessite
#d'exporter les tables comme fichiers csv

server = r"DESKTOP-RV0ORQK\SQLEXPRESS_SALAH"
database = "ETL_Northwind"
username = "dbo"
driver = "ODBC Driver 17 for SQL Server"

#extraire des donnees depuis notre data warehouse "ETL_Northwind"
def extract():
    connection_url = URL.create(
        "mssql+pyodbc",
        username=username,
        password="",
        host=server,
        database=database,
        query={"driver": driver, "trusted_connection": "yes"},
    )

    engine = create_engine(connection_url)
    try:
        with engine.connect() as conn:
            tables_q = text(
                "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_CATALOG = :db"
            )
            result = conn.execute(tables_q, {"db": database})
            table_names = [row[0] for row in result.fetchall()]
        src_conn = engine.raw_connection()
        for raw_name in table_names:
            if raw_name is None:
                continue
            else:
                table_name = str(raw_name)

            safe_table = f"[{table_name}]"
            try:
                df = pd.read_sql_query(f"SELECT * FROM {safe_table}", src_conn)
                transform(df, table_name)
            except Exception as e:
                print(f"Error reading table {table_name}: {e}")
    except Exception as e:
        print(f"Error during extraction: {e}")  
    finally:
        try:
            if src_conn is not None:
                src_conn.close()
        except Exception:
            pass

#nettoyage et transformation des donnees
def transform(df, table_name: str):
    if df is None or df.empty:
        return pd.DataFrame()
    df = df.dropna(how="all")
    for col in df.select_dtypes(include="object").columns:
        df[col] = df[col].astype(str).str.strip()
    load(df, table_name)
    return df

#creer les fichiers csv
def load(df, table_name: str):
    df.to_csv(f"data/csv/{table_name}.csv", index=False)
    print(f"Data from table {table_name} written to {table_name}.csv")        

if __name__ == "__main__":
    extract()    