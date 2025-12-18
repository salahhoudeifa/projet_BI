from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL
import pandas as pd
import os

server = r"DESKTOP-RV0ORQK\SQLEXPRESS_SALAH"
database = "ETL_Northwind"
username = "dbo"
driver = "ODBC Driver 17 for SQL Server"


#extraire des donnees des fichiers excel (extension .xlsx)
def extract(file):
    df = pd.read_excel(file)
    table_name = os.path.splitext(os.path.basename(file))[0]
    transform(df, table_name)

#nettoyer les donnees
def transform(df, table_name: str):
    if df is None:
        return pd.DataFrame()
    df = df.dropna(how="all")
    for col in df.select_dtypes(include="object").columns:
        df[col] = df[col].astype(str).str.strip()
    load(df, table_name)
    return df

#apres le nettoyage, on exporte les donnees vers
#une base de donnees qui joue le role d'un data
#warehouse (d'apres ce projet, on utilise SQL Servers)
def load(df, table_name: str):
    connection_url = URL.create(
        "mssql+pyodbc",
        host=server,
        database=database,
        query={"driver": driver, "trusted_connection": "yes"},
    )
    engine = create_engine(connection_url)

    if '.' in table_name:
        schema, tbl = table_name.split('.', 1)
    else:
        schema, tbl = 'dbo', table_name

    try:
        with engine.connect() as conn:
            exists_q = text(
                "SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = :schema AND TABLE_NAME = :name"
            )
            result = conn.execute(exists_q, {"schema": schema, "name": tbl})
            exists_count = result.scalar() or 0

            if exists_count:
                print(f"Table {schema}.{tbl} exists (count={exists_count}); it will be replaced.")
            else:
                print(f"Table {schema}.{tbl} does not exist; it will be created.")

            df.to_sql(tbl, con=conn, schema=schema, if_exists='replace', index=False, method='multi')
            print("Data loaded successfully")

    except Exception as e:
        print(f"Error during loading: {e}")


if __name__ == "__main__":
    for file in os.listdir('data/xlsx/'):
        if file.endswith('.xlsx'):
            extract(os.path.join('data/xlsx/', file))