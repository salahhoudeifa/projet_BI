from sqlalchemy import create_engine
from sqlalchemy.engine import URL
import pandas as pd

class ConnectionHandler:

    def __init__(
        self,
        server: str,
        database: str,
        driver: str,
        username: str,
        use_trusted_connection: bool = True,
    ) -> None:
        self.server = server
        self.database = database
        self.username = username
        self.driver = driver
        self.use_trusted_connection = use_trusted_connection
        self.engine = None

    def get_connection(self):
            if self.engine is None:
                connection_url = URL.create(
                    "mssql+pyodbc",
                    username=self.username,
                    password="",
                    host=self.server,
                    database=self.database,
                    query={
                        "driver": self.driver,
                        "trusted_connection": "yes"
                        if self.use_trusted_connection
                        else "no",
                    },
                )
                self.engine = create_engine(connection_url)
            return self.engine.connect()
    

def extract():
    conn_str = (
        f"DRIVER={driver};SERVER={server};DATABASE={database};Trusted_Connection=yes;"
    )
    src_conn = None
    try:
        src_conn = __import__('pyodbc').connect(conn_str, autocommit=True)
        cursor = src_conn.cursor()
        query = (
            "SELECT t.name as table_name FROM sys.tables t where t.name in "
            "('Customers', 'Orders', 'Products', 'Suppliers', 'Employees', 'Shippers', 'Categories', 'Order Details', 'Region', 'Territories', 'EmployeeTerritories', 'CustomerDemographics', 'CustomerCustomerDemo')"
        )
        cursor.execute(query)
        src_tables = cursor.fetchall()

        for table_row in src_tables:
            raw_name = table_row[0]
            if isinstance(raw_name, bytes):
                for enc in ("utf-8", "utf-16le", "latin-1"):
                    try:
                        table_name = raw_name.decode(enc)
                        break
                    except Exception:
                        table_name = None
                if table_name is None:
                    print("Unable to decode table name; skipping:", raw_name)
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

def transform(df, table_name):
    if df is None or df.empty:
        return pd.DataFrame()
    df = df.dropna(how="all")
    for col in df.select_dtypes(include="object").columns:
        df[col] = df[col].astype(str).str.strip()
    load(df, table_name)
    return df

def load(df, table_name):
    engine = create_engine(URL.create(
        "mssql+pyodbc",
        username=username,
        password="",
        host=server,
        database="ETL_Northwind",
        query={"driver": driver, "trusted_connection": "yes"},
    ))
    try:
        df.to_sql(table_name, con=engine, if_exists='replace', index=False)
        print(f"Data loaded to table {table_name}")
    except Exception as e:
        print(f"Error during loading: {e}")

if __name__ == "__main__":
    server = r"DESKTOP-RV0ORQK\SQLEXPRESS_SALAH"
    database = "Northwind"
    username = "dbo"
    driver = "ODBC Driver 17 for SQL Server"

    conn_handler = ConnectionHandler(
        server=server,
        database=database,
        driver=driver,
        username=username,
        use_trusted_connection=True,
    )

    df = extract()