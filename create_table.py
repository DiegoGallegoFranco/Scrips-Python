import pyodbc

SERVER = 'host'
DATABASE = 'database_name'
USERNAME = 'user'
PASSWORD = 'password'

#connectionString = f'DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={SERVER};DATABASE={DATABASE};UID={USERNAME};PWD={PASSWORD}'

connection_string = (
    f"DRIVER={'ODBC Driver 18 for SQL Server'};SERVER=tcp:host,1433;"
    f"DATABASE=consignas;Uid=c2-horus;Pwd={'password'};Encrypt=yes;"
    f"TrustServerCertificate=no;Connection Timeout=30;"
    f"DSN=my_dsn"
)
try:
    # Connect to the database
    connection = pyodbc.connect(connection_string)

    # Create the table
    cursor = connection.cursor()
    cursor.execute(
        """
        CREATE TABLE [dbo].[table_name] (
            Id INT NOT NULL,
            Matricula VARCHAR(255),
            Bimas INT,
            Informacion_complemetaria VARCHAR(255),
            Fecha_inicial DATE,
            Fecha_final DATE,
            Observaciones VARCHAR(255),
            Ordenado_por VARCHAR(255),
            Creada_por VARCHAR(255),
            Consigna_ VARCHAR(255),
            CONSTRAINT [PK_registros] PRIMARY KEY (Id)
        )
        """
    )

    # Commit the changes
    connection.commit()

    # Close the connection
    connection.close()

except Exception as e:
    print(f"Error: {e}")

