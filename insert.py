import pyodbc

SERVER = 'host'
DATABASE = 'database_name'
USERNAME = 'user'
PASSWORD = 'password'

#connectionString = f'DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={SERVER};DATABASE={DATABASE};UID={USERNAME};PWD={PASSWORD}'

connection_string = (
    f"DRIVER={'ODBC Driver 18 for SQL Server'};SERVER=tcp:remplace_host,1433;"
    f"DATABASE=consignas;Uid=user;Pwd={'password'};Encrypt=yes;"
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
        INSERT INTO [dbo].[table_name]
        (
            Id,
            Matricula,
            Bimas,
            Informacion_complemetaria,
            Fecha_inicial,
            Fecha_final,
            Observaciones,
            Creada_por,
            Consigna_ 
        )
        VALUES
        (
            ?,
            ?,
            ?,
            ?,
            ?,
            ?,
            ?,
            ?,
            ?
        )
        """,
        130,
        "fac4110",
        234,
        "hola",
        "2023-11-01",
        "2023-11-08",
        "test",
        "At. Gallego Franco Diego",
        "gracias carlos"
    )

    # Commit the changes
    connection.commit()

    # Close the connection
    connection.close()

except Exception as e:
    print(f"Error: {e}")
