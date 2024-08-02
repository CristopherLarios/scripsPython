import pyodbc
import csv
from tqdm import tqdm

# Establecer la cadena de conexión a SQL Server en Azure
server = 'sql-aludra-prod-01.database.windows.net'
database = 'DB_Cochez'
username = 'hnladmin'
password = 'Linaka10'
driver = '{ODBC Driver 17 for SQL Server}'
connection_string = f'DRIVER={driver};SERVER={server};DATABASE={database};UID={username};PWD={password}'

# Establecer la conexión a la base de datos
connection = pyodbc.connect(connection_string)

# Leer códigos de productos desde un archivo CSV
with open('codigosivend.csv', 'r') as file:
    reader = csv.reader(file)
    product_codes = [row[0] for row in reader]

# Paso 1: Ejecutar la primera consulta para obtener ProductIDs
product_ids = set()  # Utilizamos un conjunto para almacenar valores únicos
query_1 = f"SELECT DISTINCT Productid FROM MDL04.pltblProductExternalCode WHERE ExternalCode IN ({','.join(['?'] * len(product_codes))})"
cursor = connection.cursor()
cursor.execute(query_1, *product_codes)
for row in cursor:
    product_ids.add(row[0])  # Accede al primer elemento (columna) de la fila

# Paso 2: Ejecutar la segunda consulta para obtener LogIDs
log_ids = set()
query_2 = f"SELECT DISTINCT Logid FROM MDL10.tblEventLogPrice WHERE ProductId IN ({','.join(['?'] * len(product_ids))})"
cursor.execute(query_2, *product_ids)
for row in cursor:
    log_ids.add(row[0])  # Accede al primer elemento (columna) de la fila

# Paso 3: Realizar las actualizaciones en la base de datos con barra de progreso por lotes
batch_size = 200  # Puedes ajustar el tamaño del lote según tus necesidades
query_update1 = "UPDATE MDL10.tblEventLogPrice SET Status = 1, Complete = 0, Processed = 0 WHERE LogId = ?"
with tqdm(total=len(log_ids)) as pbar:
    cursor.execute("BEGIN TRANSACTION")
    for i, log_id in enumerate(log_ids):
        cursor.execute(query_update1, log_id)
        if (i + 1) % batch_size == 0 or i == len(log_ids) - 1:
            cursor.execute("COMMIT")
        pbar.update(1)

# Paso 4: Realizar las actualizaciones en la base de datos con barra de progreso por lotes
query_update2 = "UPDATE MDL10.Tbleventlogfile SET Status = 1, Complete = 0, Processed = 0 WHERE LogId = ?"
with tqdm(total=len(log_ids)) as pbar:
    cursor.execute("BEGIN TRANSACTION")
    for i, log_id in enumerate(log_ids):
        cursor.execute(query_update2, log_id)
        if (i + 1) % batch_size == 0 or i == len(log_ids) - 1:
            cursor.execute("COMMIT")
        pbar.update(1)

# Confirmar y cerrar la transacción
cursor.execute("COMMIT")

# Cerrar la conexión a la base de datos
connection.close()