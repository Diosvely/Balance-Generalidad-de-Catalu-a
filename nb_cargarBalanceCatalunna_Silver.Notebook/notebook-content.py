# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "5615efcd-392e-4999-b961-e303f68d576a",
# META       "default_lakehouse_name": "lh_datoscrudoCataluña",
# META       "default_lakehouse_workspace_id": "23b09664-88cc-4cc1-87df-e4ee207345fd",
# META       "known_lakehouses": [
# META         {
# META           "id": "5615efcd-392e-4999-b961-e303f68d576a"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Leer el archivo Csv factDiario.csv desde el Lakehouse (Bronce), transformarlo a DataFrame Spark y mostrar una vista previa.


# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, DateType

# ------------------------
# Ruta del CSV en Lakehouse
# ------------------------
csv_path = "abfss://23b09664-88cc-4cc1-87df-e4ee207345fd@onelake.dfs.fabric.microsoft.com/5615efcd-392e-4999-b961-e303f68d576a/Files/Balance situacion Generalidad de Cataluña.csv"

# ------------------------
# Lectura inicial con Spark
# - header=True: primera fila son nombres de columnas
# - inferSchema=True: Spark intenta detectar tipos automáticamente
# - sep=';' : separador de columnas (ajustar si es tab)
# ------------------------
df_spark = spark.read.option("header", True) \
                     .option("inferSchema", True) \
                     .option("sep", ";") \
                     .option("encoding", "ISO-8859-1") \
                     .csv(csv_path)

# ------------------------
# Corrección de nombres de columnas mal codificados
# ------------------------

# Diccionario de reemplazo de encabezados mal decodificados → correcto
column_rename_map = {
    "Exercici": "Ejercicio",
    "PerÃ­ode": "Periodo",
    "VersiÃ³": "Version",
    "Entitat": "Entidad",
    "Descriptiu Entitat": "Descripcion_Entidad",
    "AcrÃ²nim": "Acronimo",
    "Tipus": "Tipo",
    "EpÃ­graf": "Epigrafe",
    "Descriptiu EpÃ­graf": "Descripcion_Epigrafe",
    "Moneda": "Moneda",
    "Import": "Importe"
}

# Renombrar las columnas según el diccionario
for old_name, new_name in column_rename_map.items():
    if old_name in df_spark.columns:
        df_spark = df_spark.withColumnRenamed(old_name, new_name)

# Validar resultado
df_spark.printSchema()
df_spark.show(5, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
