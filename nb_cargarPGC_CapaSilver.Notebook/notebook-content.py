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

# # Leer el archivo Excel `dimPGC.xlsx` desde el Lakehouse (Bronce), transformarlo a DataFrame Spark y mostrar una vista previa.


# CELL ********************

import pandas as pd  # Librería para manejar Excel

# Ruta del archivo Excel en Lakehouse
excel_path = "abfss://23b09664-88cc-4cc1-87df-e4ee207345fd@onelake.dfs.fabric.microsoft.com/5615efcd-392e-4999-b961-e303f68d576a/Files/dimPGC.xlsx"

# Leemos la hoja 'dimPGC' en un DataFrame de pandas
df_pandas = pd.read_excel(excel_path, sheet_name="dimPGC")

# Convertimos el DataFrame de pandas a Spark
df_spark = spark.createDataFrame(df_pandas)

# Mostramos las primeras filas para validar la carga
df_spark.show()



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Guardar datos de la tabla dimPGC en el warehouse schema silver

# CELL ********************

import com.microsoft.spark.fabric                # Importa librerías Fabric para escribir en DW
from com.microsoft.spark.fabric.Constants import Constants  # Constantes para la integración

# Escribimos el DataFrame en el Data Warehouse, esquema Silve

# Sobrescribe la tabla si ya existe
df_spark.write \
    .mode("overwrite").synapsesql("DWH_Catalunna.SILVER.dimPGC")  # Destino: DW, esquema Silver, tabla dimPGC


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
