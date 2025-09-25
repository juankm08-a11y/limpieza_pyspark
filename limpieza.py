from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim,upper
import os
import shutil 

# 1. Creamos la sesion de Spark
spark = SparkSession.builder \
    .appName("ETL_Ventas_limpieza") \
        .master("local[*]") \
            .getOrCreate()
            

# 2. Leemos el archivo sucio
df = spark.read.csv("calificaciones_sucias.csv",header=True,inferSchema=True)

# 3. Mostramos los datos originales
print("---- DATOS ORIGINALES: ------")
df.show(truncate=False)

# 4. Lipiamos los espacios en nombres de columnas y los convertimos a mayusculas

for col_name in df.columns:
    df = df.withColumnRenamed(col_name,col_name.strip().upper())
    
# 5. Limpiamos espacios en datos
df_clean = df.select([trim(col(c)).alias(c) if df.schema[c].dataType == "string" else col(c) for c in df.columns])

# 6 Eliminamos los registros con calificación nula
df_clean = df_clean.na.drop(subset=["CALIFICACION"])

# Guardamos el archivo limpio
output_path = "csv/calificaciones_limpias.csv"

if os.path.exists(output_path):
    shutil.rmtree(output_path)
    
df_clean.coalesce(1).write.mode("overwrite").option("header",True).csv(output_path)

print(f"Archivo limpiado y guardado correctamente en: {output_path}")

spark.stop()