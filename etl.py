from pyspark.sql import SparkSession
from pyspark.sql.functions import col,avg,max

spark = SparkSession.builder \
    .appName("ETL_Calificaciones") \
        .master("local[*]") \
            .getOrCreate()
            

# Leemos el archivo limpio
df = spark.read.csv("csv/calificaciones_limpias.csv/part-00000-592da84e-eabc-4690-a004-7c2f31f426ec-c000.csv",header=True,inferSchema=True)

# Mostramos los datos limpios 
df.show(truncate=False)

# Top 10 mejores estudiantes por promedio
top_mejores_estudiantes = df.groupBy("NOMBRE").agg(avg("CALIFICACION").alias("PROMEDIO")) \
    .orderBy(col("PROMEDIO").desc())
    
print(f"Top 10 mejores estudiantes por promedio: {top_mejores_estudiantes}")
top_mejores_estudiantes.show(10,truncate=False)

# Top 10 peores estudiantes por promedio
top_peores_estudiantes = df.groupBy("NOMBRE").agg(avg("CALIFICACION").alias("PROMEDIO")) \
    .orderBy(col("PROMEDIO").asc())
    
print(f"Top 10 peores estudiantes por promedio:{top_peores_estudiantes}")
top_mejores_estudiantes.show(10,truncate=False)

# Estudiante con la mejor nota
mejor_nota = df.orderBy(col("CALIFICACION").desc()).limit(1)
print(f"Esudiante con la mejor calificación: {mejor_nota}")
mejor_nota.show(truncate=False)

# top 10 mejores universidades por promedio
top_mejores_universidades = df.groupBy("UNIVERSIDAD").agg(avg("CALIFICACION").alias("PROMEDIO")) \
    .orderBy(col("PROMEDIO").desc())
    
print(f"Top 10 mejores universidades por promedio: {top_mejores_universidades}")
top_mejores_universidades.show(10,truncate=False)

# top 10 peores universidades por promedio
top_peores_universidades = df.groupBy("UNIVERSIDAD").agg(avg("CALIFICACION").alias("PROMEDIO")) \
    .orderBy(col("PROMEDIO").desc())
    
print(f"Top 10 peores universidades por promedio: {top_peores_universidades}")
top_peores_universidades.show(10,truncate=False)

top_mejores_estudiantes.coalesce(1).write.mode("overwrite").option("header",True).csv("out/top_mejores_estudiantes")
top_peores_estudiantes.coalesce(1).write.mode("overwrite").option("header",True).csv("out/top_peores_estudiantes")
mejor_nota.coalesce(1).write.mode("overwrite").option("header",True).csv("out/mejor_nota")
top_mejores_universidades.coalesce(1).write.mode("overwrite").option("header",True).csv("out/top_mejores_universidades")
top_peores_universidades.coalesce(1).write.mode("overwrite").option("header",True).csv("out/top_peores_universidades")

print("Resultados guardados correctamente en la carpeta 'out'/")

spark.stop()