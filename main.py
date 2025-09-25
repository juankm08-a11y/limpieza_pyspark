from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, avg, sum as spark_sum, count

spark = SparkSession.builder.appName("LimpiezaConcesionario").getOrCreate()

df = spark.read.option("header", True).option("inferSchema", True).csv("csv/concesionario_limpio.csv/part-00000-56c4fd3b-c26c-4f81-a4be-7731a05798fe-c000.csv")

df = df.withColumn("marca", when(col("marca") == "totyot", "toyota").otherwise(col("marca")))

gasto_por_cliente = df.groupBy("cliente").agg(spark_sum("precio").alias("gasto_total"))
cliente_max = gasto_por_cliente.orderBy(col("gasto_total").desc()).first()
print(f"1. Cliente con mayor gasto: {cliente_max['cliente']} - Valor: {cliente_max['gasto_total']}")

cliente_min = gasto_por_cliente.orderBy(col("gasto_total").asc()).first()
print(f"2. Cliente con menor gasto: {cliente_min['cliente']} - Valor: {cliente_min['gasto_total']}")

promedio_marcas = df.groupBy("marca").agg(avg("precio").alias("precio_promedio"))
marca_max = promedio_marcas.orderBy(col("precio_promedio").desc()).first()
print(f"3. Marca con mayor precio promedio: {marca_max['marca']} - {marca_max['precio_promedio']:.2f}")

marca_min = promedio_marcas.orderBy(col("precio_promedio").asc()).first()
print(f"4. Marca con menor precio promedio: {marca_min['marca']} - {marca_min['precio_promedio']:.2f}")

ventas_concesionario = df.groupBy("concesionario").agg(count("*").alias("total_ventas"))
concesionario_max = ventas_concesionario.orderBy(col("total_ventas").desc()).first()
print(f"5. Concesionario con más ventas: {concesionario_max['concesionario']} - {concesionario_max['total_ventas']} ventas")

marca_mas_vendida = df.groupBy("marca").agg(count("*").alias("total_ventas"))
marca_max_ventas = marca_mas_vendida.orderBy(col("total_ventas").desc()).first()
print(f"6. Marca más vendida: {marca_max_ventas['marca']} - {marca_max_ventas['total_ventas']} ventas")

output_path = "out/"
df.write.mode("overwrite").option("header", True).csv(output_path)

spark.stop()
