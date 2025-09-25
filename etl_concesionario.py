from pyspark.sql import SparkSession
from pyspark.sql.functions import col,trim,lower,when

spark = SparkSession.builder \
    .appName("ETL_Calificaciones") \
        .master("local[*]") \
            .getOrCreate()
            
print("--- DATOS ORIGINALES ---")

input_path = "concesionario_sucio.csv"

# Leemos el archivo limpio
df = spark.read.csv(input_path,header=True,inferSchema=True)

# Mostramos los datos limpios 
df.show(truncate=False)

# 1. Eliminamos duplicados
df = df.dropDuplicates()

string_columns = [field.name for field in df.schema.fields if field.dataType.simpleString() == "string"]
for column in string_columns:
    df = df.withColumn(column,trim(lower(col(column))))
    

# 3. Eliminamos filas con valores nulos
df = df.dropna()

# 4. Normalizar marcas incorrectas
df = (
    df.withColumn("marca",
    when(col("marca")  == "totyot","toyota")
    .when(col("marca") == "toyta","toyota")
    .when(col("marca") == "honda","honda")
    .when(col("marca")=="kiaa","kia")
    .when(col("marca")=="chevy","chevrolet")
    .otherwise(col("marca")))
)

df = df.filter(col("precio") > 0)

output_path = "csv/concesionario_limpio.csv"

df.write.mode("overwrite").option("header",True).csv(output_path)

spark.stop()