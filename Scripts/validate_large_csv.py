from pyspark.sql import SparkSession

# Crear sesión de Spark
spark = SparkSession.builder.appName("ValidateLargeCSV").getOrCreate()

# Ruta al archivo completo
data_path = "gs://horizontal-time-442413-m5/crypto-transactions.csv"

# Leer el archivo
df = spark.read.csv(data_path, header=True, inferSchema=True)

# Mostrar esquema
print("Esquema del archivo completo:")
df.printSchema()

# Mostrar las primeras filas
print("Primeras filas del archivo completo:")
df.show(10)

# Contar total de filas
total_rows = df.count()
print(f"Total de filas en el archivo completo: {total_rows}")

# Finalizar sesión
spark.stop()
