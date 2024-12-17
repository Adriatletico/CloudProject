from pyspark.sql import SparkSession

# Crear sesión de Spark
spark = SparkSession.builder.appName("ValidateSmallCSV").getOrCreate()

# Ruta del archivo reducido en el bucket
data_path = "gs://horizontal-time-442413-m5/crypto-transactions-small.csv"

# Cargar los datos desde el CSV
df = spark.read.csv(data_path, header=True, inferSchema=True)

# Mostrar el esquema del archivo
print("Esquema del archivo reducido:")
df.printSchema()

# Mostrar una muestra de filas
print("Primeras filas del archivo reducido:")
df.show(10)

# Contar filas
print(f"Total de filas en el archivo reducido: {df.count()}")

spark.stop()
