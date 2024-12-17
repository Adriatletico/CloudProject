from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, max, min, count, stddev

# Crear sesión de Spark
spark = SparkSession.builder.appName("BasicStats").getOrCreate()

# Ruta del archivo de entrada
data_path = "gs://horizontal-time-442413-m5/crypto-transactions.csv"

# Ruta para guardar las estadísticas
output_path = "gs://horizontal-time-442413-m5/output/basic_stats"

# Cargar datos
df = spark.read.csv(data_path, header=True, inferSchema=True)

# Calcular estadísticas básicas
stats = df.select(
    avg("value").alias("avg_value"),
    max("value").alias("max_value"),
    min("value").alias("min_value"),
    stddev("value").alias("stddev_value"),
    count("*").alias("total_transactions")
)

# Mostrar estadísticas en la consola
print("Estadísticas básicas del archivo completo:")
stats.show()

# Guardar estadísticas en formato CSV
stats.write.csv(output_path, header=True, mode="overwrite")

print(f"Estadísticas guardadas en: {output_path}")

# Finalizar sesión de Spark
spark.stop()
