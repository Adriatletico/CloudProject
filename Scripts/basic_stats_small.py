from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, max, min, count, stddev

# Crear sesión de Spark
spark = SparkSession.builder.appName("BasicStatsSmall").getOrCreate()

# Ruta del archivo reducido
data_path = "gs://horizontal-time-442413-m5/crypto-transactions-small.csv"

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

# Mostrar estadísticas
print("Estadísticas básicas del archivo reducido:")
stats.show()

spark.stop()
