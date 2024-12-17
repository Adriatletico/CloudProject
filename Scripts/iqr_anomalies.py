from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from pyspark.sql.functions import expr

# Crear sesión de Spark
spark = SparkSession.builder.appName("IQR_Anomalies").getOrCreate()

# Ruta al archivo de datos
data_path = "gs://horizontal-time-442413-m5/crypto-transactions.csv"

# Cargar datos
df = spark.read.csv(data_path, header=True, inferSchema=True)

# Seleccionar la columna relevante
value_df = df.select("value").na.drop()

# Calcular los percentiles
quantiles = value_df.approxQuantile("value", [0.25, 0.75], 0.05)  # Q1 y Q3
q1, q3 = quantiles[0], quantiles[1]

# Calcular el rango intercuartílico (IQR)
iqr = q3 - q1

# Definir los límites inferior y superior
lower_bound = q1 - 1.5 * iqr
upper_bound = q3 + 1.5 * iqr

# Filtrar valores atípicos (outliers)
anomalies_df = value_df.filter((col("value") < lower_bound) | (col("value") > upper_bound))

# Mostrar las transacciones anómalas detectadas
print("Transacciones anómalas detectadas (IQR):")
anomalies_df.show(10)

# Guardar las transacciones anómalas en el bucket
anomalies_output_path = "gs://horizontal-time-442413-m5/output/anomalous_transactions_iqr"
anomalies_df.write.csv(anomalies_output_path, header=True)

print(f"Transacciones anómalas guardadas en: {anomalies_output_path}")

# Finalizar sesión de Spark
spark.stop()
