from pyspark.sql import SparkSession

# Crear sesión de Spark
spark = SparkSession.builder.appName("SaveDistributions").getOrCreate()

# Ruta a los datos ya procesados
all_data_path = "gs://horizontal-time-442413-m5/crypto-transactions.csv"
anomalies_path = "gs://horizontal-time-442413-m5/output/anomalous_transactions_iqr/"

# Cargar datos
all_data_df = spark.read.csv(all_data_path, header=True, inferSchema=True)
anomalies_df = spark.read.csv(anomalies_path, header=True, inferSchema=True)

# Calcular frecuencias para los datos normales
all_data_distribution = all_data_df.groupBy("value").count()

# Calcular frecuencias para las anomalías
anomalies_distribution = anomalies_df.groupBy("value").count()

# Guardar distribuciones en Google Cloud Storage
all_data_distribution_path = "gs://horizontal-time-442413-m5/output/all_data_distribution"
anomalies_distribution_path = "gs://horizontal-time-442413-m5/output/anomalies_distribution"

# Guardar como CSV
all_data_distribution.write.csv(all_data_distribution_path, header=True)
anomalies_distribution.write.csv(anomalies_distribution_path, header=True)

# Opcional: Guardar como Parquet (formato más eficiente)
all_data_distribution_parquet_path = "gs://horizontal-time-442413-m5/output/all_data_distribution_parquet"
anomalies_distribution_parquet_path = "gs://horizontal-time-442413-m5/output/anomalies_distribution_parquet"

all_data_distribution.write.parquet(all_data_distribution_parquet_path)
anomalies_distribution.write.parquet(anomalies_distribution_parquet_path)

# Mensajes informativos
print(f"Distribución de datos normales guardada en: {all_data_distribution_path}")
print(f"Distribución de anomalías guardada en: {anomalies_distribution_path}")
print(f"Distribución de datos normales (Parquet) guardada en: {all_data_distribution_parquet_path}")
print(f"Distribución de anomalías (Parquet) guardada en: {anomalies_distribution_parquet_path}")

# Finalizar sesión
spark.stop()
