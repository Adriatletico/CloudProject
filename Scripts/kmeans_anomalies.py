from pyspark.sql import SparkSession
from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import VectorAssembler
from pyspark.sql.functions import udf
from pyspark.sql.types import DoubleType
import numpy as np

# Crear sesión de Spark
spark = SparkSession.builder.appName("KMeansAnomaliesSmall").getOrCreate()

# Restaurar el comportamiento de manejo de fechas a Spark 2.x para evitar errores
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

# Ruta al archivo
data_path = "gs://horizontal-time-442413-m5/crypto-transactions.csv"

# Cargar datos
df = spark.read.csv(data_path, header=True, inferSchema=True)

# Convertir block_timestamp a formato numérico para análisis
df = df.withColumn("timestamp_numeric", df["block_timestamp"].cast("timestamp").cast("long"))

# Seleccionar columnas relevantes para el clustering
features_df = df.select("value", "timestamp_numeric").na.drop()

# Ensamblar las columnas seleccionadas en un vector
assembler = VectorAssembler(inputCols=["value", "timestamp_numeric"], outputCol="features")
vector_df = assembler.transform(features_df)

# Configurar el modelo K-Means
kmeans = KMeans(k=5, seed=1, featuresCol="features", predictionCol="cluster")
model = kmeans.fit(vector_df)

# Añadir las predicciones al DataFrame
clustered_df = model.transform(vector_df)

# Obtener los centros del clúster
centers = model.clusterCenters()

# UDF para calcular la distancia euclidiana al centro más cercano
def compute_distance(features, cluster):
    center = np.array(centers[cluster])
    features_array = np.array(features.toArray())
    return float(np.sqrt(np.sum((features_array - center) ** 2)))

# Registrar la UDF
distance_udf = udf(compute_distance, DoubleType())

# Añadir columna con la distancia calculada
clustered_df = clustered_df.withColumn(
    "distance",
    distance_udf(clustered_df["features"], clustered_df["cluster"])
)

# Filtrar transacciones cuya distancia al centro esté en el percentil 95
threshold = clustered_df.approxQuantile("distance", [0.95], 0.05)[0]
anomalies_df = clustered_df.filter(clustered_df["distance"] > threshold)

# Mostrar transacciones anómalas
print("Transacciones anómalas detectadas:")
anomalies_df.show(10)

# Guardar las anomalías detectadas en el bucket
anomalies_output_path = "gs://horizontal-time-442413-m5/output/anomalous_transactions_large"
anomalies_df.drop("features").write.csv(anomalies_output_path, header=True)


print(f"Transacciones anómalas guardadas en: {anomalies_output_path}")

# Finalizar sesión de Spark
spark.stop()
