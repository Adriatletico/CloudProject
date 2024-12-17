import pandas as pd
import matplotlib.pyplot as plt

# Cargar los datos desde los archivos combinados
all_data_path = "all_data_distribution_combined.csv"
anomalies_path = "anomalies_distribution_combined.csv"

# Leer los archivos CSV asegurando que las columnas sean numéricas
all_data = pd.read_csv(all_data_path, dtype={"value": str, "count": str}, low_memory=False)
anomalies = pd.read_csv(anomalies_path, dtype={"value": str, "count": str}, low_memory=False)

# Convertir a numéricos, reemplazando valores no numéricos o vacíos con NaN
all_data["value"] = pd.to_numeric(all_data["value"], errors="coerce")
all_data["count"] = pd.to_numeric(all_data["count"], errors="coerce")
anomalies["value"] = pd.to_numeric(anomalies["value"], errors="coerce")
anomalies["count"] = pd.to_numeric(anomalies["count"], errors="coerce")

# Eliminar filas con valores NaN o count negativos
all_data = all_data.dropna(subset=["value", "count"])
all_data = all_data[all_data["count"] > 0]

anomalies = anomalies.dropna(subset=["value", "count"])
anomalies = anomalies[anomalies["count"] > 0]

# Expandir los datos según el 'count' (frecuencia de cada valor)
all_data_expanded = all_data.loc[all_data.index.repeat(all_data["count"].astype(int))]
anomalies_expanded = anomalies.loc[anomalies.index.repeat(anomalies["count"].astype(int))]

# Visualizar la distribución de los datos completos en ESCALA LOGARÍTMICA
plt.figure(figsize=(12, 6))
plt.hist(all_data_expanded["value"], bins=50, alpha=0.7, label="All Data", color="blue", log=True)
plt.title("Distribución de valores de todas las transacciones (Escala Logarítmica)")
plt.xlabel("Valor")
plt.ylabel("Frecuencia (log)")
plt.legend()
plt.grid(True)
plt.savefig("all_data_distribution_log.png")
plt.show()

# Filtrar valores atípicos utilizando el percentil 99
threshold = all_data_expanded["value"].quantile(0.99)

# Filtrar valores por debajo del umbral
filtered_data = all_data_expanded[all_data_expanded["value"] <= threshold]

# Visualizar la distribución sin outliers
plt.figure(figsize=(12, 6))
plt.hist(filtered_data["value"], bins=50, alpha=0.7, label="All Data (Sin Outliers)", color="blue")
plt.title("Distribución de valores de todas las transacciones (Sin Outliers)")
plt.xlabel("Valor")
plt.ylabel("Frecuencia")
plt.legend()
plt.grid(True)
plt.savefig("all_data_distribution_filtered.png")
plt.show()

# Visualizar la distribución de las anomalías en ESCALA LOGARÍTMICA
plt.figure(figsize=(12, 6))
plt.hist(anomalies_expanded["value"], bins=50, alpha=0.7, label="Anomalies", color="red", log=True)
plt.title("Distribución de valores de transacciones anómalas (Escala Logarítmica)")
plt.xlabel("Valor")
plt.ylabel("Frecuencia (log)")
plt.legend()
plt.grid(True)
plt.savefig("anomalies_distribution_log.png")
plt.show()

# Filtrar valores atípicos en anomalías usando el percentil 99
anomalies_threshold = anomalies_expanded["value"].quantile(0.99)
filtered_anomalies = anomalies_expanded[anomalies_expanded["value"] <= anomalies_threshold]

# Visualizar la distribución de anomalías sin outliers
plt.figure(figsize=(12, 6))
plt.hist(filtered_anomalies["value"], bins=50, alpha=0.7, label="Anomalies (Sin Outliers)", color="red")
plt.title("Distribución de valores de transacciones anómalas (Sin Outliers)")
plt.xlabel("Valor")
plt.ylabel("Frecuencia")
plt.legend()
plt.grid(True)
plt.savefig("anomalies_distribution_filtered.png")
plt.show()

print("Visualización completada. Las imágenes se han guardado en el directorio actual.")
