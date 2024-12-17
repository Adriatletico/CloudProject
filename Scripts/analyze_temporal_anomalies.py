import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

# Rutas de los archivos
all_data_path = "all_data_distribution_combined.csv"
anomalies_path = "anomalies_distribution_combined.csv"

# Leer archivos como texto (sin aplicar dtypes inicialmente)
all_data = pd.read_csv(all_data_path, skiprows=1, names=["value", "count"], low_memory=False)
anomalies = pd.read_csv(anomalies_path, skiprows=1, names=["value", "count"], low_memory=False)

# Limpiar valores no numéricos y convertir a float e int
all_data = all_data[pd.to_numeric(all_data["value"], errors="coerce").notnull()]
all_data["value"] = all_data["value"].astype(float)
all_data["count"] = all_data["count"].astype(int)

anomalies = anomalies[pd.to_numeric(anomalies["value"], errors="coerce").notnull()]
anomalies["value"] = anomalies["value"].astype(float)
anomalies["count"] = anomalies["count"].astype(int)

# Generar fechas aleatorias dentro de un rango
np.random.seed(42)  # Para reproducibilidad
all_data["timestamp"] = pd.to_datetime(np.random.randint(1672531200, 1704067200, size=len(all_data)), unit="s")

anomalies["timestamp"] = pd.to_datetime(np.random.randint(1672531200, 1704067200, size=len(anomalies)), unit="s")

# Agrupar por fecha para sumar las transacciones
all_data_grouped = all_data.groupby("timestamp")["count"].sum().reset_index()
anomalies_grouped = anomalies.groupby("timestamp")["count"].sum().reset_index()

# Renombrar columnas para claridad
all_data_grouped.rename(columns={"count": "total_transactions"}, inplace=True)
anomalies_grouped.rename(columns={"count": "anomalous_transactions"}, inplace=True)

# Fusionar datos en un solo DataFrame
merged_data = pd.merge(all_data_grouped, anomalies_grouped, on="timestamp", how="left").fillna(0)

# Graficar la evolución temporal
plt.figure(figsize=(12, 6))
plt.plot(merged_data["timestamp"], merged_data["total_transactions"], label="Total Transactions", color="blue")
plt.plot(merged_data["timestamp"], merged_data["anomalous_transactions"], label="Anomalous Transactions", color="red")
plt.title("Evolución de Transacciones Totales vs Anómalas")
plt.xlabel("Fecha")
plt.ylabel("Número de Transacciones")
plt.legend()
plt.grid(True)
plt.tight_layout()
plt.savefig("temporal_anomalies.png")  # Guardar el gráfico
plt.show()

print("Gráfico generado y guardado como 'temporal_anomalies.png'.")
