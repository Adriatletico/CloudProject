import pandas as pd
import matplotlib.pyplot as plt

# Ruta del archivo de anomalías
anomalies_path = "anomalies_distribution_combined.csv"

# Leer el archivo sin encabezado y convertir columnas manualmente
anomalies = pd.read_csv(
    anomalies_path,
    skiprows=1,  # Ignora la primera fila (encabezado)
    header=None,  # No usar encabezado automático
    names=["value", "count"],  # Asignar nombres correctos a las columnas
    low_memory=False
)

# Asegurarse de que 'count' sea de tipo entero, ignorando errores
anomalies["count"] = pd.to_numeric(anomalies["count"], errors="coerce").fillna(0).astype(int)

# Reducir las filas si es necesario
anomalies = anomalies.head(365)  # Limitar a 1 año de datos

# Generar un rango de fechas diario
anomalies["date"] = pd.date_range(start="2023-01-01", periods=len(anomalies), freq="D")

# Agrupar las anomalías por fecha y sumar la cantidad de ocurrencias
anomalies_per_day = anomalies.groupby("date")["count"].sum().reset_index()

# Ordenar por la cantidad de anomalías descendente y mostrar los primeros 10 días
top_anomaly_days = anomalies_per_day.sort_values(by="count", ascending=False).head(10)

# Mostrar los días con más anomalías en la consola
print("Días con más transacciones anómalas:")
print(top_anomaly_days)

# Graficar los días con más anomalías
plt.figure(figsize=(12, 6))
plt.bar(top_anomaly_days["date"].astype(str), top_anomaly_days["count"], color="red", alpha=0.7)
plt.title("Top 10 Días con Más Transacciones Anómalas")
plt.xlabel("Fecha")
plt.ylabel("Número de Anomalías")
plt.xticks(rotation=45)
plt.tight_layout()
plt.savefig("top_anomaly_days.png")  # Guardar el gráfico como imagen
plt.show()

print("Gráfico generado y guardado como 'top_anomaly_days.png'.")
