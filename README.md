# Proyecto de Cloud y Big Data: Detección de Anomalías en Transacciones Criptográficas

## Resumen

Este proyecto tiene como objetivo resolver un problema de Big Data en la nube mediante la **detección de transacciones anómalas** utilizando **Apache Spark**. La solución implementa procesamiento distribuido, algoritmos de detección de anomalías (KMeans e IQR), visualización de datos y análisis del rendimiento en un clúster de Google Cloud Dataproc.

---

## Tabla de Contenidos

1. [Descripción del Problema](#descripcion-del-problema)
2. [Necesidad de Big Data y Cloud](#necesidad-de-big-data-y-cloud)
3. [Descripción de los Datos](#descripcion-de-los-datos)
4. [Descripción de la Aplicación](#descripcion-de-la-aplicacion)
5. [Diseño del Software](#diseno-del-software)
6. [Uso](#uso)
7. [Evaluación del Rendimiento](#evaluacion-del-rendimiento)
8. [Características Avanzadas](#caracteristicas-avanzadas)
9. [Conclusiones](#conclusiones)
10. [Referencias](#referencias)
11. [Capturas de Resultados](#capturas-de-resultados)

---

## 1. Descripción del Problema <a name="descripcion-del-problema"></a>

El incremento en las transacciones de criptomonedas ha generado la necesidad de identificar **operaciones atípicas** que puedan representar fraudes, errores o comportamientos inusuales.

Este proyecto resuelve el problema utilizando **procesamiento distribuido** con Apache Spark para manejar grandes volúmenes de datos (Big Data).

---

## 2. Necesidad de Big Data y Cloud <a name="necesidad-de-big-data-y-cloud"></a>

- **Big Data**: Los datos analizados consisten en millones de transacciones (más de 1 GB). Se requiere el uso de herramientas distribuidas como Apache Spark.
- **Cloud**: Google Cloud Dataproc proporciona recursos escalables y eficientes para ejecutar análisis de datos de alto volumen.

---

## 3. Descripción de los Datos <a name="descripcion-de-los-datos"></a>

- **Origen**: Datos de transacciones en cadena de bloques.
- **Formato**: CSV con las siguientes columnas:
   - `block_number`: Número del bloque.
   - `transaction_hash`: Hash único de la transacción.
   - `from_address` y `to_address`: Dirección de origen y destino.
   - `value`: Valor de la transacción.
   - `block_timestamp`: Marca temporal de la transacción.

- **Tamaño**: 1 GB+  /// 

---

## 4. Descripción de la Aplicación <a name="descripcion-de-la-aplicacion"></a>

### Componentes Principales:
1. **Validación de Datos**: Verifica integridad y estructura.
2. **Detección de Anomalías**:
   - **KMeans Clustering**: Algoritmo para identificar grupos de transacciones fuera de lo común.
   - **IQR (Interquartile Range)**: Análisis estadístico para detectar valores atípicos.
3. **Visualización de Resultados**:
   - Distribuciones de valores normales y anómalos.
   - Evolución temporal de anomalías.
4. **Procesamiento Escalable**: Apache Spark en Dataproc.

---

## 5. Diseño del Software <a name="diseno-del-software"></a>

- **Lenguaje**: Python
- **Frameworks**:
   - Apache Spark
   - Pandas
   - Matplotlib
- **Infraestructura**: Google Cloud Platform (GCP) - Dataproc
- **Dependencias**:
   ```bash
   pip install pyspark pandas matplotlib numpy

---

## 6. Uso <a name="uso"></a>
- ** Ejecución de Scripts en Dataproc ** 
	- validate_large_csv.py verifica que el archivo de datos sea válido y que tenga la estructura esperada
- ** Detección de Anomalías ** 
	- KMeans Clustering: Agrupa los datos y detecta puntos fuera de los clústeres. kmeans_anomalies.py
	- IQR (Interquartile Range): Calcula y filtra los valores atípicos estadísticamente. iqr_anomalies.py
- ** Visualización de Resultados **
	- visualize_distributions.py genera histogramas que muestran la distribución de valores tanto normales como anómalos
- ** Análisis Temporal de Anomalías ** 
	- analyze_temporal_anomalies.py visualiza la evolución de las anomalías a lo largo del tiempo.
- ** Top Días con Mayor Cantidad de Anomalías ** 
	- top_anomaly_days.py Identifica y muestra los días con más anomalías detectadas

---

## 7. Evaluación del Rendimiento <a name="evaluacion-del-rendimiento"></a>
El tiempo de procesamiento se evaluó con diferentes configuraciones de clúster:
| Número de Nodos | Tiempo de Ejecución |
|-----------------|----------------------|
| 2               | 15 minutos          |
| 4               | 9 minutos           |

---

## 8. Características Avanzadas <a name="caracteristicas-avanzadas"></a>
Combinación de Técnicas: Uso de clustering (KMeans) y análisis estadístico (IQR).
Optimización de Procesamiento: Distribución de tareas en Spark.
Visualización Completa: Histogramas, gráficos temporales y comparaciones.

---

## 9. Conclusiones <a name="conclusiones"></a>
Se logró identificar transacciones anómalas de manera efectiva.
Los resultados se visualizaron de forma clara para identificar patrones y tendencias.
El procesamiento en Google Cloud Dataproc fue eficiente y permitió el manejo de grandes volúmenes de datos.

---

## 10. Referencias <a name="referencias"></a>
Apache Spark Documentation
Google Cloud Dataproc
Pandas Documentation
Matplotlib Documentation

---

## 11. Capturas de Resultados <a name="capturas-de-resultados"></a>
Se encuentran en la carpeta de ejemplos, con su debida imagen en PNG

## Autores
- **Adrian Dumitru Chitic**

## Licencia
Este proyecto está bajo la licencia MIT. Consulta el archivo `LICENSE` para más información.
