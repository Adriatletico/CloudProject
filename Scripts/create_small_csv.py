import pandas as pd

# Ruta del archivo original
input_file = "gs://horizontal-time-442413-m5/crypto-transactions.csv"

# Ruta del archivo reducido de salida
output_file = "crypto-transactions-small.csv"

# Número de filas deseadas
num_rows = 100

# Cargar un subconjunto de datos directamente desde el archivo en GCS
print("Leyendo el archivo original...")
df = pd.read_csv(input_file, nrows=10000)  # Cargar una parte para eficiencia inicial
df_small = df.sample(n=num_rows, random_state=42)  # Seleccionar aleatoriamente 100 filas

# Guardar el archivo reducido localmente
print(f"Guardando el archivo reducido con {num_rows} filas...")
df_small.to_csv(output_file, index=False)

print(f"Archivo reducido creado: {output_file}")
