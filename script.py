from influxdb_client_3 import InfluxDBClient3
import polars as pl
import os
import certifi


INFLUX_HOST = os.getenv("INFLUX_HOST")
INFLUX_TOKEN = os.getenv("INFLUX_TOKEN")
INFLUX_DB = os.getenv("INFLUX_DATABASE", "raw_data")

# Configurar certificados SSL
os.environ["GRPC_DEFAULT_SSL_ROOTS_FILE_PATH"] = certifi.where()


# Configurar cliente
client = InfluxDBClient3(
    host=f"{INFLUX_HOST}",
    token=f"{INFLUX_TOKEN}",
    database=f"{INFLUX_DB}",
)

# Consulta para obtener datos de la última hora
query = """
SELECT 
    COALESCE(pm10_table.time, co_table.time, nox_table.time) AS time,
    COALESCE(pm10_table.location, co_table.location, nox_table.location) AS location,
    pm10_table.pm10_mean,
    co_table.co_mean,
    nox_table.no_mean,
    nox_table.no2_mean,
    nox_table.nox_mean
FROM pm10_table
FULL OUTER JOIN co_table 
    ON pm10_table.time = co_table.time AND pm10_table.location = co_table.location
FULL OUTER JOIN nox_table 
    ON pm10_table.time = nox_table.time AND pm10_table.location = nox_table.location
WHERE 
    pm10_table.time >= date_trunc('hour', now()) - interval '1 hour'
    AND pm10_table.time < date_trunc('hour', now())
ORDER BY time;
"""


def calcular_promedios_horarios(df: pl.DataFrame) -> pl.DataFrame:
    """
    Calcula promedios horarios, filtrando horas con <75% de datos (45 minutos).
    """
    return (
        df.group_by_dynamic("time", every="1h", group_by="location")
        .agg([
            pl.count("pm10_mean").alias("n"),  # Contar muestras (de cualquier variable)
            pl.col("pm10_mean").mean().alias("pm10"),
            pl.col("co_mean").mean().alias("co"),
            pl.col("no_mean").mean().alias("no"),
            pl.col("no2_mean").mean().alias("no2"),
            pl.col("nox_mean").mean().alias("nox"),
        ])
        .filter(pl.col("n") >= 45)  # Filtrar horas incompletas
        .drop("n")  # Eliminar columna de conteo
    )

# def enviar_a_gas(df: pl.DataFrame) -> None:
#     """Envía promedios horarios a Google Apps Script."""
#     payload = [
#         {
#             "hora": str(row["time"]),
#             "location": row["location"],
#             "pm10": row["pm10"],
#             "co": row["co"],
#             "no": row["no"],
#             "no2": row["no2"],
#             "nox": row["nox"],
#             "temp": row["temp"],
#             "hr": row["hr"],
#             "atm_press": row["atm-press"],
#             "uv": row["uv"],
#         }
#         for row in df.iter_rows(named=True)
#     ]
#     response = requests.post(GAS_ENDPOINT, json=payload)
#     response.raise_for_status()  # Lanza error si falla el POST

try:
    # Ejecutar consulta y obtener tabla Arrow desde InfluxDB
    datos_minutales = client.query(query)
    if datos_minutales is None:
        raise ValueError("No se obtuvieron datos de la consulta.")
   
    
    print(datos_minutales)
    # Convertir a DataFrame de Polars
    df = pl.from_arrow(datos_minutales)

    # # Mostrar información básica
    # print(f"DataFrame shape: {df.shape}")
    # print(f"Columnas: {df.columns}")
    # print(f"Primeros registros:\n{df.head()}")

    promedios_horarios = calcular_promedios_horarios(df)
    
    if not promedios_horarios.is_empty():
        print("Promedios horarios calculados:")
        print(promedios_horarios)
        #enviar_a_gas(promedios_horarios)
    else:
        print("No hay datos suficientes para calcular promedios horarios.")
        
except Exception as e:
    print(f"Error: {e}")
    import traceback
    traceback.print_exc()
finally:
    client.close()