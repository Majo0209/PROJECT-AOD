import json
import os
import time
from typing import Dict, Tuple, Optional

import numpy as np
import pandas as pd
from kafka import KafkaConsumer, KafkaProducer

import requests_cache
from retry_requests import retry
import openmeteo_requests

INPUT_TOPIC = "general_output"
ENRICHED_TOPIC = "check"

CACHE_DIR = os.path.join(os.path.expanduser("~"), ".cache", "openmeteo")
os.makedirs(CACHE_DIR, exist_ok=True)
CACHE_PATH = os.path.join(CACHE_DIR, "http_cache")

OUTPUT_LOG_PREFIX = "[EnrichConsumer]"

cache_session = requests_cache.CachedSession(CACHE_PATH, expire_after=-1)
retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
openmeteo = openmeteo_requests.Client(session=retry_session)

OPEN_METEO_URL = "https://archive-api.open-meteo.com/v1/archive"

METEO_VARIABLES = [
    "temperature_2m_mean",
    "shortwave_radiation_sum",
    "relative_humidity_2m_mean",
    "wind_speed_10m_max",
    "wind_direction_10m_dominant",
    "et0_fao_evapotranspiration",
    "sunshine_duration",
]

weather_cache: Dict[Tuple[str, float, float], Optional[int]] = {}
next_weather_id: int = 1

def fetch_weather_for(date_str: str, lat: float, lon: float) -> Optional[dict]:
    try:
        params = {
            "latitude": lat,
            "longitude": lon,
            "start_date": date_str,
            "end_date": date_str,
            "daily": METEO_VARIABLES,
        }

        responses = openmeteo.weather_api(OPEN_METEO_URL, params=params)
        if not responses:
            raise RuntimeError("Respuesta vacía de Open-Meteo")

        response = responses[0]
        daily = response.Daily()

        data = {
            "Date": date_str,
            "Latitude": lat,
            "Longitude": lon,
            "temperature_2m_mean": float(daily.Variables(0).ValuesAsNumpy()[0]),
            "shortwave_radiation_sum": float(daily.Variables(1).ValuesAsNumpy()[0]),
            "relative_humidity_2m_mean": float(daily.Variables(2).ValuesAsNumpy()[0]),
            "wind_speed_10m_max": float(daily.Variables(3).ValuesAsNumpy()[0]),
            "wind_direction_10m_dominant": float(daily.Variables(4).ValuesAsNumpy()[0]),
            "et0_fao_evapotranspiration": float(daily.Variables(5).ValuesAsNumpy()[0]),
            "sunshine_duration": float(daily.Variables(6).ValuesAsNumpy()[0]),
        }
        return data

    except Exception as e:
        print(f"{OUTPUT_LOG_PREFIX} [Meteo] Error para {lat}, {lon}, {date_str}: {e}")
        return None

consumer = KafkaConsumer(
    INPUT_TOPIC,
    bootstrap_servers=["localhost:9092"],
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    auto_offset_reset="earliest",
    enable_auto_commit=False,
    group_id="enrich_meteo_group",
)

producer = KafkaProducer(
    bootstrap_servers=["localhost:9092"],
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

print(f"{OUTPUT_LOG_PREFIX} 🔄 Esperando mensajes en topic '{INPUT_TOPIC}'...")

for msg in consumer:
    payload = msg.value
    msg_type = payload.get("type")

    if msg_type == "sentinel":
        print(f"{OUTPUT_LOG_PREFIX} ⛔ Sentinela recibido. Terminando consumo...")
        producer.send(ENRICHED_TOPIC, {"type": "sentinel"})
        producer.flush()
        break

    if msg_type != "clean_batch":
        continue

    fact_df = pd.DataFrame(payload["fact"])
    dim_date_df = pd.DataFrame(payload["dim_date"])
    dim_wav_df = pd.DataFrame(payload["dim_wavelength"])
    dim_site_df = pd.DataFrame(payload["dim_site"])

    print(
        f"{OUTPUT_LOG_PREFIX} 📥 Lote limpio recibido: "
        f"fact={len(fact_df)}, dim_date={len(dim_date_df)}, "
        f"dim_site={len(dim_site_df)}, dim_wavelength={len(dim_wav_df)}"
    )

    if fact_df.empty:
        print(f"{OUTPUT_LOG_PREFIX} ⚠ Tabla de hechos vacía, se omite lote.")
        continue

    if "Date" in dim_date_df.columns:
        dim_date_df["Date"] = dim_date_df["Date"].astype(str)

    fact_df = fact_df.reset_index(drop=True)
    fact_df["_row_id"] = fact_df.index

    merged = (
        fact_df[["_row_id", "id_site", "id_date"]]
        .merge(dim_site_df[["id_site", "Latitude", "Longitude"]], on="id_site", how="left")
        .merge(dim_date_df[["id_date", "Date"]], on="id_date", how="left")
    )

    merged["Latitude_round"] = merged["Latitude"].astype(float).round(6)
    merged["Longitude_round"] = merged["Longitude"].astype(float).round(6)

    combos_df = (
        merged[["Date", "Latitude_round", "Longitude_round"]]
        .dropna()
        .drop_duplicates()
        .reset_index(drop=True)
    )

    print(
        f"{OUTPUT_LOG_PREFIX} 🔍 Combinaciones únicas Date/Lat/Lon en este lote: "
        f"{len(combos_df)}"
    )

    new_weather_rows = []

    for idx, row in combos_df.iterrows():
        date_str = row["Date"]
        lat = float(row["Latitude_round"])
        lon = float(row["Longitude_round"])
        key = (date_str, lat, lon)

        if key in weather_cache:
            continue

        print(f"{OUTPUT_LOG_PREFIX} [Meteo] ({idx+1}/{len(combos_df)}) {lat}, {lon}, {date_str}")

        weather_data = fetch_weather_for(date_str, lat, lon)

        time.sleep(0.1)

        if weather_data is None:
            weather_cache[key] = None
            continue

        new_weather_rows.append(
            {
                "Date": weather_data["Date"],
                "Latitude": weather_data["Latitude"],
                "Longitude": weather_data["Longitude"],
                "temperature_2m_mean": weather_data["temperature_2m_mean"],
                "shortwave_radiation_sum": weather_data["shortwave_radiation_sum"],
                "relative_humidity_2m_mean": weather_data["relative_humidity_2m_mean"],
                "wind_speed_10m_max": weather_data["wind_speed_10m_max"],
                "wind_direction_10m_dominant": weather_data["wind_direction_10m_dominant"],
                "et0_fao_evapotranspiration": weather_data["et0_fao_evapotranspiration"],
                "sunshine_duration": weather_data["sunshine_duration"],
            }
        )

    dim_weather_batch = pd.DataFrame(new_weather_rows)

    if not dim_weather_batch.empty:
        dim_weather_batch = dim_weather_batch.reset_index(drop=True)
        dim_weather_batch["id_weather"] = np.arange(
            next_weather_id, next_weather_id + len(dim_weather_batch)
        )

        for _, row in dim_weather_batch.iterrows():
            key = (
                row["Date"],
                float(row["Latitude"]),
                float(row["Longitude"]),
            )
            weather_cache[key] = int(row["id_weather"])

        next_weather_id += len(dim_weather_batch)

    merged["key"] = list(
        zip(
            merged["Date"],
            merged["Latitude_round"],
            merged["Longitude_round"],
        )
    )

    def map_id_weather(k):
        return weather_cache.get(k, None)

    merged["id_weather"] = merged["key"].apply(map_id_weather)
    merged["is_enriched"] = merged["id_weather"].notna().astype(int)

    enrich_map = merged.set_index("_row_id")[["id_weather", "is_enriched"]]

    fact_df = fact_df.join(enrich_map, on="_row_id")
    fact_df.drop(columns=["_row_id"], inplace=True)

    if dim_weather_batch.empty:
        dim_weather_payload = []
    else:
        dim_weather_cols = [
            "id_weather",
            "Date",
            "Latitude",
            "Longitude",
            "temperature_2m_mean",
            "shortwave_radiation_sum",
            "relative_humidity_2m_mean",
            "wind_speed_10m_max",
            "wind_direction_10m_dominant",
            "et0_fao_evapotranspiration",
            "sunshine_duration",
        ]
        dim_weather_batch = dim_weather_batch[dim_weather_cols]
        dim_weather_payload = dim_weather_batch.to_dict(orient="records")

    enriched_payload = {
        "type": "enriched_batch",
        "fact": fact_df.to_dict(orient="records"),
        "dim_date": dim_date_df.to_dict(orient="records"),
        "dim_wavelength": dim_wav_df.to_dict(orient="records"),
        "dim_site": dim_site_df.to_dict(orient="records"),
        "dim_weather": dim_weather_payload,
    }

    producer.send(ENRICHED_TOPIC, enriched_payload)
    producer.flush()

    n_enriched = int(fact_df["is_enriched"].sum())
    total_fact = len(fact_df)
    print(
        f"{OUTPUT_LOG_PREFIX} 📤 Lote enriquecido enviado a '{ENRICHED_TOPIC}'. "
        f"Registros hechos: {total_fact}, enriquecidos: {n_enriched}, "
        f"nuevas filas dim_weather: {len(dim_weather_payload)}"
    )

print(f"{OUTPUT_LOG_PREFIX} 🎉 Proceso de consumer enriquecedor finalizado.")