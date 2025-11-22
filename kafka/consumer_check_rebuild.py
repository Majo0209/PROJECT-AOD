import json
import os
import pandas as pd
from kafka import KafkaConsumer

import great_expectations as gx
from great_expectations.core.batch import RuntimeBatchRequest
from great_expectations.data_context import BaseDataContext
from sqlalchemy import create_engine

INPUT_TOPIC = "check"

print(f"[QC-Consumer] Esperando mensajes en '{INPUT_TOPIC}'...")

consumer = KafkaConsumer(
    INPUT_TOPIC,
    bootstrap_servers=["localhost:9092"],
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    auto_offset_reset="earliest",
    enable_auto_commit=False,
    group_id="qc_consumer_group"
)

MYSQL_HOST = "localhost"
MYSQL_PORT = 3306
MYSQL_USER = "root"
MYSQL_PASSWORD = "root"
MYSQL_DB = "aeronet_dw"

MYSQL_TABLES = {
    "fact": "fact_aeronet",
    "dim_date": "dim_date",
    "dim_wavelength": "dim_wavelength",
    "dim_site": "dim_site",
    "dim_weather": "dim_weather"
}

MYSQL_ENGINE = create_engine(
    f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB}?charset=utf8mb4"
)

GLOBAL_QC_THRESHOLD = 0.85

ge_config = {
    "config_version": 3,
    "datasources": {},
    "stores": {
        "expectations_store": {
            "class_name": "ExpectationsStore",
            "store_backend": {"class_name": "InMemoryStoreBackend"},
        },
        "validations_store": {
            "class_name": "ValidationsStore",
            "store_backend": {"class_name": "InMemoryStoreBackend"},
        },
        "evaluation_parameter_store": {
            "class_name": "EvaluationParameterStore"
        }
    },
    "expectations_store_name": "expectations_store",
    "validations_store_name": "validations_store",
    "evaluation_parameter_store_name": "evaluation_parameter_store",
    "data_docs_sites": {},
    "anonymous_usage_statistics": {"enabled": False},
}

ge_context = BaseDataContext(project_config=ge_config)

datasource_config = {
    "name": "pandas_runtime",
    "class_name": "Datasource",
    "execution_engine": {"class_name": "PandasExecutionEngine"},
    "data_connectors": {
        "runtime_connector": {
            "class_name": "RuntimeDataConnector",
            "batch_identifiers": ["run_id"]
        }
    }
}

ge_context.add_datasource(**datasource_config)

def run_qc(df, suite_name, asset_name, expectations_fn):
    if df is None or df.empty:
        print(f"{suite_name} DF vacío. Aprobado.")
        return 1.0

    try:
        suite = ge_context.get_expectation_suite(suite_name)
    except:
        suite = ge_context.add_expectation_suite(suite_name)

    batch_request = RuntimeBatchRequest(
        datasource_name="pandas_runtime",
        data_connector_name="runtime_connector",
        data_asset_name=asset_name,
        runtime_parameters={"batch_data": df},
        batch_identifiers={"run_id": suite_name},
    )

    validator = ge_context.get_validator(
        batch_request=batch_request,
        expectation_suite_name=suite_name
    )

    expectations_fn(validator, df)

    validator.save_expectation_suite()

    results = validator.validate()
    res_list = results["results"]

    passed = sum(r["success"] for r in res_list)
    total = len(res_list)
    ratio = passed / total if total > 0 else 0

    print(f"{suite_name} {passed}/{total} {ratio*100:.2f}%")

    return ratio

def qc_dim_date(validator, df):
    for col in df.columns:
        validator.expect_column_values_to_not_be_null(col)

    validator.expect_column_values_to_be_unique("id_date")
    validator.expect_column_values_to_be_between("Year", 1993, 2025)
    validator.expect_column_values_to_be_between("Month", 1, 12)
    validator.expect_column_values_to_be_between("Day", 1, 31)
    validator.expect_column_values_to_be_between("Day_of_Year", 1, 366)
    validator.expect_column_values_to_match_regex("Date", r"^\d{4}-\d{2}-\d{2}$")

    if "Time" in df.columns:
        validator.expect_column_values_to_match_regex("Time", r"^\d{2}:\d{2}:\d{2}$")

def qc_dim_site(validator, df):
    for col in ["AERONET_Site", "Latitude", "Longitude", "Elevation", "id_site"]:
        validator.expect_column_values_to_not_be_null(col)

    validator.expect_column_values_to_not_be_null("Country", mostly=0.85)
    validator.expect_column_values_to_not_be_null("Continent", mostly=0.85)

    validator.expect_column_values_to_be_unique("id_site")
    validator.expect_column_values_to_be_between("Latitude", -90, 90)
    validator.expect_column_values_to_be_between("Longitude", -180, 180)
    validator.expect_column_values_to_be_between("Elevation", -400, 9000)

    validator.expect_column_values_to_not_match_regex("AERONET_Site", r"^\s*$")
    validator.expect_column_values_to_not_match_regex("Country", r"^\s*$", mostly=0.85)
    validator.expect_column_values_to_not_match_regex("Continent", r"^\s*$", mostly=0.85)

def qc_dim_wavelength(validator, df):
    for col in df.columns:
        validator.expect_column_values_to_not_be_null(col)

    validator.expect_column_values_to_be_unique("id_wavelength")
    validator.expect_column_values_to_be_between("Wavelength_nm", 300, 2000)

    validator.expect_column_values_to_be_in_set("Spectral_Band", ["UV", "VIS", "NIR"])

    validator.expect_column_values_to_be_in_set(
        "Sensitive_Aerosol",
        ["fine-sensitive", "balanced", "coarse-sensitive"]
    )

def qc_dim_weather(validator, df):
    for col in df.columns:
        validator.expect_column_values_to_not_be_null(col)

    numeric_cols = [
        "temperature_2m_mean",
        "shortwave_radiation_sum",
        "relative_humidity_2m_mean",
        "wind_speed_10m_max",
        "wind_direction_10m_dominant",
        "et0_fao_evapotranspiration",
        "sunshine_duration"
    ]

    for col in numeric_cols:
        validator.expect_column_values_to_be_between(col, -500, 200000)

def qc_fact(validator, df):
    validator.expect_column_values_to_not_be_null("Fact_ID")
    validator.expect_column_values_to_be_unique("Fact_ID")

    for col in ["id_date", "id_wavelength", "id_site", "id_weather"]:
        validator.expect_column_values_to_not_be_null(col)

    validator.expect_column_values_to_be_in_set("Particle_type", ["fine", "coarse"])
    validator.expect_column_values_to_be_between("AOD_Value", 0, None)

    validator.expect_column_values_to_not_be_null("Precipitable_Water", mostly=0.99)
    validator.expect_column_values_to_be_between("Precipitable_Water", 0, None, mostly=0.99)

    validator.expect_column_values_to_be_between("Angstrom_Exponent", 0, None)

    validator.expect_column_values_to_be_in_set("is_enriched", [0, 1])

def validate_batch(fact_df, dim_date_df, dim_site_df, dim_wav_df, dim_weather_df):
    ratios = {}

    ratios["dim_date"] = run_qc(dim_date_df, "dim_date_suite", "dim_date", qc_dim_date)
    ratios["dim_site"] = run_qc(dim_site_df, "dim_site_suite", "dim_site", qc_dim_site)
    ratios["dim_wavelength"] = run_qc(dim_wav_df, "dim_wavelength_suite", "dim_wavelength", qc_dim_wavelength)
    ratios["dim_weather"] = run_qc(dim_weather_df, "dim_weather_suite", "dim_weather", qc_dim_weather)
    ratios["fact"] = run_qc(fact_df, "fact_suite", "fact", qc_fact)

    print("RESULTADO GLOBAL DEL LOTE")
    for k, v in ratios.items():
        print(f"{k}: {v*100:.2f}%")

    global_ratio = sum(ratios.values()) / len(ratios)
    print(f"GLOBAL: {global_ratio*100:.2f}%")

    return global_ratio, ratios

def send_batch_to_mysql(fact_df, dim_date_df, dim_site_df, dim_wav_df, dim_weather_df):
    tables_dfs = {
        "fact": fact_df.drop_duplicates() if fact_df is not None else None,
        "dim_date": dim_date_df.drop_duplicates() if dim_date_df is not None else None,
        "dim_wavelength": dim_wav_df.drop_duplicates() if dim_wav_df is not None else None,
        "dim_site": dim_site_df.drop_duplicates() if dim_site_df is not None else None,
        "dim_weather": dim_weather_df.drop_duplicates() if dim_weather_df is not None else None,
    }

    for key, df in tables_dfs.items():
        if df is not None and not df.empty:
            table_name = MYSQL_TABLES[key]
            print(f"MySQL enviando {len(df)} filas a '{table_name}'...")
            df.to_sql(table_name, con=MYSQL_ENGINE, if_exists="append", index=False)
        else:
            print(f"MySQL tabla {key} sin filas.")

for msg in consumer:
    payload = msg.value
    msg_type = payload.get("type")

    if msg_type in ("end", "sentinel", "end_of_stream"):
        print("Fin de stream recibido. Terminando consumo...")
        break

    if msg_type != "enriched_batch":
        continue

    print("Lote enriquecido recibido. Validando calidad...")

    fact_df = pd.DataFrame(payload.get("fact", []))
    dim_date_df = pd.DataFrame(payload.get("dim_date", []))
    dim_wav_df = pd.DataFrame(payload.get("dim_wavelength", []))
    dim_site_df = pd.DataFrame(payload.get("dim_site", []))
    dim_weather_df = pd.DataFrame(payload.get("dim_weather", []))

    global_ratio, ratios = validate_batch(
        fact_df=fact_df,
        dim_date_df=dim_date_df,
        dim_site_df=dim_site_df,
        dim_wav_df=dim_wav_df,
        dim_weather_df=dim_weather_df,
    )

    if global_ratio >= GLOBAL_QC_THRESHOLD:
        print(f"Lote aprobado ({GLOBAL_QC_THRESHOLD*100:.0f}%). Enviando a MySQL...")
        send_batch_to_mysql(
            fact_df=fact_df,
            dim_date_df=dim_date_df,
            dim_site_df=dim_site_df,
            dim_wav_df=dim_wav_df,
            dim_weather_df=dim_weather_df,
        )
        print("Envío de lote completado.")
    else:
        print(f"Lote rechazado ({GLOBAL_QC_THRESHOLD*100:.0f}%). No se enviará a MySQL.")
