from typing import Optional
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.exceptions import AirflowFailException
from datetime import datetime, timedelta
from uuid import uuid4
import os
import logging
import polars as pl
import mysql.connector as mysql


def get_conf(name: str, default: Optional[str] = None) -> Optional[str]:
    v = os.getenv(name)
    if v is not None:
        return v
    try:
        return Variable.get(name)
    except Exception:
        return default


def make_stage_path(staging_dir: str, prefix: str, ext: str = "parquet") -> str:
    os.makedirs(staging_dir, exist_ok=True)
    return os.path.join(staging_dir, f"{prefix}_{uuid4().hex}.{ext}")


def get_mysql_conn():
    host = get_conf("MYSQL_HOST", "localhost")
    port = int(get_conf("MYSQL_PORT", "3306"))
    user = get_conf("MYSQL_USER", "root")
    pwd = get_conf("MYSQL_PASSWORD", "root")
    db = get_conf("MYSQL_DB", "aerosol_dw")

    return mysql.connect(
        host=host,
        port=port,
        user=user,
        password=pwd,
        database=db,
        autocommit=False,
    )


# =====================================================================
#                           DAG COMPLETO
# =====================================================================

@dag(
    dag_id="etl_fine_particles_meteo_enrichment",
    start_date=datetime(2025, 11, 1),
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    concurrency=1,
    dagrun_timeout=timedelta(hours=2),
    tags=["aerosol", "fine_particles", "meteo", "enrichment"]
)
def etl_fine_particles_meteo():

    DATA_DIR = get_conf("DATA_DIR", "/opt/airflow/data")
    FP_CSV = get_conf("FINE_PARTICLES_CSV", "fine_particles.csv")

    CSV_PATH = FP_CSV if os.path.isabs(
        FP_CSV) else os.path.join(DATA_DIR, FP_CSV)
    STAGING_DIR = get_conf("STAGING_DIR", "/opt/airflow/data/staging")

    # ============================================================
    # 1) EXTRACT
    # ============================================================

    @task(task_id="extract_fine_particles", retries=2, retry_delay=timedelta(minutes=2),
          execution_timeout=timedelta(minutes=15))
    def t_extract_fine_particles(csv_path: str, staging_dir: str) -> str:
        if not os.path.exists(csv_path):
            raise FileNotFoundError(
                f"[Extract] No existe el archivo: {csv_path}")

        try:
            df_pl = pl.read_csv(csv_path)
        except Exception as e:
            raise AirflowFailException(
                f"[Extract] No se pudo leer el CSV: {e}")

        expected = {"Date", "Latitude", "Longitude"}
        if not expected.issubset(set(df_pl.columns)):
            raise AirflowFailException(
                f"[Extract] El CSV debe contener columnas {expected}. "
                f"Columnas actuales: {list(df_pl.columns)}"
            )

        out = make_stage_path(staging_dir, "fine_particles_raw", "parquet")
        df_pl.write_parquet(out)
        logging.info(f"[Extract] OK. Parquet crudo guardado en: {out}")
        return out

    # ============================================================
    # 2) ENRICH METEO
    # ============================================================

    @task(task_id="enrich_with_meteo", retries=2, retry_delay=timedelta(minutes=5),
          execution_timeout=timedelta(minutes=60))
    def t_enrich_with_meteo(raw_parquet_path: str, staging_dir: str) -> str:
        import pandas as pd
        import openmeteo_requests
        import requests_cache
        from retry_requests import retry
        import time

        if not os.path.exists(raw_parquet_path):
            raise AirflowFailException(
                f"[Meteo] No existe el parquet de entrada: {raw_parquet_path}")

        df = pl.read_parquet(raw_parquet_path)

        base = df.select(["Date", "Latitude", "Longitude"]).unique()
        base_pd = base.to_pandas()

        cache_dir = get_conf("CACHE_DIR", os.path.join(
            os.path.expanduser("~"), ".cache", "openmeteo"))
        os.makedirs(cache_dir, exist_ok=True)
        cache_path = os.path.join(cache_dir, "http_cache")

        cache_session = requests_cache.CachedSession(
            cache_path, expire_after=-1)
        retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
        openmeteo = openmeteo_requests.Client(session=retry_session)

        variables = [
            "temperature_2m_mean", "shortwave_radiation_sum",
            "relative_humidity_2m_mean", "wind_speed_10m_max",
            "wind_direction_10m_dominant", "et0_fao_evapotranspiration",
            "sunshine_duration"
        ]

        url = "https://archive-api.open-meteo.com/v1/archive"

        resultados = []
        for idx, row in base_pd.iterrows():
            try:
                lat = float(row["Latitude"])
                lon = float(row["Longitude"])
                fecha = pd.to_datetime(row["Date"]).strftime("%Y-%m-%d")

                params = {
                    "latitude": lat, "longitude": lon,
                    "start_date": fecha, "end_date": fecha,
                    "daily": variables
                }

                responses = openmeteo.weather_api(url, params=params)
                if not responses:
                    raise RuntimeError("Respuesta vacía de Open-Meteo")

                response = responses[0]
                daily = response.Daily()

                data = {
                    "Date": fecha,
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
                resultados.append(data)

                print(
                    f"[Meteo] ({idx+1}/{len(base_pd)}) {lat}, {lon}, {fecha}")
                time.sleep(1)

            except Exception as e:
                print(f"[Meteo] Error fila {idx}: {e}")

        if not resultados:
            raise AirflowFailException(
                "[Meteo] No se obtuvieron datos de la API.")

        dfw = pl.from_pandas(pd.DataFrame(resultados))

        dfw = dfw.with_columns([
            pl.col("Date").str.strptime(pl.Date, strict=False),
            pl.col("Latitude").cast(pl.Float64),
            pl.col("Longitude").cast(pl.Float64),
        ]).with_columns([
            pl.col("Latitude").round(6),
            pl.col("Longitude").round(6),
        ])

        out = make_stage_path(staging_dir, "meteo_daily", "parquet")
        dfw.write_parquet(out)

        logging.info(f"[Meteo] Parquet guardado: {out}")
        return out

    # ============================================================
    # 3) MERGE
    # ============================================================

    @task(task_id="merge_fine_particles_with_meteo", retries=1,
          retry_delay=timedelta(minutes=2),
          execution_timeout=timedelta(minutes=20))
    def t_merge(raw_parquet_path: str, meteo_parquet_path: str, staging_dir: str) -> str:

        if not os.path.exists(raw_parquet_path):
            raise AirflowFailException(
                f"[Merge] No existe raw parquet: {raw_parquet_path}")
        if not os.path.exists(meteo_parquet_path):
            raise AirflowFailException(
                f"[Merge] No existe meteo parquet: {meteo_parquet_path}")

        df = pl.read_parquet(raw_parquet_path)
        dfw = pl.read_parquet(meteo_parquet_path)

        df = df.with_columns([
            pl.col("Date").str.strptime(pl.Date, strict=False)
            if df.schema["Date"] == pl.Utf8 else pl.col("Date"),
            pl.col("Latitude").cast(pl.Float64),
            pl.col("Longitude").cast(pl.Float64),
        ]).with_columns([
            pl.col("Latitude").round(6),
            pl.col("Longitude").round(6),
        ])

        df_enriched = df.join(
            dfw, on=["Date", "Latitude", "Longitude"], how="left")

        out = make_stage_path(
            staging_dir, "fine_particles_enriched", "parquet")
        df_enriched.write_parquet(out)

        csv_out = out.replace(".parquet", ".csv")
        df_enriched.write_csv(csv_out)

        logging.info(f"[Merge] Dataset enriquecido en: {out}")
        return out

    # ============================================================
    # 4) QUALITY CHECK (NEW)
    # ============================================================

    @task(
        task_id="quality_check",
        retries=0,
        execution_timeout=timedelta(minutes=15)
    )
    def t_quality_check(enriched_parquet_path: str):

        import pandas as pd
        import great_expectations as gx
        from great_expectations.data_context import BaseDataContext
        from great_expectations.core.batch import RuntimeBatchRequest

        if not os.path.exists(enriched_parquet_path):
            raise AirflowFailException(
                f"[QualityCheck] No existe el parquet enriquecido: {enriched_parquet_path}"
            )

        df = pd.read_parquet(enriched_parquet_path)

        # 1. Contexto GE
        config = {
            "config_version": 3,
            "datasources": {},
            "stores": {
                "expectations_store": {
                    "class_name": "ExpectationsStore",
                    "store_backend": {"class_name": "InMemoryStoreBackend"}
                },
                "validations_store": {
                    "class_name": "ValidationsStore",
                    "store_backend": {"class_name": "InMemoryStoreBackend"}
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

        context = BaseDataContext(project_config=config)

        # 2. Datasource
        datasource_config = {
            "name": "pandas_runtime",
            "class_name": "Datasource",
            "execution_engine": {"class_name": "PandasExecutionEngine"},
            "data_connectors": {
                "runtime_connector": {
                    "class_name": "RuntimeDataConnector",
                    "batch_identifiers": ["run_id"],
                }
            },
        }
        context.add_datasource(**datasource_config)

        suite = context.add_expectation_suite("aerosol_quality_suite")

        batch_request = RuntimeBatchRequest(
            datasource_name="pandas_runtime",
            data_connector_name="runtime_connector",
            data_asset_name="aerosol_asset",
            runtime_parameters={"batch_data": df},
            batch_identifiers={"run_id": "run_001"},
        )

        validator = context.get_validator(
            batch_request=batch_request,
            expectation_suite_name="aerosol_quality_suite"
        )

        # -----------------------------
        # EXPECTATIONS
        # -----------------------------
        validator.expect_column_values_to_not_be_null("Fact_ID")
        validator.expect_column_values_to_be_unique("Fact_ID")

        for col in ["id_date", "id_wavelength", "id_site"]:
            validator.expect_column_values_to_not_be_null(col)

        validator.expect_column_values_to_not_be_null("Date")
        validator.expect_column_values_to_be_between("Year", 1993, 2025)
        validator.expect_column_values_to_be_between("Month", 1, 12)
        validator.expect_column_values_to_be_between("Day", 1, 31)
        validator.expect_column_values_to_be_between("Day_of_Year", 1, 366)

        num_cols = [
            "AOD_Value", "Precipitable_Water", "Angstrom_Exponent",
            "Latitude", "Longitude", "Elevation", "Wavelength_nm",
            "temperature_2m_mean", "shortwave_radiation_sum",
            "relative_humidity_2m_mean", "wind_speed_10m_max",
            "wind_direction_10m_dominant", "et0_fao_evapotranspiration",
            "sunshine_duration"
        ]
        for col in num_cols:
            validator.expect_column_values_to_not_be_null(col)

        cat_cols = [
            "Particle_type", "AERONET_Site", "Country",
            "Continent", "Spectral_Band", "Sensitive_Aerosol"
        ]
        for col in cat_cols:
            validator.expect_column_values_to_not_be_null(col)

        validator.save_expectation_suite()

        # -----------------------------
        # VALIDATION
        # -----------------------------
        results = validator.validate()

        passed = sum(r["success"] for r in results["results"])
        total = len(results["results"])
        ratio = passed / total

        print(f"[QualityCheck] Aprobadas: {passed}/{total}")
        print(f"[QualityCheck] Porcentaje: {ratio*100:.2f}%")

        if ratio < 0.90:
            raise AirflowFailException(
                f"[QualityCheck] FAILED: {ratio*100:.2f}% (<90%) — Deteniendo DAG"
            )

        print("[QualityCheck] PASSED: >= 90%")

        return "OK"

    # ============================================================
    # 5) LOAD DIMENSION
    # ============================================================

    @task(
        task_id="load_dim_estatic",
        retries=1,
        retry_delay=timedelta(minutes=5),
        execution_timeout=timedelta(minutes=20)
    )
    def t_load_dim_estatic(meteo_parquet_path: str) -> str:
        if not os.path.exists(meteo_parquet_path):
            raise AirflowFailException(
                f"[LoadDim] No existe meteo parquet: {meteo_parquet_path}")

        dfw = pl.read_parquet(meteo_parquet_path)

        cols_dim = [
            "Date", "Latitude", "Longitude",
            "temperature_2m_mean", "shortwave_radiation_sum",
            "relative_humidity_2m_mean", "wind_speed_10m_max",
            "wind_direction_10m_dominant", "et0_fao_evapotranspiration",
            "sunshine_duration",
        ]
        cols_dim = [c for c in cols_dim if c in dfw.columns]

        dim_estatic = (
            dfw.select(cols_dim)
               .unique()
               .sort(["Date", "Latitude", "Longitude"])
               .with_row_count(name="id_estatic", offset=1)
        )

        conn = get_mysql_conn()
        cur = conn.cursor()
        try:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS dim_estatic (
                    id_estatic INT PRIMARY KEY,
                    Date DATE NOT NULL,
                    Latitude DOUBLE,
                    Longitude DOUBLE,
                    temperature_2m_mean DOUBLE NULL,
                    shortwave_radiation_sum DOUBLE NULL,
                    relative_humidity_2m_mean DOUBLE NULL,
                    wind_speed_10m_max DOUBLE NULL,
                    wind_direction_10m_dominant DOUBLE NULL,
                    et0_fao_evapotranspiration DOUBLE NULL,
                    sunshine_duration DOUBLE NULL
                ) ENGINE=InnoDB;
            """)

            cur.execute("TRUNCATE TABLE dim_estatic;")

            dim_pd = dim_estatic.to_pandas()

            sql = """
                INSERT INTO dim_estatic (
                    id_estatic,
                    Date,
                    Latitude,
                    Longitude,
                    temperature_2m_mean,
                    shortwave_radiation_sum,
                    relative_humidity_2m_mean,
                    wind_speed_10m_max,
                    wind_direction_10m_dominant,
                    et0_fao_evapotranspiration,
                    sunshine_duration
                )
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """
            rows = []
            for _, r in dim_pd.iterrows():
                rows.append((
                    int(r["id_estatic"]),
                    r["Date"].date() if hasattr(
                        r["Date"], "date") else r["Date"],
                    float(r["Latitude"]) if r["Latitude"] is not None else None,
                    float(r["Longitude"]) if r["Longitude"] is not None else None,
                    r.get("temperature_2m_mean"),
                    r.get("shortwave_radiation_sum"),
                    r.get("relative_humidity_2m_mean"),
                    r.get("wind_speed_10m_max"),
                    r.get("wind_direction_10m_dominant"),
                    r.get("et0_fao_evapotranspiration"),
                    r.get("sunshine_duration"),
                ))
            if rows:
                cur.executemany(sql, rows)
            conn.commit()
            logging.info(f"[LoadDim] Insertadas {len(rows)} filas en dim_estatic.")

            # =========================
            # NUEVA PARTE: enlazar fact_aod con dim_estatic
            # =========================

            # 1) Asegurar que fact_aod tenga columna id_estatic
            try:
                cur.execute("""
                    ALTER TABLE fact_aod
                        ADD COLUMN id_estatic INT NULL,
                        ADD INDEX idx_fact_aod_id_estatic (id_estatic);
                """)
                logging.info("[LoadDim] Columna id_estatic agregada a fact_aod.")
            except Exception as e:
                logging.warning(
                    "[LoadDim] ALTER TABLE fact_aod omitido o fallido (posible columna existente): %s", e
                )

            # 2) Actualizar fact_aod.id_estatic usando dim_date, dim_site y dim_estatic
            update_sql = """
                UPDATE fact_aod f
                JOIN dim_date d
                    ON f.id_date = d.id_date
                JOIN dim_site s
                    ON f.id_site = s.id_site
                JOIN dim_estatic e
                      ON d.Date = e.Date
                     AND ROUND(s.Latitude, 6)  = ROUND(e.Latitude, 6)
                     AND ROUND(s.Longitude, 6) = ROUND(e.Longitude, 6)
                SET f.id_estatic = e.id_estatic
                WHERE f.Particle_type = 'fine';
            """
            cur.execute(update_sql)
            conn.commit()
            logging.info(f"[LoadDim] fact_aod actualizado con id_estatic. Filas afectadas: {cur.rowcount}")

        finally:
            cur.close()
            conn.close()

        return "OK"

    # =====================================================================
    # Relación de tasks
    # =====================================================================

    raw_p = t_extract_fine_particles(CSV_PATH, STAGING_DIR)
    meteo_p = t_enrich_with_meteo(raw_p, STAGING_DIR)
    enriched_p = t_merge(raw_p, meteo_p, STAGING_DIR)
    quality_ok = t_quality_check(enriched_p)
    dim_task = t_load_dim_estatic(meteo_p)

    raw_p >> meteo_p
    [raw_p, meteo_p] >> enriched_p >> quality_ok >> dim_task


dag = etl_fine_particles_meteo()