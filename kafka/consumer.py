import json
import os
import re
import zipfile
import requests
import numpy as np
import pandas as pd
from kafka import KafkaConsumer, KafkaProducer

DATA_DIR = "../data"
OUTPUT_DIR = "../data"
os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(OUTPUT_DIR, exist_ok=True)

NE_URL = "https://naciscdn.org/naturalearth/50m/cultural/ne_50m_admin_0_countries.zip"
NE_BASENAME = "ne_50m_admin_0_countries"
NE_ZIP_PATH = os.path.join(DATA_DIR, NE_BASENAME + ".zip")
NE_SHP_PATH = os.path.join(DATA_DIR, NE_BASENAME + ".shp")
NE_SIDEKICKS = [".shp", ".dbf", ".shx", ".prj"]

try:
    import geopandas as gpd
    _HAS_GPD = True
except Exception:
    _HAS_GPD = False

def _ne_shapefile_present(basename: str, folder: str) -> bool:
    return all(os.path.exists(os.path.join(folder, basename + ext)) for ext in NE_SIDEKICKS)

def _ne_download_zip(url: str, out_path: str):
    headers = {"User-Agent": "Mozilla/5.0"}
    print("📥 Descargando Natural Earth...")
    with requests.get(url, headers=headers, stream=True, timeout=60) as r:
        r.raise_for_status()
        with open(out_path, "wb") as f:
            for chunk in r.iter_content(8192):
                if chunk:
                    f.write(chunk)

def _ensure_ne_countries():
    if _ne_shapefile_present(NE_BASENAME, DATA_DIR):
        print("✔ Shapefile Natural Earth ya disponible.")
        return
    if not os.path.exists(NE_ZIP_PATH):
        _ne_download_zip(NE_URL, NE_ZIP_PATH)
    print("📦 Extrayendo shapefile...")
    with zipfile.ZipFile(NE_ZIP_PATH, "r") as z:
        z.extractall(DATA_DIR)
    print("✔ Shapefile Natural Earth listo.")

AE_FINE_TH = 1.5
AE_COARSE_TH = 1.0

def _classify_particle_type(ae: float):
    if pd.isna(ae):
        return np.nan
    if ae >= AE_FINE_TH:
        return "fine"
    if ae <= AE_COARSE_TH:
        return "coarse"
    return "mixed"

def _spectral_band(wavelength_nm: float):
    if wavelength_nm < 400:
        return "UV"
    if wavelength_nm <= 700:
        return "VIS"
    return "NIR"

def _sensitive_aerosol(wavelength_nm: float):
    if wavelength_nm <= 500:
        return "fine-sensitive"
    if wavelength_nm >= 800:
        return "coarse-sensitive"
    return "balanced"

def _build_dim_site_with_country_continent(df_long: pd.DataFrame) -> pd.DataFrame:
    site_cols = [
        "AERONET_Site", "Site_Latitude(Degrees)",
        "Site_Longitude(Degrees)", "Site_Elevation(m)"
    ]
    site_cols = [c for c in site_cols if c in df_long.columns]
    dim_site = (
        df_long[site_cols]
        .drop_duplicates()
        .rename(columns={
            "Site_Latitude(Degrees)": "Latitude",
            "Site_Longitude(Degrees)": "Longitude",
            "Site_Elevation(m)": "Elevation",
        })
        .reset_index(drop=True)
    )
    dim_site["Latitude"] = pd.to_numeric(dim_site["Latitude"], errors="coerce")
    dim_site["Longitude"] = pd.to_numeric(dim_site["Longitude"], errors="coerce")
    swap = (dim_site["Latitude"].abs() > 90) & (dim_site["Longitude"].abs() <= 90)
    dim_site.loc[swap, ["Latitude", "Longitude"]] = dim_site.loc[swap, ["Longitude", "Latitude"]].values
    dim_site = dim_site[
        dim_site["Latitude"].between(-90, 90) &
        dim_site["Longitude"].between(-180, 180)
    ].reset_index(drop=True)
    try:
        if not _HAS_GPD:
            raise RuntimeError("GeoPandas no disponible")
        _ensure_ne_countries()
        g_sites = gpd.GeoDataFrame(
            dim_site,
            geometry=gpd.points_from_xy(dim_site["Longitude"], dim_site["Latitude"]),
            crs="EPSG:4326"
        )
        world = gpd.read_file(NE_SHP_PATH).to_crs("EPSG:4326")
        world = world.rename(columns={"ADMIN": "Country", "CONTINENT": "Continent"})
        world = world[["Country", "Continent", "geometry"]]
        joined = gpd.sjoin(g_sites, world, predicate="within", how="left")
        na_mask = joined["Country"].isna()
        if na_mask.any():
            joined2 = gpd.sjoin(
                g_sites.loc[na_mask], world, predicate="intersects", how="left"
            )
            joined.loc[na_mask, ["Country", "Continent"]] = joined2[["Country", "Continent"]].values
        dim_site["Country"] = joined["Country"].values
        dim_site["Continent"] = joined["Continent"].values
    except Exception as e:
        print("⚠ Enriquecimiento geográfico omitido:", e)
        dim_site["Country"] = np.nan
        dim_site["Continent"] = np.nan
    return dim_site[[
        "AERONET_Site", "Latitude", "Longitude",
        "Elevation", "Country", "Continent"
    ]]

def transform_aerosoles(df: pd.DataFrame):
    df = df.copy()
    df.replace([-999, -999.0, "-999", "-999.0"], np.nan, inplace=True)
    if "Date(dd:mm:yyyy)" in df.columns:
        df["Date"] = pd.to_datetime(
            df["Date(dd:mm:yyyy)"], format="%d:%m:%Y", errors="coerce"
        )
    else:
        df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
    numeric_cols = [
        "Precipitable_Water(cm)", "440-870_Angstrom_Exponent",
        "Site_Latitude(Degrees)", "Site_Longitude(Degrees)",
        "Site_Elevation(m)"
    ] + [c for c in df.columns if re.fullmatch(r"AOD_\d+nm", c)]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    ae_col = "440-870_Angstrom_Exponent"
    df["Particle_type"] = df[ae_col].apply(_classify_particle_type)
    aod_cols = [c for c in df.columns if re.fullmatch(r"AOD_\d+nm", c)]
    id_vars = [
        "AERONET_Site", "Date", "Time(hh:mm:ss)", "Day_of_Year",
        "Precipitable_Water(cm)", ae_col,
        "Particle_type", "Site_Latitude(Degrees)",
        "Site_Longitude(Degrees)", "Site_Elevation(m)"
    ]
    id_vars = [c for c in id_vars if c in df.columns]
    df_long = df.melt(
        id_vars=id_vars,
        value_vars=aod_cols,
        var_name="AOD_band",
        value_name="AOD_Value"
    )
    df_long["Wavelength_nm"] = df_long["AOD_band"].str.extract(
        r"AOD_(\d+)nm"
    ).astype(float)
    df_long["Spectral_Band"] = df_long["Wavelength_nm"].apply(_spectral_band)
    df_long["Sensitive_Aerosol"] = df_long["Wavelength_nm"].apply(_sensitive_aerosol)
    df_long["AOD_Value"] = pd.to_numeric(df_long["AOD_Value"], errors="coerce").clip(lower=0)
    df_long = df_long.dropna(subset=["AOD_Value", "Wavelength_nm"]).reset_index(drop=True)
    dim_wavelength = (
        df_long[["Wavelength_nm", "Spectral_Band", "Sensitive_Aerosol"]]
        .drop_duplicates()
        .sort_values("Wavelength_nm")
        .reset_index(drop=True)
    )
    dim_date = (
        df_long[["Date"]]
        .drop_duplicates()
        .sort_values("Date")
        .reset_index(drop=True)
    )
    dim_date["Year"] = dim_date["Date"].dt.year
    dim_date["Month"] = dim_date["Date"].dt.month
    dim_date["Day"] = dim_date["Date"].dt.day
    dim_date["Day_of_Year"] = dim_date["Date"].dt.dayofyear
    dim_site = _build_dim_site_with_country_continent(df_long)
    return df_long, dim_wavelength, dim_date, dim_site

consumer = KafkaConsumer(
    "general_input",
    bootstrap_servers=["localhost:9092"],
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="general_input_group"
)

producer = KafkaProducer(
    bootstrap_servers=["localhost:9092"],
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

print("🔄 Esperando primer mensaje en 'general_input'...")

received_first_message = False

global_dim_site = pd.DataFrame(
    columns=["id_site", "AERONET_Site", "Latitude", "Longitude",
             "Elevation", "Country", "Continent"]
)
global_dim_date = pd.DataFrame(
    columns=["id_date", "Date", "Year", "Month",
             "Day", "Day_of_Year", "Date_key"]
)
global_dim_wav = pd.DataFrame(
    columns=["id_wavelength", "Wavelength_nm",
             "Spectral_Band", "Sensitive_Aerosol"]
)

next_site_id = 1
next_date_id = 1
next_wavelength_id = 1
next_fact_id = 1

for msg in consumer:
    payload = msg.value
    msg_type = payload.get("type")

    if not received_first_message:
        received_first_message = True
        print("📨 Primer mensaje recibido → preparando Natural Earth...")
        _ensure_ne_countries()
        print("✔ Sistema listo. Procesando lotes...")

    if msg_type == "data":
        batch = payload["rows"]
        print(f"📥 Recibido lote sucio con {len(batch)} filas")
        if not batch:
            print("⚠ Lote vacío, se omite.")
            continue
        df_batch = pd.DataFrame(batch)
        df_long, dim_wav_batch, dim_date_batch, dim_site_batch = transform_aerosoles(df_batch)

        if global_dim_wav.empty:
            dim_wav_batch = dim_wav_batch.sort_values("Wavelength_nm").reset_index(drop=True)
            dim_wav_batch["id_wavelength"] = np.arange(
                next_wavelength_id, next_wavelength_id + len(dim_wav_batch)
            )
            next_wavelength_id += len(dim_wav_batch)
            global_dim_wav = dim_wav_batch.copy()
        else:
            existing_wav = set(global_dim_wav["Wavelength_nm"].tolist())
            new_wav = dim_wav_batch[~dim_wav_batch["Wavelength_nm"].isin(existing_wav)].copy()
            if not new_wav.empty:
                new_wav = new_wav.sort_values("Wavelength_nm").reset_index(drop=True)
                new_wav["id_wavelength"] = np.arange(
                    next_wavelength_id, next_wavelength_id + len(new_wav)
                )
                next_wavelength_id += len(new_wav)
                global_dim_wav = pd.concat([global_dim_wav, new_wav], ignore_index=True)

        df_long = df_long.merge(
            global_dim_wav[["id_wavelength", "Wavelength_nm"]],
            on="Wavelength_nm",
            how="left"
        )

        dim_date_batch = dim_date_batch.copy()
        dim_date_batch["Date_key"] = dim_date_batch["Date"].dt.date

        if global_dim_date.empty:
            dim_date_batch = dim_date_batch.sort_values("Date").reset_index(drop=True)
            dim_date_batch["id_date"] = np.arange(
                next_date_id, next_date_id + len(dim_date_batch)
            )
            next_date_id += len(dim_date_batch)
            global_dim_date = dim_date_batch.copy()
        else:
            existing_keys = set(global_dim_date["Date_key"].tolist())
            new_dates = dim_date_batch[~dim_date_batch["Date_key"].isin(existing_keys)].copy()
            if not new_dates.empty:
                new_dates = new_dates.sort_values("Date").reset_index(drop=True)
                new_dates["id_date"] = np.arange(
                    next_date_id, next_date_id + len(new_dates)
                )
                next_date_id += len(new_dates)
                global_dim_date = pd.concat([global_dim_date, new_dates], ignore_index=True)

        df_long["Date_key"] = df_long["Date"].dt.date
        df_long = df_long.merge(
            global_dim_date[["id_date", "Date_key"]],
            on="Date_key",
            how="left"
        )
        df_long = df_long.drop(columns=["Date_key"])

        if global_dim_site.empty:
            dim_site_batch = dim_site_batch.sort_values("AERONET_Site").reset_index(drop=True)
            dim_site_batch["id_site"] = np.arange(
                next_site_id, next_site_id + len(dim_site_batch)
            )
            next_site_id += len(dim_site_batch)
            global_dim_site = dim_site_batch.copy()
        else:
            existing_sites = set(global_dim_site["AERONET_Site"].tolist())
            new_sites = dim_site_batch[~dim_site_batch["AERONET_Site"].isin(existing_sites)].copy()
            if not new_sites.empty:
                new_sites = new_sites.sort_values("AERONET_Site").reset_index(drop=True)
                new_sites["id_site"] = np.arange(
                    next_site_id, next_site_id + len(new_sites)
                )
                next_site_id += len(new_sites)
                global_dim_site = pd.concat([global_dim_site, new_sites], ignore_index=True)

        df_long = df_long.merge(
            global_dim_site[["id_site", "AERONET_Site"]],
            on="AERONET_Site",
            how="left"
        )

        fact_df = df_long.copy()
        fact_df = fact_df.rename(columns={
            "Precipitable_Water(cm)": "Precipitable_Water",
            "440-870_Angstrom_Exponent": "Angstrom_Exponent",
            "Time(hh:mm:ss)": "Time"
        })

        fact_df["Fact_ID"] = np.arange(next_fact_id, next_fact_id + len(fact_df))
        next_fact_id += len(fact_df)

        fact_df = fact_df[[
            "Fact_ID", "id_date", "id_wavelength", "id_site",
            "Time", "Particle_type", "AOD_Value",
            "Precipitable_Water", "Angstrom_Exponent"
        ]]

        used_date_ids = fact_df["id_date"].dropna().unique()
        used_site_ids = fact_df["id_site"].dropna().unique()
        used_wav_ids = fact_df["id_wavelength"].dropna().unique()

        dim_date_out = global_dim_date[global_dim_date["id_date"].isin(used_date_ids)].copy()
        dim_site_out = global_dim_site[global_dim_site["id_site"].isin(used_site_ids)].copy()
        dim_wav_out = global_dim_wav[global_dim_wav["id_wavelength"].isin(used_wav_ids)].copy()

        dim_date_out["Date"] = pd.to_datetime(
            dim_date_out["Date"], errors="coerce"
        ).dt.strftime("%Y-%m-%d")
        if "Date_key" in dim_date_out.columns:
            dim_date_out = dim_date_out.drop(columns=["Date_key"])

        clean_payload = {
            "type": "clean_batch",
            "fact": fact_df.to_dict(orient="records"),
            "dim_wavelength": dim_wav_out.to_dict(orient="records"),
            "dim_date": dim_date_out.to_dict(orient="records"),
            "dim_site": dim_site_out.to_dict(orient="records")
        }

        producer.send("general_output", clean_payload)
        print(f"📤 Lote limpio enviado con {len(fact_df)} hechos.")

        fine = fact_df[fact_df["Particle_type"] == "fine"]

        if not fine.empty:
            join = fine.merge(dim_date_out, on="id_date", how="left")
            join = join.merge(dim_site_out, on="id_site", how="left")
            join = join.merge(dim_wav_out, on="id_wavelength", how="left")
            fine_payload = {
                "type": "fine_batch",
                "rows": join.to_dict(orient="records")
            }
            producer.send("fine_particles", fine_payload)
            print(f"✨ Enviado {len(join)} partículas finas.")
        else:
            print("ℹ No hay partículas finas en este lote.")

    elif msg_type == "sentinel":
        print("⛔ Sentinela recibido → finalizando procesamiento")
        producer.send("general_output", {"type": "sentinel"})
        producer.send("fine_particles", {"type": "sentinel"})
        producer.flush()
        break

    else:
        print("⚠ Tipo de mensaje desconocido. Se ignora.")
        continue

print("🎉 Consumer finalizado correctamente.")
