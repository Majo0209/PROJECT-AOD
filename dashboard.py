import pandas as pd
import numpy as np
import streamlit as st
import plotly.express as px
from sqlalchemy import create_engine
from streamlit_autorefresh import st_autorefresh

st.set_page_config(
    page_title="AOD Streaming Dashboard",
    layout="wide",
    initial_sidebar_state="expanded"
)

st_autorefresh(interval=15 * 1000, key="data_refresh")

st.title("🌍 AERONET AOD Dashboard (Streaming desde MySQL)")
st.caption(
    "Objective: Analyze the temporal, spatial, and spectral evolution of AOD "
    "to identify differences between fine vs. coarse particles at AERONET sites. "
    "Datos llegan en tiempo (casi) real vía Kafka → MySQL."
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
    "dim_weather": "dim_weather",
}

ENGINE_URL = (
    f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}"
    f"@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB}?charset=utf8mb4"
)

MYSQL_ENGINE = create_engine(ENGINE_URL)

@st.cache_data(ttl=60, show_spinner="Cargando datos desde MySQL...")
def load_data_from_mysql():
    dim_date = pd.read_sql(f"SELECT * FROM {MYSQL_TABLES['dim_date']}", MYSQL_ENGINE)
    dim_site = pd.read_sql(f"SELECT * FROM {MYSQL_TABLES['dim_site']}", MYSQL_ENGINE)
    fact = pd.read_sql(f"SELECT * FROM {MYSQL_TABLES['fact']}", MYSQL_ENGINE)
    dim_wave = pd.read_sql(f"SELECT * FROM {MYSQL_TABLES['dim_wavelength']}", MYSQL_ENGINE)
    dim_weather = pd.read_sql(f"SELECT * FROM {MYSQL_TABLES['dim_weather']}", MYSQL_ENGINE)

    dim_date = dim_date.rename(columns={"Date": "Date_dim"})

    dim_weather = dim_weather.rename(columns={
        "Date": "Weather_Date",
        "Latitude": "Weather_Latitude",
        "Longitude": "Weather_Longitude"
    })

    df = (
        fact
        .merge(dim_date, on="id_date", how="left")
        .merge(dim_site, on="id_site", how="left")
        .merge(dim_wave, on="id_wavelength", how="left")
        .merge(dim_weather, on="id_weather", how="left")
    )

    df["Measurement_Date"] = pd.to_datetime(df["Date_dim"], errors="coerce")

    if "Time" in df.columns:
        df["Measurement_DateTime"] = pd.to_datetime(
            df["Date_dim"].astype(str) + " " + df["Time"].astype(str),
            errors="coerce"
        )
    else:
        df["Measurement_DateTime"] = df["Measurement_Date"]

    if "Year" in df.columns:
        df["Year_dim"] = df["Year"]
    if "Month" in df.columns:
        df["Month_dim"] = df["Month"]
    if "Day" in df.columns:
        df["Day_dim"] = df["Day"]

    def classify_particle(ptype):
        if isinstance(ptype, str):
            s = ptype.lower()
            if "fine" in s:
                return "Fine"
            if "coarse" in s:
                return "Coarse"
        return "Other"

    df["Particle_group"] = df["Particle_type"].apply(classify_particle)

    return df


df = load_data_from_mysql()
if df.empty:
    st.error("No se cargaron datos desde MySQL. Verifica la BD y el consumer de Kafka.")
    st.stop()


st.sidebar.title("🎛 Filtros de análisis")

with st.sidebar.expander("🕒 Dimensión temporal", expanded=True):
    min_date = df["Measurement_Date"].min()
    max_date = df["Measurement_Date"].max()

    date_range = st.date_input(
        "Rango de fechas (Measurement_Date)",
        value=(min_date, max_date)
    )

    time_agg = st.radio(
        "Nivel de agregación temporal",
        options=["Diario", "Mensual", "Anual"],
        index=1
    )

with st.sidebar.expander("🌫️ AOD & tipos de partículas", expanded=True):
    particle_groups = sorted(df["Particle_group"].dropna().unique().tolist())
    selected_particle_groups = st.multiselect(
        "Grupo de partículas (Fine / Coarse / Other)",
        options=particle_groups,
        default=particle_groups
    )

    min_aod = float(df["AOD_Value"].min())
    max_aod = float(df["AOD_Value"].max())
    aod_range = st.slider(
        "Rango de AOD_Value",
        min_value=float(np.floor(min_aod * 100) / 100),
        max_value=float(np.ceil(max_aod * 100) / 100),
        value=(min_aod, max_aod)
    )

with st.sidebar.expander("📍 Dimensión espacial", expanded=True):
    countries = sorted(df["Country"].dropna().unique().tolist())
    selected_countries = st.multiselect(
        "País",
        options=countries,
        default=countries
    )

    continents = sorted(df["Continent"].dropna().unique().tolist())
    selected_continents = st.multiselect(
        "Continente",
        options=continents,
        default=continents
    )

with st.sidebar.expander("🌈 Dimensión espectral", expanded=False):
    wavelengths = sorted(df["Wavelength_nm"].dropna().unique().tolist())
    selected_wavelengths = st.multiselect(
        "Longitud de onda (nm)",
        options=wavelengths,
        default=wavelengths
    )

with st.sidebar.expander("⚙️ Calidad / enriquecimiento", expanded=False):
    enriched_option = st.selectbox(
        "Filtrar por enriquecimiento",
        options=[
            "Todos",
            "Solo enriquecidos (is_enriched=1)",
            "Solo no enriquecidos (is_enriched=0)"
        ],
        index=0
    )

df_filtered = df.copy()

# Fecha
if isinstance(date_range, tuple) and len(date_range) == 2:
    start_date, end_date = date_range
    df_filtered = df_filtered[
        (df_filtered["Measurement_Date"] >= pd.to_datetime(start_date)) &
        (df_filtered["Measurement_Date"] <= pd.to_datetime(end_date))
    ]

# Particle_group (Fine / Coarse / Other)
if selected_particle_groups:
    df_filtered = df_filtered[df_filtered["Particle_group"].isin(selected_particle_groups)]

# Wavelength_nm
if selected_wavelengths:
    df_filtered = df_filtered[df_filtered["Wavelength_nm"].isin(selected_wavelengths)]

# Country
if selected_countries:
    df_filtered = df_filtered[df_filtered["Country"].isin(selected_countries)]

# Continent
if selected_continents:
    df_filtered = df_filtered[df_filtered["Continent"].isin(selected_continents)]

# AOD range
df_filtered = df_filtered[
    (df_filtered["AOD_Value"] >= aod_range[0]) &
    (df_filtered["AOD_Value"] <= aod_range[1])
]

# Enrichment
if enriched_option == "Solo enriquecidos (is_enriched=1)":
    df_filtered = df_filtered[df_filtered["is_enriched"] == 1]
elif enriched_option == "Solo no enriquecidos (is_enriched=0)":
    df_filtered = df_filtered[df_filtered["is_enriched"] == 0]

if df_filtered.empty:
    st.warning("No hay datos que coincidan con los filtros seleccionados.")
    st.stop()

# KPIs PRINCIPALES

st.subheader("📊 Indicadores generales (según filtros)")

col1, col2, col3, col4 = st.columns(4)

with col1:
    st.metric("Nº de observaciones", f"{len(df_filtered):,}")

with col2:
    st.metric("AOD promedio", f"{df_filtered['AOD_Value'].mean():.3f}")

with col3:
    st.metric("Angstrom (media)", f"{df_filtered['Angstrom_Exponent'].mean():.3f}")

with col4:
    st.metric("Nº de sitios", f"{df_filtered['AERONET_Site'].nunique():,}")


climate_cols = [
    "temperature_2m_mean",
    "relative_humidity_2m_mean",
    "wind_speed_10m_max",
    "shortwave_radiation_sum",
    "et0_fao_evapotranspiration",
    "sunshine_duration"
]
available_climate = [c for c in climate_cols if c in df_filtered.columns]

tab_overview, tab_map, tab_weather, tab_data = st.tabs(
    ["📈 Temporal / Espectral", "🗺️ Espacial", "🌦️ Clima & Correlaciones", "📄 Datos / Resúmenes"]
)

with tab_overview:
    st.markdown("### 1.1 Evolución temporal de AOD (Fine vs Coarse)")

    ts_df = df_filtered.dropna(subset=["Measurement_Date"])
    if not ts_df.empty:
        # Agregación temporal
        if time_agg == "Diario":
            ts_df["TimeKey"] = ts_df["Measurement_Date"].dt.date
        elif time_agg == "Mensual":
            ts_df["TimeKey"] = ts_df["Measurement_Date"].dt.to_period("M").astype(str)
        else:  # Anual
            ts_df["TimeKey"] = ts_df["Measurement_Date"].dt.year.astype(str)

        agg_ts = (
            ts_df
            .groupby(["TimeKey", "Particle_group"], as_index=False)["AOD_Value"]
            .mean()
        )

        fig_ts = px.line(
            agg_ts.sort_values("TimeKey"),
            x="TimeKey",
            y="AOD_Value",
            color="Particle_group",
            markers=False,
            title=f"AOD promedio ({time_agg}) por grupo de partículas"
        )
        fig_ts.update_layout(xaxis_title="Tiempo", yaxis_title="AOD_Value")
        st.plotly_chart(fig_ts, use_container_width=True)
    else:
        st.info("No hay fechas válidas para la serie temporal.")

    st.markdown("### 1.2 Distribución de AOD por grupo de partículas")

    hist_df = df_filtered.dropna(subset=["AOD_Value"])
    if not hist_df.empty:
        fig_hist = px.histogram(
            hist_df,
            x="AOD_Value",
            color="Particle_group",
            nbins=40,
            marginal="box",
            title="Distribución de AOD por grupo de partículas"
        )
        fig_hist.update_layout(xaxis_title="AOD_Value", yaxis_title="Frecuencia")
        st.plotly_chart(fig_hist, use_container_width=True)
    else:
        st.info("No hay datos para el histograma de AOD.")

    st.markdown("### 1.3 Relación espectral: AOD vs longitud de onda")

    col_b1, col_b2 = st.columns(2)

    # AOD vs Wavelength (line/bar)
    with col_b1:
        spec_df = df_filtered.dropna(subset=["Wavelength_nm"])
        if not spec_df.empty:
            spec_agg = (
                spec_df
                .groupby(["Wavelength_nm", "Particle_group"], as_index=False)["AOD_Value"]
                .mean()
            )
            fig_spec = px.line(
                spec_agg.sort_values("Wavelength_nm"),
                x="Wavelength_nm",
                y="AOD_Value",
                color="Particle_group",
                markers=True,
                title="AOD promedio por longitud de onda y grupo de partículas"
            )
            fig_spec.update_layout(xaxis_title="Wavelength (nm)", yaxis_title="AOD_Value")
            st.plotly_chart(fig_spec, use_container_width=True)
        else:
            st.info("No hay datos para la relación espectral AOD vs wavelength.")

    # AOD vs Angstrom (tamaño de partículas)
    with col_b2:
        ang_df = df_filtered.dropna(subset=["Angstrom_Exponent"])
        if not ang_df.empty:
            fig_ang = px.scatter(
                ang_df,
                x="Angstrom_Exponent",
                y="AOD_Value",
                color="Particle_group",
                hover_data=["AERONET_Site", "Country"],
                title="AOD vs Angstrom Exponent (por grupo de partículas)"
            )
            fig_ang.update_layout(
                xaxis_title="Angstrom Exponent (indicador de tamaño de partícula)",
                yaxis_title="AOD_Value"
            )
            st.plotly_chart(fig_ang, use_container_width=True)
        else:
            st.info("No hay datos para graficar AOD vs Angstrom.")

with tab_map:
    st.markdown("### 2.1 Mapas globales de AOD")

    # Datos agregados por sitio
    map_df = (
        df_filtered
        .dropna(subset=["Latitude", "Longitude"])
        .groupby(
            ["AERONET_Site", "Latitude", "Longitude", "Country", "Continent"],
            as_index=False
        )
        .agg(
            AOD_mean=("AOD_Value", "mean"),
            Angstrom_mean=("Angstrom_Exponent", "mean")
        )
    )

    # Datos agregados por país
    country_map = (
        df_filtered
        .dropna(subset=["Country"])
        .groupby("Country", as_index=False)["AOD_Value"]
        .mean()
    )

    col_m1, col_m2 = st.columns(2)

    # Mapa global por sitio
    with col_m1:
        if not map_df.empty:
            fig_map = px.scatter_geo(
                map_df,
                lat="Latitude",
                lon="Longitude",
                color="AOD_mean",
                hover_name="AERONET_Site",
                hover_data={
                    "Country": True,
                    "Continent": True,
                    "AOD_mean": ":.3f",
                    "Angstrom_mean": ":.3f"
                },
                size="AOD_mean",
                projection="natural earth",
                title="AOD promedio por sitio AERONET"
            )
            st.plotly_chart(fig_map, use_container_width=True)
        else:
            st.info("No hay datos suficientes (lat/long) para mostrar el mapa por sitio.")

    # Choropleth por país
    with col_m2:
        if not country_map.empty:
            fig_choro = px.choropleth(
                country_map,
                locations="Country",
                locationmode="country names",
                color="AOD_Value",
                title="AOD promedio por país",
                color_continuous_scale="Viridis"
            )
            st.plotly_chart(fig_choro, use_container_width=True)
        else:
            st.info("No hay datos para el choropleth por país.")

    st.markdown("### 2.2 Mapa AOD + variable climática seleccionada y su distribución")

    if available_climate:
        selected_clim_map = st.selectbox(
            "Selecciona variable climática para el mapa",
            available_climate,
            key="climate_map_var"
        )

        clim_df = df_filtered.dropna(subset=["Latitude", "Longitude", selected_clim_map]).copy()

        if not clim_df.empty:
            agg_map_clim = (
                clim_df
                .groupby(
                    ["AERONET_Site", "Latitude", "Longitude", "Country", "Continent"],
                    as_index=False
                )
                .agg(
                    AOD_mean=("AOD_Value", "mean"),
                    Clim_mean=(selected_clim_map, "mean")
                )
            )

            # Pasar a formato largo para tener dos puntos por sitio: AOD y clima
            long_map = agg_map_clim.melt(
                id_vars=["AERONET_Site", "Latitude", "Longitude", "Country", "Continent"],
                value_vars=["AOD_mean", "Clim_mean"],
                var_name="Variable_type",
                value_name="Value"
            )

            # Renombrar variables para la leyenda
            var_map = {
                "AOD_mean": "AOD",
                "Clim_mean": selected_clim_map
            }
            long_map["Variable"] = long_map["Variable_type"].map(var_map)

            # Pequeño desplazamiento horizontal para no superponer los puntos
            long_map["Lon_plot"] = long_map["Longitude"]
            long_map.loc[long_map["Variable"] == "AOD", "Lon_plot"] = (
                long_map["Longitude"] - 0.3
            )
            long_map.loc[long_map["Variable"] == selected_clim_map, "Lon_plot"] = (
                long_map["Longitude"] + 0.3
            )

            # Tamaño del marcador: base 8, clima escalado con mínimo positivo
            long_map["Marker_size"] = 8.0
            mask_clim = long_map["Variable"] == selected_clim_map
            clim_vals = long_map.loc[mask_clim, "Value"]
            pos_vals = clim_vals[clim_vals > 0]
            if not pos_vals.empty:
                min_size = float(pos_vals.min())
            else:
                min_size = 4.0
            long_map.loc[mask_clim, "Marker_size"] = clim_vals.clip(lower=min_size)

            col_c1, col_c2 = st.columns(2)

            # Mapa AOD + clima con dos símbolos por sitio
            with col_c1:
                fig_map_clim = px.scatter_geo(
                    long_map,
                    lat="Latitude",
                    lon="Lon_plot",
                    color="Variable",
                    symbol="Variable",
                    size="Marker_size",
                    hover_name="AERONET_Site",
                    hover_data={
                        "Country": True,
                        "Continent": True,
                        "Variable": True,
                        "Value": ":.3f"
                    },
                    projection="natural earth",
                    title=f"AOD y {selected_clim_map} promedio por sitio"
                )
                st.plotly_chart(fig_map_clim, use_container_width=True)

            # Scatter AOD vs clima (por observación)
            with col_c2:
                fig_scatter_clim = px.scatter(
                    clim_df,
                    x=selected_clim_map,
                    y="AOD_Value",
                    color="Particle_group",
                    hover_name="AERONET_Site",
                    title=f"AOD vs {selected_clim_map} (distribución por observación)"
                )
                fig_scatter_clim.update_layout(
                    xaxis_title=selected_clim_map,
                    yaxis_title="AOD_Value"
                )
                st.plotly_chart(fig_scatter_clim, use_container_width=True)
        else:
            st.info("No hay datos suficientes para el mapa AOD + variable climática.")
    else:
        st.info("No hay variables climáticas disponibles en los datos filtrados.")

    st.markdown("### 2.3 AOD por continente y grupo de partículas")

    cont_group = (
        df_filtered
        .dropna(subset=["Continent"])
        .groupby(["Continent", "Particle_group"], as_index=False)["AOD_Value"]
        .mean()
    )
    if not cont_group.empty:
        fig_cont = px.bar(
            cont_group,
            x="Continent",
            y="AOD_Value",
            color="Particle_group",
            barmode="group",
            title="AOD promedio por continente y grupo de partículas"
        )
        fig_cont.update_layout(xaxis_title="Continente", yaxis_title="AOD promedio")
        st.plotly_chart(fig_cont, use_container_width=True)
    else:
        st.info("No hay datos suficientes para el gráfico por continente.")

with tab_weather:
    st.markdown("### 3.1 Matriz de correlación (AOD + clima)")

    corr_cols = ["AOD_Value"] + available_climate
    corr_df = df_filtered[corr_cols].dropna()

    if not corr_df.empty and len(corr_cols) > 1:
        corr_matrix = corr_df.corr(numeric_only=True)
        fig_corr = px.imshow(
            corr_matrix,
            text_auto=True,
            aspect="auto",
            title="Matriz de correlación (Pearson)"
        )
        st.plotly_chart(fig_corr, use_container_width=True)
    else:
        st.info("No hay suficientes datos numéricos para calcular correlaciones.")

    st.markdown("### 3.2 Series temporales mensuales: AOD y variable climática seleccionada")

    ts_clim = df_filtered.dropna(subset=["Measurement_Date"]).copy()
    if not ts_clim.empty and available_climate:
        selected_clim_ts = st.selectbox(
            "Selecciona variable climática para la serie temporal",
            available_climate,
            key="climate_ts_var"
        )

        ts_clim["YearMonth"] = ts_clim["Measurement_Date"].dt.to_period("M").astype(str)
        agg_clim = (
            ts_clim
            .groupby("YearMonth", as_index=False)
            .agg(
                AOD_mean=("AOD_Value", "mean"),
                Clim_mean=(selected_clim_ts, "mean")
            )
        )
        agg_clim = agg_clim.sort_values("YearMonth")

        col_ts1, col_ts2 = st.columns(2)

        # Serie AOD
        with col_ts1:
            fig_ts_aod = px.line(
                agg_clim,
                x="YearMonth",
                y="AOD_mean",
                title="Evolución mensual de AOD"
            )
            fig_ts_aod.update_layout(xaxis_title="Año-Mes", yaxis_title="AOD promedio")
            st.plotly_chart(fig_ts_aod, use_container_width=True)

        # Serie variable climática seleccionada
        with col_ts2:
            fig_ts_clim = px.line(
                agg_clim,
                x="YearMonth",
                y="Clim_mean",
                title=f"Evolución mensual de {selected_clim_ts}"
            )
            fig_ts_clim.update_layout(xaxis_title="Año-Mes", yaxis_title=f"{selected_clim_ts} promedio")
            st.plotly_chart(fig_ts_clim, use_container_width=True)
    else:
        st.info("No hay datos suficientes para las series temporales AOD + clima.")

with tab_data:
    st.markdown("### 4.1 Vista de datos filtrados")
    st.dataframe(df_filtered, use_container_width=True, height=400)

    csv_export = df_filtered.to_csv(index=False).encode("utf-8")
    st.download_button(
        label="⬇️ Descargar datos filtrados (CSV)",
        data=csv_export,
        file_name="aod_filtered_data.csv",
        mime="text/csv"
    )

    st.markdown("### 4.2 Resumen por grupo de partículas y continente")

    summary_group = (
        df_filtered
        .groupby(["Particle_group", "Continent"], dropna=False, as_index=False)
        .agg(
            AOD_mean=("AOD_Value", "mean"),
            AOD_std=("AOD_Value", "std"),
            n_obs=("AOD_Value", "count")
        )
    )
    st.dataframe(summary_group, use_container_width=True)
