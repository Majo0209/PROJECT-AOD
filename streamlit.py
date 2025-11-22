import os
from datetime import datetime

import mysql.connector as mysql
import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

# Configuración básica de la página

st.set_page_config(
    page_title="AOD – Partículas finas enriquecidas",
    layout="wide"
)

st.title("Dashboard de AOD en partículas finas con información climática")
st.markdown(
    """
Este dashboard presenta el análisis de la Aerosol Optical Depth (AOD) 
asociada a partículas finas, utilizando únicamente registros que han sido 
enriquecidos con variables climáticas diarias (temperatura, humedad relativa, 
radiación solar, velocidad del viento, evapotranspiración y duración de la insolación) 
para el periodo 2015–2024.
"""
)

# Conexión y carga de datos

def get_mysql_conn():
    return mysql.connect(
        host=os.getenv("MYSQL_HOST", "localhost"),
        port=int(os.getenv("MYSQL_PORT", "3306")),
        user=os.getenv("MYSQL_USER", "root"),
        password=os.getenv("MYSQL_PASSWORD", "root"),
        database="prueba",
        autocommit=False,
    )


@st.cache_data(ttl=600, show_spinner=True)
def load_aod_fine_enriched():
    conn = get_mysql_conn()

    query = """
        SELECT
            f.Fact_ID,
            d.Date,
            d.Year,
            d.Month,
            d.Day,
            s.AERONET_Site,
            s.Latitude,
            s.Longitude,
            f.Particle_type,
            f.AOD_Value,
            f.Angstrom_Exponent,
            f.Precipitable_Water,
            f.id_openmeteo,
            om.temperature_2m_mean,
            om.shortwave_radiation_sum,
            om.relative_humidity_2m_mean,
            om.wind_speed_10m_max,
            om.wind_direction_10m_dominant,
            om.et0_fao_evapotranspiration,
            om.sunshine_duration
        FROM fact_aod f
        JOIN dim_date d
            ON f.id_date = d.id_date
        JOIN dim_site s
            ON f.id_site = s.id_site
        JOIN dim_openmeteo om
            ON f.id_openmeteo = om.id_openmeteo
        WHERE f.Particle_type = 'fine'
          AND d.Year BETWEEN 2015 AND 2024;
    """

    df = pd.read_sql(query, conn)
    conn.close()

    df["Date"] = pd.to_datetime(df["Date"])
    df["Year"] = df["Year"].astype(int)

    return df

# Cargar datos enriquecidos

with st.spinner("Cargando datos enriquecidos (partículas finas con clima)..."):
    df = load_aod_fine_enriched()

if df.empty:
    st.error("No se encontraron registros enriquecidos. Verifique la base de datos 'prueba'.")
    st.stop()

# Sidebar – Filtros

st.sidebar.header("Filtros")

years = sorted(df["Year"].unique())

if len(years) == 1:
    year_min = year_max = years[0]
    st.sidebar.info(f"Solo hay datos enriquecidos para el año {year_min}.")
    year_range = (year_min, year_max)
else:
    year_min, year_max = int(min(years)), int(max(years))
    year_range = st.sidebar.slider(
        "Rango de años",
        min_value=year_min,
        max_value=year_max,
        value=(year_min, year_max),
        step=1
    )

sites = sorted(df["AERONET_Site"].unique())
selected_sites = st.sidebar.multiselect(
    "Sitios AERONET",
    options=sites,
    default=sites
)

df_filt = df[
    (df["Year"] >= year_range[0]) &
    (df["Year"] <= year_range[1]) &
    (df["AERONET_Site"].isin(selected_sites))
].copy()

if df_filt.empty:
    st.warning("No hay datos enriquecidos para el rango de filtros seleccionado.")
    st.stop()

# Variables climáticas

vars_clima = [
    "temperature_2m_mean",
    "shortwave_radiation_sum",
    "relative_humidity_2m_mean",
    "wind_speed_10m_max",
    "et0_fao_evapotranspiration",
    "sunshine_duration",
    "Precipitable_Water"
]

p90 = df_filt["AOD_Value"].quantile(0.9)

# KPIs principales

col_kpi1, col_kpi2, col_kpi3, col_kpi4 = st.columns(4)

with col_kpi1:
    st.metric("AOD fina promedio (enriquecidos)", f"{df_filt['AOD_Value'].mean():.3f}")

with col_kpi2:
    st.metric("Número de observaciones enriquecidas", f"{len(df_filt):,}".replace(",", "."))

with col_kpi3:
    st.metric("Número de sitios con clima", len(df_filt['AERONET_Site'].unique()))

with col_kpi4:
    st.metric("Percentil 90 de AOD", f"{p90:.3f}")

st.markdown("---")

# Tabs principales 

tab_overview, tab_scatter, tab_interactions, tab_box, tab_series = st.tabs([
    "Visión general",
    "AOD vs clima",
    "Interacciones 2D",
    "AOD alto vs bajo",
    "Serie temporal AOD + clima"
])

# 1) VISIÓN GENERAL

with tab_overview:
    st.subheader("Tendencia temporal de AOD fina (registros enriquecidos)")

    df_daily = (
        df_filt.groupby("Date", as_index=False)
        .agg(AOD_mean=("AOD_Value", "mean"))
        .sort_values("Date")
    )

    fig_trend = px.line(
        df_daily,
        x="Date",
        y="AOD_mean",
        labels={"Date": "Fecha", "AOD_mean": "AOD fina promedio"},
        title="Evolución temporal de AOD fina (registros enriquecidos)"
    )
    st.plotly_chart(fig_trend, use_container_width=True)

    st.caption(
        "Los picos en esta curva indican días con mayor carga de partículas finas "
        "bajo condiciones climáticas específicas."
    )

    # ----- Mapa de AOD -----
    agg_dict = {
        "AOD_Value": "mean",
        "temperature_2m_mean": "mean",
        "shortwave_radiation_sum": "mean",
        "relative_humidity_2m_mean": "mean",
        "wind_speed_10m_max": "mean",
        "et0_fao_evapotranspiration": "mean",
        "sunshine_duration": "mean",
        "Precipitable_Water": "mean",
    }

    df_sites = (
        df_filt.groupby(["AERONET_Site", "Latitude", "Longitude"], as_index=False)
        .agg(agg_dict)
    )

    st.subheader("Mapa de sitios AERONET – AOD fina promedio")

    df_map_aod = df_sites.dropna(subset=["Latitude", "Longitude", "AOD_Value"])

    fig_map_aod = px.scatter_geo(
        df_map_aod,
        lat="Latitude",
        lon="Longitude",
        hover_name="AERONET_Site",
        size="AOD_Value",
        color="AOD_Value",
        color_continuous_scale="YlOrRd",
        labels={"AOD_Value": "AOD fina promedio"},
        title="Distribución espacial de AOD fina promedio por sitio AERONET"
    )

    fig_map_aod.update_layout(
        geo=dict(showland=True, landcolor="rgb(240,240,240)")
    )

    st.plotly_chart(fig_map_aod, use_container_width=True)

    # ----- Mapa de variables climáticas -----
    st.subheader("Mapa de variables climáticas promedio por sitio (AOD como tamaño)")

    nombre_variable = {
        "temperature_2m_mean": "Temperatura media (°C)",
        "shortwave_radiation_sum": "Radiación solar diaria",
        "relative_humidity_2m_mean": "Humedad relativa media (%)",
        "wind_speed_10m_max": "Velocidad máxima del viento (m/s)",
        "et0_fao_evapotranspiration": "Evapotranspiración FAO (mm)",
        "sunshine_duration": "Duración de la insolación (s)",
        "Precipitable_Water": "Agua precipitable",
    }

    var_map = st.selectbox(
        "Variable climática a visualizar",
        options=vars_clima,
        format_func=lambda v: nombre_variable.get(v, v)
    )

    etiqueta_color = f"{nombre_variable.get(var_map, var_map)} promedio"

    df_map_clima = df_sites.dropna(subset=["Latitude", "Longitude", "AOD_Value", var_map])

    fig_map_clima = px.scatter_geo(
        df_map_clima,
        lat="Latitude",
        lon="Longitude",
        hover_name="AERONET_Site",
        size="AOD_Value",
        color=var_map,
        color_continuous_scale="YlOrRd",
        labels={var_map: etiqueta_color, "AOD_Value": "AOD fina promedio"},
        title=f"Distribución espacial de {etiqueta_color} (tamaño de burbuja según AOD fina)"
    )

    fig_map_clima.update_layout(
        geo=dict(showland=True, landcolor="rgb(240,240,240)")
    )

    st.plotly_chart(fig_map_clima, use_container_width=True)


# 2) SCATTER AOD vs CLIMA

with tab_scatter:
    st.subheader("AOD fina frente a variables climáticas")

    var_x = st.selectbox(
        "Variable climática en el eje X",
        [
            "temperature_2m_mean",
            "shortwave_radiation_sum",
            "relative_humidity_2m_mean",
            "wind_speed_10m_max",
            "et0_fao_evapotranspiration",
            "sunshine_duration",
            "Precipitable_Water",
            "Angstrom_Exponent",
        ]
    )

    var_color = st.selectbox(
        "Color por",
        ["Year", "AERONET_Site"],
        index=0
    )

    df_sc = df_filt[[var_x, "AOD_Value", var_color]].dropna()

    if df_sc.empty:
        st.warning("No hay suficientes datos para esta combinación.")
    else:
        fig_sc = px.scatter(
            df_sc,
            x=var_x,
            y="AOD_Value",
            color=var_color,
            opacity=0.7,
            labels={var_x: var_x, "AOD_Value": "AOD (partículas finas)"},
            title=f"AOD fina en función de {var_x} (registros enriquecidos)"
        )
        st.plotly_chart(fig_sc, use_container_width=True)

# 3) INTERACCIONES 2D (HEATMAP)

with tab_interactions:
    st.subheader("Interacciones entre variables climáticas y AOD fina")

    x_var = st.selectbox(
        "Variable en el eje X",
        ["temperature_2m_mean", "relative_humidity_2m_mean",
         "wind_speed_10m_max", "et0_fao_evapotranspiration"],
        index=0
    )

    y_var = st.selectbox(
        "Variable en el eje Y",
        ["relative_humidity_2m_mean", "temperature_2m_mean",
         "wind_speed_10m_max", "sunshine_duration"],
        index=1
    )

    if x_var == y_var:
        st.info("Seleccione variables distintas en X e Y para visualizar el mapa 2D.")
    else:
        df_2d = df_filt[[x_var, y_var, "AOD_Value"]].dropna()

        if df_2d.empty:
            st.warning("No hay suficientes datos para estas dos variables.")
        else:
            fig_heat = px.density_heatmap(
                df_2d,
                x=x_var,
                y=y_var,
                z="AOD_Value",
                histfunc="avg",
                color_continuous_scale="YlOrRd",
                labels={"z": "AOD promedio"},
                title=f"AOD fina promedio según {x_var} y {y_var}"
            )
            st.plotly_chart(fig_heat, use_container_width=True)

# 4) BOXPLOTS AOD ALTO vs BAJO

with tab_box:
    st.subheader("Comparación climática: días de AOD alta y baja")

    var_sel = st.selectbox(
        "Variable climática a comparar",
        vars_clima
    )

    df_box = df_filt.copy()
    df_box["AOD_group"] = np.where(
        df_box["AOD_Value"] >= p90,
        "AOD alta (≥ p90)",
        "AOD baja (< p90)"
    )

    fig_box = px.box(
        df_box,
        x="AOD_group",
        y=var_sel,
        color="AOD_group",
        points="outliers",
        labels={"AOD_group": "Grupo de AOD", var_sel: var_sel},
        title=f"{var_sel}: comparación entre días de AOD fina alta y baja"
    )

    st.plotly_chart(fig_box, use_container_width=True)

# 5) SERIE TEMPORAL AOD + CLIMA (promedios mensuales)

with tab_series:
    st.subheader("Serie temporal de promedios mensuales: AOD fina y variable climática")

    var_t = st.selectbox(
        "Variable climática en el eje derecho",
        vars_clima
    )

    df_ts = df_filt.sort_values("Date").set_index("Date")

    df_monthly = (
        df_ts[["AOD_Value", var_t]]
        .resample("M")
        .mean()
        .reset_index()
    )

    if df_monthly.empty:
        st.warning("No hay datos suficientes para construir la serie mensual.")
    else:
        fig_ts = go.Figure()

        fig_ts.add_trace(go.Scatter(
            x=df_monthly["Date"],
            y=df_monthly["AOD_Value"],
            name="AOD fina (promedio mensual)",
            mode="lines+markers"
        ))

        fig_ts.add_trace(go.Scatter(
            x=df_monthly["Date"],
            y=df_monthly[var_t],
            name=f"{var_t} (promedio mensual)",
            mode="lines+markers",
            yaxis="y2"
        ))

        fig_ts.update_layout(
            title=f"AOD fina y {var_t} (promedios mensuales, registros enriquecidos)",
            yaxis=dict(title="AOD fina"),
            yaxis2=dict(
                title=var_t,
                overlaying="y",
                side="right"
            )
        )

        st.plotly_chart(fig_ts, use_container_width=True)

st.markdown(
    """
---
Este dashboard utiliza exclusivamente registros de AOD en partículas finas
que han sido enriquecidos con información climática diaria, con el propósito
de analizar de forma integrada el comportamiento de los aerosoles y las
condiciones meteorológicas asociadas.
"""
)