# AERONET AOD Streaming Pipeline
### [Enlace a la presentación](https://drive.google.com/file/d/1rl5ZqbNASIptdiwON7dj8_h9-P1MApZR/view?usp=drive_link)
---

Pipeline ETL en streaming para procesar, enriquecer y validar datos de aerosoles atmosféricos (AOD) de la red AERONET de la NASA, integrados con información meteorológica histórica de la API Open-Meteo y cargados a un Data Warehouse en MySQL.
Sobre este DW se construye un dashboard interactivo en Streamlit para análisis temporal, espacial y climático del AOD.

---

# 0. Objetivo general del proyecto

El objetivo general del proyecto es analizar la evolución temporal, espacial y espectral del Aerosol Optical Depth (AOD) para identificar diferencias en la composición de partículas atmosféricas (finas vs gruesas) en los sitios de observación AERONET administrados por NASA.

Este objetivo se logra mediante un pipeline híbrido (streaming + batch) que integra Kafka, Airflow, Open-Meteo, Great Expectations y un DW en MySQL.

---

## 1. Contexto del problema

Los **aerosoles atmosféricos** son partículas diminutas que flotan en el aire. Aunque no las vemos, tienen efectos importantes:

* Pueden empeorar la **calidad del aire** y afectar la **salud**.
* Modifican el **clima** al alterar la radiación solar que llega a la superficie.
* Reducen la **visibilidad** y pueden influir en la formación de nubes y lluvia.

La NASA creó la red **AERONET**, que mide la **profundidad óptica de aerosoles (AOD)**. El AOD indica cuánta luz solar es absorbida o dispersada por estas partículas: cuanto más alto, más carga de aerosoles en la columna atmosférica.

Este proyecto construye un pipeline completo para:

* Procesar datos AOD de AERONET en **streaming**.
* Enriquecerlos con clima histórico.
* Validar su calidad.
* Cargarlos en un modelo dimensional para su análisis en un **dashboard interactivo**.

---

## 2. Fuentes de datos

### 2.1. AERONET – Aerosoles finos y gruesos

Los aerosoles provienen de dos grandes tipos de fuentes:

* **Naturales**: incendios forestales, volcanes, desiertos (polvo mineral), océanos (sal marina).
* **Antropogénicas**: industria, tráfico urbano, quema de combustibles, agricultura.

En el análisis distinguimos entre:

* **Partículas finas**: asociadas principalmente a contaminación y procesos de combustión (tráfico, industria, humo).
* **Partículas gruesas**: asociadas a polvo mineral, sal marina y otras fuentes naturales.

Esta distinción es clave porque los impactos en **salud** y **clima** dependen del tipo de partícula. El dataset de AERONET aporta el AOD en varias longitudes de onda y el **Angstrom Exponent**, que se utiliza para clasificar el tipo de partícula (fine / coarse / mixed).

### 2.2. API Meteo Historical (Open-Meteo)

Además de los datos de la NASA, el pipeline enriquece la información con la **API histórica de Open-Meteo**:

* Permite descargar datos meteorológicos desde el año 2000.
* Variables usadas (entre otras):

  * Temperatura media.
  * Humedad relativa.
  * Velocidad y dirección del viento.
  * Radiación solar.
  * Evapotranspiración (ET0).
  * Duración de la luz solar.

Estas variables ayudan a entender **por qué cambia el AOD**:

* Alta humedad → las partículas se hinchan con agua → el AOD tiende a subir.
* Viento fuerte → puede traer polvo o limpiar la atmósfera.
* Radiación solar ↓ → puede indicar mayor carga de aerosoles.
* Temperatura y evaporación influyen en formación, transporte y dispersión de partículas.

Combinando **AERONET + Open-Meteo**, no solo vemos cuánto AOD hay, sino también **bajo qué condiciones meteorológicas** aumentan o disminuyen los aerosoles.

---

## 3. Arquitectura y flujo del pipeline

<img width="1920" height="1080" alt="image" src="https://github.com/user-attachments/assets/19a59fe5-f56e-4fbe-8902-1b8f9f082d93" />

Flujo general del proyecto:

1. **Producer (`producer.py`)**

   * Lee el archivo original de AERONET
     `All_Sites_Times_Daily_Averages_AOD20.csv`.
   * Lo envía al topic `general_input` en **lotes de 100 registros**, simulando un stream de datos (con pequeñas pausas entre lotes).

2. **Consumer transformador (`consumer.py`)**

   * Escucha el topic `general_input`.
   * Limpia el lote, reemplaza códigos de missing, convierte tipos y formatea fechas.
   * Pasa los AOD a formato **largo** (de columnas tipo `AOD_440nm`, `AOD_675nm`, etc. a filas sitio–fecha–hora–longitud de onda).
   * Construye las dimensiones **básicas del modelo dimensional**:

     * `dim_date`
     * `dim_site`
     * `dim_wavelength`
   * Genera la tabla de hechos `fact` con:

     * IDs de dimensiones.
     * Valores de AOD.
     * Agua precipitable.
     * Angstrom Exponent.
     * Tipo de partícula (fine / coarse / mixed).
   * Envía:

     * Un mensaje `clean_batch` a `general_output` (modelo dimensional limpio).
     * Un mensaje `fine_batch` a `fine_particles` con solo partículas finas.

3. **Consumer de enriquecimiento (`enriquecimiento_general.py`)**

   * Escucha el topic `general_output`.
   * Para cada combinación única **fecha–latitud–longitud**:

     * Consulta la API histórica de Open-Meteo (con caché y reintentos).
     * Construye la dimensión `dim_weather` con variables climáticas.
     * Asigna un `id_weather` a cada observación y un flag `is_enriched` (1 si encontró clima, 0 en caso contrario).
   * Empaqueta todo como `enriched_batch` y lo envía al topic `check`.

4. **Consumer de calidad de datos (`consumer_check_rebuild.py`)**

   * Escucha el topic `check`.
   * Valida **fact y dimensiones** con **Great Expectations**.
   * Calcula un porcentaje global de reglas cumplidas por lote.
   * Solo si el promedio global ≥ **85 %**, el lote se **aprueba**:

     * Se eliminan duplicados.
     * Se carga el lote en MySQL alimentando el **Data Warehouse**.
   * Si no cumple el 85 %, el lote se **rechaza** y no se carga.

5. **Consumer de partículas finas (`consumer_fine_particles.py`)**

   * Escucha el topic `fine_particles`.
   * Reconstruye un dataset de ~5000 registros **solo de partículas finas**.
   * Guarda un CSV (por ejemplo `fine_particles.csv`) con esa muestra.
   * Lanza un **trigger de Airflow** para iniciar un DAG batch basado en ese dataset.

6. **Dashboards**

   * El **Data Warehouse en MySQL** se va actualizando casi en tiempo real a través de Kafka.
   * Un dashboard en **Streamlit** consulta ese DW y muestra:

     * Evolución temporal de AOD.
     * Comparación de partículas finas vs gruesas.
     * Mapas por sitio/país/continente.
     * Relación entre AOD y variables climáticas.
   * El CSV de partículas finas se usa también en flujos batch orquestados con Airflow.

---

# **4. EDA del archivo original y transformaciones**

## 4.1 Características generales del archivo original

El archivo contenía más de 1.3 millones de registros con las siguientes particularidades:

* Mediciones diarias estandarizadas a las 12:00 UTC.
* Sitios identificados por latitud, longitud y elevación.
* 82 columnas totales, muchas redundantes o irrelevantes.
  <img width="1061" height="429" alt="image" src="https://github.com/user-attachments/assets/72eb78e7-a66a-4847-a7d1-23f79e79c6bb" />

* Valores faltantes representados por -999.
  <img width="1057" height="451" alt="image" src="https://github.com/user-attachments/assets/e4f0410f-8b33-4226-90b0-a4d79d8f145a" />

* Valores de AOD negativos (fisicamente imposible)
* Varias columnas de AOD para diferentes longitudes de onda (340–1020 nm).
* Presencia de Ångström Exponent, útil para estimar el tamaño relativo de las partículas.
* Variabilidad significativa entre sitios y entre bandas espectrales.

**Conclusión general del EDA:**
El dataset es extenso, heterogéneo y complejo. Su estructura en formato ancho y la presencia de valores inválidos vuelven inviable su análisis directo, lo que justifica la creación del pipeline ETL.

---

## 4.2 Problemas detectados y transformaciones asociadas

### 1. Exceso de columnas y redundancia

* Reducción de 82 columnas a 31 columnas clave.
* Se mantuvieron solo las variables esenciales para análisis espectral, espacial y temporal.

### 2. Valores faltantes representados por -999

* Se reemplazaron por `NaN`.
* Se excluyeron en cálculos sensibles para evitar sesgos.

### 3. AOD con valores negativos

* Cualquier valor < 0 fue reemplazado por 0.
* Se aplicó un criterio físico consistente: el AOD no puede ser negativo.

### 4. Falta de contexto geográfico

* Se enriqueció con Natural Earth (país y continente).
* Permite análisis espaciales coherentes.

### 5. Dataset en formato ancho (wide)

* Conversión a formato largo mediante `melt()`.
* Cada fila pasó a representar: sitio – fecha – longitud de onda – valor AOD.

Esto permitió:

* Comparación correcta entre bandas espectrales,
* Normalización,
* Estructura ideal para el modelo dimensional (fact + dims).

---

## 4.3 Nuevas columnas creadas durante las transformaciones

* **Spectral_Band:** clasificación UV, VIS o NIR.
* **Sensitive_Aerosol:** sensibilidad del AOD a partículas finas, mixtas o gruesas.
* **Particle_type:**

  * ≥ 1.5 → finas
  * 1.0–1.5 → mixtas
  * ≤ 1.0 → gruesas

Estas columnas permitieron relacionar el espectro óptico con el tamaño de partícula para análisis físico y climático.

---

## 5. Componentes principales

### 5.1. `producer.py`

* Punto de entrada del pipeline.

* Carga el CSV original de AERONET en un DataFrame de pandas.

* Envía lotes de 100 filas al topic `general_input` con mensajes tipo:

  ```json
  {
    "type": "data",
    "rows": [ ... 100 filas en formato JSON ... ]
  }
  ```

* Incluye pausas entre lotes para simular mediciones en tiempo real.

* Al final envía un mensaje `sentinel` para indicar el fin del stream.

### 5.2. `consumer.py` (transformador principal)

* Escucha `general_input`.
* Limpia y transforma:

  * Reemplaza valores `-999` por `NaN`.
  * Convierte fechas y horas al formato correcto.
  * Fuerza columnas a tipo numérico.
  * Clasifica el **tipo de partícula** según el Angstrom Exponent.
* Convierte el dataset a formato **largo** por longitud de onda.
* Construye y mantiene en memoria las dimensiones:

  * `dim_date` (fechas, componentes de calendario).
  * `dim_site` (sitio, coordenadas, país, continente).
  * `dim_wavelength` (longitud de onda, banda espectral, sensibilidad a finas/gruesas).
* Genera la tabla de hechos `fact` con:

  * `Fact_ID`
  * `id_date`, `id_site`, `id_wavelength`
  * AOD, agua precipitable, Angstrom, tipo de partícula.
* Envía:

  * `clean_batch` → `general_output`
  * `fine_batch` → `fine_particles` (solo partículas finas).
* Reenvía `sentinel` cuando se termina el stream.

### 5.3. `enriquecimiento_general.py` (consumer enriquecedor)

* Escucha `general_output`.
* Construye combinaciones únicas **fecha–latitud–longitud** desde `fact + dim_site + dim_date`.
* Usando la API de Open-Meteo:

  * Descarga clima histórico con un cliente con **caché** y **reintentos**.
  * Crea `dim_weather` con temperatura, humedad, radiación, viento, ET0, etc.
  * Asigna `id_weather` a cada observación.
* Marca en `fact` si una observación fue enriquecida (`is_enriched`).
* Envía un `enriched_batch` con:

  * `fact` enriquecida.
  * `dim_date`, `dim_site`, `dim_wavelength`.
  * Nuevas filas de `dim_weather`.
* Envía `sentinel` a `check` al final del flujo.

### 5.4. `consumer_check_rebuild.py` (data quality)

Este archivo es el **consumidor de calidad** del pipeline:

* Escucha el topic `check`.
* Configura un contexto de **Great Expectations** en modo runtime para validar DataFrames de pandas.
* Define expectativas para:

  * `dim_date`:

    * Formato válido de fecha y hora.
    * Años, meses y días en rangos válidos.
    * No nulos en campos clave.
  * `dim_site`:

    * Coordenadas dentro del rango del planeta.
    * IDs y nombres de sitio únicos.
    * País y continente informados.
  * `dim_wavelength`:

    * Longitudes de onda dentro de rangos físicos razonables.
    * Bandas espectrales y tipos de aerosol en conjuntos permitidos.
  * `dim_weather`:

    * Variables climáticas no nulas.
    * Rangos coherentes para temperatura, humedad, viento, radiación, etc.
  * `fact`:

    * Llaves foráneas válidas.
    * Tipo de partícula permitido.
    * AOD, agua precipitable, Angstrom no negativos.
    * Flags de enriquecimiento consistentes.

Flujo de validación:

1. Convierte el mensaje `enriched_batch` en cinco DataFrames (fact + 4 dimensiones).
2. Ejecuta todas las expectativas sobre cada tabla.
3. Calcula el **porcentaje de tests cumplidos por tabla**.
4. Calcula el **promedio global de calidad del lote**.
5. Si el promedio ≥ **85 %**:

   * El lote se **aprueba**.
   * Se eliminan duplicados.
   * Se inserta en MySQL (DW).
6. Si el promedio < 85 %:

   * El lote se **rechaza** (no se carga al DW).

En resumen, este script es el **filtro de control de calidad en tiempo real**: solo los datos que pasan por este umbral alimentan el data warehouse y, por tanto, lo que se ve en el dashboard.

### 5.5. `consumer_fine_particles.py`

* Escucha el topic `fine_particles`.
* Espera el primer mensaje válido (`fine_batch` o `sentinel`).
* Va acumulando filas desde cada `fine_batch` en memoria.
* Cuando:

  * Se alcanzan ~5000 registros, o
  * Llega un `sentinel`,

  entonces:

  * Construye un DataFrame de pandas.
  * Lo guarda como `fine_particles.csv` en el directorio de datos.
  * Lanza un **trigger de Airflow** para que un DAG procese ese CSV (flujo batch).

Sirve para obtener un dataset denso solo de partículas finas para análisis, pruebas y experimentos sin tener que reprocesar todo el stream.

---

## 6. Data Quality con Great Expectations

En el pipeline, el módulo de **data quality** se ejecuta **en tiempo real antes de cargar cualquier lote** a la base de datos:

* Se aplican pruebas específicas a cada tabla:

  * Formatos de fecha y hora.
  * Rango de coordenadas.
  * Valores físicos plausibles para AOD y clima.
  * Integridad de claves y unicidad de IDs.
* Cada regla genera un “mini test” y se calcula el porcentaje de cumplimiento:

  * Por tabla.
  * Y un promedio global.

**Política de aceptación:**

* Si el **promedio global de calidad ≥ 85 %** → el lote **sí se carga** a MySQL.
* Si **< 85 %** → el lote **no se carga** al DW.

De esta forma, el dashboard trabaja siempre con datos que han pasado por un mínimo de **consistencia y confiabilidad**, no con datos crudos sin validar.

---

# **6. Airflow DAG Design**

El flujo batch procesado por Apache Airflow se basa en un DAG diseñado para ejecutar cinco etapas clave:

### 1. Extract

Lee el CSV generado por el consumidor de partículas finas, valida columnas esenciales y genera un parquet de staging.

### 2. Enrich

Consulta la API histórica de Open-Meteo para obtener variables climáticas para cada combinación única de fecha, latitud y longitud.
Incluye caching, reintentos y control de errores.

### 3. Merge

Integra el parquet crudo con el parquet meteorológico utilizando Polars.
Genera un dataset enriquecido en formato Parquet y CSV.

### 4. Quality Check

Usa Great Expectations para validar:

* Rango físico de variables,
* Ausencia de nulos en columnas clave,
* Integridad de llaves,
* Unicidad de Fact_ID.

Si menos del 90 % de las expectativas se cumplen, el DAG falla intencionalmente.

### 5. Load

Carga la dimensión `dim_estatic` en MySQL y actualiza `fact_aod` asignando `id_estatic` mediante join con `dim_site` y `dim_date`.

El DAG está configurado sin schedule automático y se activa mediante el script `consumer_fine_particles.py`.

---

## 8. Cómo ejecutar el proyecto

### 8.1. Requisitos

* Python 3.x
* Apache Kafka + Zookeeper (entorno local).
* MySQL (para el Data Warehouse).
* Airflow (opcional, para los flujos batch con el CSV de finas).
* Librerías Python típicas:

  * `pandas`, `numpy`, `kafka-python`, `geopandas`, `requests`, `requests-cache`, `openmeteo-requests`, `great_expectations==0.17.23`, `sqlalchemy`, `streamlit`, etc.

*(Se recomienda usar `requirements.txt` y un entorno virtual.)*

### 8.2. Arrancar Zookeeper y Kafka (Windows)

Desde la carpeta de Kafka:

```bash
# Iniciar Zookeeper
.\bin\windows\zookeeper-server-start.bat .\config\zookeeper.properties

# Verificar que está corriendo
netstat -an | findstr 2181
```

En otra terminal:

```bash
# Iniciar servidor de Kafka
.\bin\windows\kafka-server-start.bat .\config\server.properties

# Verificar que está corriendo
netstat -an | findstr 9092
```

### 8.3. Crear topics

```bash
.\bin\windows\kafka-topics.bat --create --topic general_input   --bootstrap-server localhost:9092
.\bin\windows\kafka-topics.bat --create --topic general_output  --bootstrap-server localhost:9092
.\bin\windows\kafka-topics.bat --create --topic fine_particles  --bootstrap-server localhost:9092
.\bin\windows\kafka-topics.bat --create --topic check           --bootstrap-server localhost:9092

# Listar topics activos
.\bin\windows\kafka-topics.bat --list --bootstrap-server localhost:9092
```

### 8.4. Orden de ejecución del pipeline

En Visual Studio Code / terminales separados:

1. **Levantar primero los consumers**:

   ```bash
   python consumer.py
   python enriquecimiento_general.py
   python consumer_check_rebuild.py
   python consumer_fine_particles.py   # si se usa la parte de finas
   ```

2. **Luego ejecutar el producer**:

   ```bash
   python producer.py
   ```

3. **Finalmente, levantar el dashboard en Streamlit**:

   ```bash
   streamlit run .\dashboard.py
   ```

A medida que el producer envía lotes, los consumers van transformando, enriqueciendo, validando y cargando los datos en MySQL, y el dashboard se va alimentando de ese DW.

### 8.5. Cómo ejecutar el DAG en Airflow (Nueva sección solicitada)

### 1. Iniciar Airflow con Docker Compose:

```bash
docker-compose up -d
```

### 2. Acceder a la interfaz web:

```
http://localhost:8080
```

Usuario por defecto:

* user: airflow
* password: airflow

### 3. Habilitar el DAG:

En la UI, activar:

```
etl_fine_particles_meteo_enrichment
```

### 4. Ejecutar el DAG:

* Click en **Trigger DAG**
* Verificar logs de cada task
* El DAG leerá `fine_particles.csv` generado por Kafka

### 5. Correr el dashboard Estático:

El dashboard se encuentra en la carpeta `dashboard/` y el script principal es `streamlit.py`.

1. Verificar conexión a MySQL  

   El dashboard se conecta a la base de datos y consulta las tablas:

   - `fact_aod`
   - `dim_date`
   - `dim_site`
   - `dim_estatic`

   El archivo `streamlit.py` usa estas variables de conexión:

   - `MYSQL_HOST` (por defecto: `localhost`)
   - `MYSQL_PORT` (por defecto: `3306`)
   - `MYSQL_USER` (por defecto: `root`)
   - `MYSQL_PASSWORD` (por defecto: `root`)
   - Base de datos fija en el código: `prueba`

   Si no defines variables de entorno, se usarán esos valores por defecto, que deben coincidir con tu servidor MySQL.

2. Configurar `secrets.toml` (opcional)

   En `dashboard/.streamlit/secrets.toml` puedes guardar la configuración de MySQL, por ejemplo:

   ```toml
   [mysql]
   host = "localhost"
   port = 3306
   user = "root"
   password = "root"
   database = "prueba"````

3. Ejecutar el dashboard

   Desde la raíz del proyecto:

   ```bash
   cd dashboard
   streamlit run streamlit.py
   ```

   Por defecto Streamlit quedará disponible en:

   ```
   http://localhost:8501
   ```
---

## 9. Dashboard de AOD

El dashboard en Streamlit permite explorar el AOD desde varias dimensiones:

### 9.1. ¿Qué se puede ver?

* **Nivel de carga de aerosoles**:

  * AOD promedio en el período y región seleccionados.
  * Clasificación cualitativa de contexto: aire relativamente limpio, moderadamente cargado, alta carga de aerosoles.

* **Evolución temporal**:

  * Series de tiempo de AOD (total, partículas finas, partículas gruesas).
  * Detección de tendencias, disminuciones o patrones estacionales.

* **Partículas finas vs gruesas**:

  * Gráficas comparando la contribución de aerosoles finos y gruesos.
  * Distribuciones y series temporales por tipo de partícula.
  * Identificación de qué tipo predomina en un sitio o continente.

* **Dimensión espacial**:

  * Mapas globales por sitio y por país con AOD promedio.
  * Comparaciones por continente y tipo de partícula.
  * Hotspots donde el AOD es sistemáticamente más alto.

* **Relación con variables climáticas**:

  * Matriz de correlación entre AOD y variables del clima.
  * Gráficos de dispersión AOD vs temperatura, humedad, viento, radiación, etc.
  * Series de tiempo AOD + una variable climatológica en paralelo.

* **Streaming y actualización**:

  * El dashboard se alimenta de un DW que se actualiza casi en tiempo real a través de Kafka.
  * Los datos que se muestran **ya pasaron por transformación, enriquecimiento y control de calidad**.

* **Descarga de datos filtrados**:

  * Posibilidad de descargar subconjuntos de datos para análisis externos, modelos adicionales o reportes personalizados.

### 9.2. ¿A quién le puede interesar?

* Investigadores en calidad del aire y clima.
* Autoridades ambientales.
* Operadores de redes de monitoreo.
* Empresas sensibles a la calidad del aire.
* Comunidad académica y estudiantil.

El dashboard funciona tanto como **herramienta de análisis histórico** como de **monitoreo continuo**.

---



## 11. Conclusiones

* El proyecto integra en un solo pipeline las fases de **extracción, transformación, enriquecimiento, control de calidad y carga** de datos AOD.

* La combinación de:

  * **AERONET** (aerosoles),
  * **Open-Meteo** (clima),
  * **Kafka** (streaming),
  * **Great Expectations** (data quality),
  * **MySQL** (DW) y
  * **Streamlit** (visualización)

  permite pasar de datos crudos a **insights accionables** sobre aerosoles.

* El dashboard permite responder preguntas como:

  * ¿Dónde están los mayores niveles de aerosoles?
  * ¿Son principalmente partículas finas (contaminación) o gruesas (polvo/sal)?
  * ¿En qué períodos del año se disparan los niveles de AOD?
  * ¿Bajo qué condiciones meteorológicas se presentan los mayores episodios de AOD?

* Gracias al umbral de calidad del **85 %**, se garantiza que las conclusiones que se extraen del dashboard estén basadas en datos con un nivel mínimo de **consistencia y confiabilidad**.

En conjunto, este proyecto muestra un ejemplo completo de cómo diseñar e implementar un **pipeline ETL en streaming** orientado a análisis ambiental, integrando datos satelitales, APIs externas, validación automática y visualización interactiva.


En conjunto, este proyecto muestra un ejemplo completo de cómo diseñar e implementar un **pipeline ETL en streaming** orientado a análisis ambiental, integrando datos satelitales, APIs externas, validación automática y visualización interactiva.
