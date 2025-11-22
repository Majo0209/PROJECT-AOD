import json
import os
import pandas as pd
from kafka import KafkaConsumer

TARGET_ROWS = 5000

OUTPUT_DIR = "../data"
os.makedirs(OUTPUT_DIR, exist_ok=True)

print("🔄 Esperando mensajes en topic 'fine_particles'...")

consumer = KafkaConsumer(
    "fine_particles",
    bootstrap_servers=["localhost:9092"],
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="fine_particles_rebuild_group"
)

rows_accumulated = []
first_message_received = False

for msg in consumer:
    payload = msg.value
    msg_type = payload.get("type")

    if not first_message_received:
        if msg_type not in ["fine_batch", "sentinel"]:
            continue

        print("📨 Primer mensaje recibido. Iniciando reconstrucción...")
        first_message_received = True

    if msg_type == "fine_batch":
        batch_rows = payload.get("rows", [])
        rows_accumulated.extend(batch_rows)

        print(
            f"📥 Recibidas {len(batch_rows)} filas nuevas — Total acumulado: {len(rows_accumulated)}")

        if len(rows_accumulated) >= TARGET_ROWS:
            print("🎯 Umbral alcanzado: 5000 registros de partículas finas.")
            break

    elif msg_type == "sentinel":
        print("⛔ Recibido sentinel. No habrá más datos.")
        break

if rows_accumulated:
    df_final = pd.DataFrame(rows_accumulated)
    print(
        f"📊 DataFrame final creado con {len(df_final)} registros de partículas finas.")

    output_path = os.path.join(OUTPUT_DIR, "fine_particles.csv")
    df_final.to_csv(output_path, index=False, encoding="utf-8")

    print(f"💾 Archivo guardado en: {output_path}")

else:
    df_final = pd.DataFrame()
    print("⚠ No se recibieron registros de partículas finas.")

print("🏁 Proceso completado.")
