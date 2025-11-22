import pandas as pd
import json
import time
from kafka import KafkaProducer

df = pd.read_csv("../data/All_Sites_Times_Daily_Averages_AOD20.csv")

producer = KafkaProducer(
    bootstrap_servers=["localhost:9092"],
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

topic = "general_input"
batch_size = 100

total_rows = len(df)
print(f"Total de filas a enviar: {total_rows}")

for i in range(0, total_rows, batch_size):
    batch = df.iloc[i:i + batch_size]

    message = {
        "type": "data",
        "rows": batch.to_dict(orient="records")
    }

    producer.send(topic, value=message)
    print(f"Lote enviado: filas {i} → {i + len(batch) - 1}")

    time.sleep(40)

sentinel_message = {"type": "sentinel"}
producer.send(topic, value=sentinel_message)
print("Mensaje sentinela enviado (no hay más datos).")

producer.flush()
print("Envío completo.")
