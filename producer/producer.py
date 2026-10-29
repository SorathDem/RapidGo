import argparse
import json
import random
import time

from kafka import KafkaProducer

from producer.generator import crear_flota, siguiente_evento, evento_invalido

TOPIC = "rapidgo.fleet.telemetry"
EQUIPO = "equipo6-flota"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bootstrap", default="localhost:9092")
    ap.add_argument("--vehiculos", type=int, default=10)
    ap.add_argument("--rate", type=float, default=5.0, help="eventos por segundo")
    ap.add_argument("--max-eventos", type=int, default=0, help="0 = infinito")
    ap.add_argument("--invalidos", type=float, default=0.02, help="fraccion de eventos corruptos")
    args = ap.parse_args()

    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap,
        key_serializer=lambda k: k.encode("utf-8"),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",
        linger_ms=20,
    )
    flota = crear_flota(args.vehiculos)
    ids = list(flota)
    enviados = 0
    try:
        while args.max_eventos == 0 or enviados < args.max_eventos:
            if random.random() < args.invalidos:
                key, evento = "INVALID", evento_invalido()
            else:
                key = random.choice(ids)
                evento = siguiente_evento(flota[key])
            futuro = producer.send(TOPIC, key=key, value=evento,
                                   headers=[("equipo", EQUIPO.encode())])
            enviados += 1
            if enviados <= 10 or enviados % 100 == 0:
                md = futuro.get(timeout=10)
                print(f"#{enviados} key={key} -> particion={md.partition} offset={md.offset}")
            time.sleep(1.0 / args.rate)
    except KeyboardInterrupt:
        print("Detenido por el usuario")
    finally:
        producer.flush()
        producer.close()
        print(f"Total enviados: {enviados}")


if __name__ == "__main__":
    main()