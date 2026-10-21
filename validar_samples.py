import json
import jsonschema

schema = json.load(open("docs/evento.schema.json", encoding="utf-8"))

for nombre in ["evento_valido", "evento_con_falla", "evento_invalido"]:
    evento = json.load(open(f"samples/{nombre}.json", encoding="utf-8"))
    try:
        jsonschema.validate(evento, schema)
        print(nombre, "-> OK")
    except jsonschema.ValidationError as e:
        print(nombre, "-> RECHAZADO:", e.message)