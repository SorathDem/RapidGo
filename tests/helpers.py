import json
from pathlib import Path

import jsonschema

RAIZ = Path(__file__).resolve().parent.parent
SCHEMA = json.loads((RAIZ / "docs" / "evento.schema.json").read_text(encoding="utf-8"))


def cargar_sample(nombre):
    return json.loads((RAIZ / "samples" / f"{nombre}.json").read_text(encoding="utf-8"))


def validar(evento):
    jsonschema.validate(evento, SCHEMA, format_checker=jsonschema.FormatChecker())