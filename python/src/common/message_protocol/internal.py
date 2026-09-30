import json

# Tipos de mensaje internos (primer elemento de cada mensaje)
DATA = "data"
EOF = "eof"
FLUSH = "flush"
COUNT = "count"
TOTAL = "total"


def serialize(message):
    return json.dumps(message).encode("utf-8")


def deserialize(message):
    return json.loads(message.decode("utf-8"))