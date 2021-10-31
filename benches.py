import timeit
import io
import os
import json
from typing import List

import fastavro


def write(size: int) -> bytes:
    schema = {
        "name": "test",
        "type": "record",
        "fields": [
            {"name": "name", "type": "string"},
        ],
    }
    # Parse the schema so we can use it to write the data
    schema = fastavro.parse_schema(schema)
    # Write data to an avro file
    f = io.BytesIO()
    records = ({"name": "foo"} for _ in range(size))
    fastavro.writer(f, schema, records)
    f.seek(0)
    return f.read()


def read(f: bytes) -> List[str]:
    reader = fastavro.reader(io.BytesIO(f))
    return [user for user in reader]


def bench(size: int) -> float:
    data = write(size)

    def f():
        reader = fastavro.reader(io.BytesIO(data))
        return [_ for _ in reader]

    seconds = timeit.Timer(f).timeit(number=100) / 100
    ns = seconds * 1000 * 1000 * 1000
    return ns


def _report(name: str, result: float):
    path = f"target/criterion/{name}/new"
    os.makedirs(path, exist_ok=True)
    with open(f"{path}/estimates.json", "w") as f:
        json.dump({"mean": {"point_estimate": result}}, f)


for size in range(10, 22, 2):
    ns = bench(2 ** size)
    _report(f"fastavro/utf8/{size}", ns)

    print(f"2^{size},{ns:.3f} ns")
