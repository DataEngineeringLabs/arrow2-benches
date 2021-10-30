## Benches for arrow2

Miscelaneous benches of the arrow2 crate against other crates or implementations
to check its performance.

```bash
python3 -m venv venv
venv/bin/pip install -U pip
venv/bin/pip install matplotlib

cargo bench
venv/bin/python summarize.py
```

## Avro

Currently the benches show a different scaling vs the official and non-official implementations:

```
arrow2
size, time (ms)
1024, 49.74483399174329
4096, 190.35771459265735
16384, 753.3089729600148
65536, 2984.9877905882354
262144, 11928.902975999996
1048576, 47522.94082
mz-avro
size, time (ms)
1024, 229.62299835950017
4096, 837.8493449459621
16384, 3256.515289375
65536, 12912.201869999999
262144, 53139.18834
1048576, 206670.37229
avro
size, time (ms)
1024, 219.86431735643893
4096, 875.1018998068221
16384, 3410.7918426666665
65536, 12594.5307275
262144, 50786.8811
1048576, 221235.15888
```
