use std::io::Cursor;
use std::sync::Arc;

use arrow2::error::Result;
use arrow2::io::avro::read;
use avro_rs::types::Record;
use avro_rs::*;
use avro_rs::{Codec, Schema as AvroSchema};
use criterion::*;
use mz_avro::Reader as MzReader;
use rand::{distributions::Alphanumeric, Rng};

#[derive(Debug, Copy, Clone)]
enum Type {
    Utf8,
    Int,
}

fn schema(type_: Type) -> AvroSchema {
    let raw_schema = match type_ {
        Type::Utf8 => {
            r#"
    {
        "type": "record",
        "name": "test",
        "fields": [
            {"name": "a", "type": "string"}
        ]
    }
"#
        }
        Type::Int => {
            r#"
    {
        "type": "record",
        "name": "test",
        "fields": [
            {"name": "a", "type": "int"}
        ]
    }
"#
        }
    };
    AvroSchema::parse_str(raw_schema).unwrap()
}

fn write(size: usize, has_codec: bool, type_: Type) -> Result<Vec<u8>> {
    let avro = schema(type_);
    // a writer needs a schema and something to write to
    let mut writer: Writer<Vec<u8>>;
    if has_codec {
        writer = Writer::with_codec(&avro, Vec::new(), Codec::Deflate);
    } else {
        writer = Writer::new(&avro, Vec::new());
    }

    match type_ {
        Type::Utf8 => {
            (0..size).for_each(|_| {
                let mut record = Record::new(writer.schema()).unwrap();
                record.put("a", "foo");
                writer.append(record).unwrap();
            });
        }
        Type::Int => {
            (0..size).for_each(|_| {
                let mut record = Record::new(writer.schema()).unwrap();
                record.put("a", 1);
                writer.append(record).unwrap();
            });
        }
    }

    Ok(writer.into_inner().unwrap())
}

fn read_avro(buffer: &[u8], size: usize) -> Result<()> {
    let file = Cursor::new(buffer);

    let reader = Reader::new(file).unwrap();

    let mut rows = 0;
    for _ in reader {
        rows += 1;
    }
    assert_eq!(rows, size);
    Ok(())
}

fn read_mz_avro(buffer: &[u8], size: usize) -> Result<()> {
    let file = Cursor::new(buffer);

    let reader = MzReader::new(file).unwrap();

    let mut rows = 0;
    for _ in reader {
        rows += 1;
    }
    assert_eq!(rows, size);
    Ok(())
}

fn read_batch(buffer: &[u8], size: usize) -> Result<()> {
    let mut file = Cursor::new(buffer);

    let (avro_schema, schema, codec, file_marker) = read::read_metadata(&mut file)?;

    let reader = read::Reader::new(
        read::Decompressor::new(
            read::BlockStreamIterator::new(&mut file, file_marker),
            codec,
        ),
        avro_schema,
        Arc::new(schema),
    );

    let mut rows = 0;
    for maybe_batch in reader {
        let batch = maybe_batch?;
        rows += batch.num_rows();
    }
    assert_eq!(rows, size);
    Ok(())
}

fn add_benchmark(c: &mut Criterion) {
    let tasks = [
        (
            "arrow_avro_read",
            read_batch as fn(&[u8], usize) -> Result<()>,
        ),
        ("mz_avro_read", read_mz_avro),
        ("avro_read", read_avro),
    ];

    for (task_name, task) in tasks {
        let mut group = c.benchmark_group(task_name);

        for type_ in [Type::Utf8, Type::Int] {
            for compressed in [true, false] {
                for log2_size in (10..=20).step_by(2) {
                    let size = 2usize.pow(log2_size);
                    let buffer = write(size, compressed, type_).unwrap();

                    group.throughput(Throughput::Elements(size as u64));

                    let name = if compressed {
                        format!("{:?} deflate", type_)
                    } else {
                        format!("{:?}", type_)
                    }
                    .to_lowercase();

                    group.bench_with_input(
                        BenchmarkId::new(name, log2_size),
                        &buffer,
                        |b, buffer| b.iter(|| (task)(buffer, size).unwrap()),
                    );
                }
            }
        }
    }
}

criterion_group!(benches, add_benchmark);
criterion_main!(benches);
