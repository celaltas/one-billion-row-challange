use criterion::{black_box, criterion_group, criterion_main, Criterion};
use one_billion_row_challange::{
    extract_city_temp, extract_city_temp_with_parser, read_by_threads_shared_data,
    read_by_threads_with_broadcast_channels, read_by_threads_with_mpsc_channels, run,
};

pub fn criterion_benchmark(c: &mut Criterion) {
    let mut group_comparing_run = c.benchmark_group("comparing_run");
    group_comparing_run.bench_function("by_multi_threads_with_broadcast", |b| {
        b.iter(|| read_by_threads_with_broadcast_channels())
    });
    group_comparing_run.bench_function("by_multi_threads_with_mpsc", |b| {
        b.iter(|| read_by_threads_with_mpsc_channels())
    });
    group_comparing_run.bench_function("by_multi_threads", |b| {
        b.iter(|| read_by_threads_shared_data())
    });
    group_comparing_run.bench_function("by_single_thread", |b| b.iter(|| run()));
    group_comparing_run.finish();

    let mut group_city_and_temp_parse = c.benchmark_group("city_and_temp_parse");
    group_city_and_temp_parse.bench_function("extract_city_temp", |b| {
        b.iter(|| extract_city_temp(black_box("Adana;32.1")))
    });
    group_city_and_temp_parse.bench_function("extract_city_temp_with_parser", |b| {
        b.iter(|| extract_city_temp_with_parser(black_box("Adana;32.1")))
    });
    group_city_and_temp_parse.finish();
}
criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
