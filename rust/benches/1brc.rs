use criterion::{black_box, criterion_group, criterion_main, Criterion};
use one_billion_row_challange::{
    extract_city_temp, extract_city_temp_with_parser, read_by_single_thread_with_btree,
    read_by_single_thread_with_fast_hasher, read_by_single_thread_with_hashmap,
    read_by_single_thread_with_hashmap_stats, read_by_threads_shared_data,
    read_by_threads_with_broadcast_channels, read_by_threads_with_mpsc_channels, run,
};

pub fn criterion_benchmark(c: &mut Criterion) {
    // let mut group_comparing_data_structures = c.benchmark_group("comparing_data_structures");
    // group_comparing_data_structures.bench_function("calc_instantlty", |b| {
    //     b.iter(|| read_by_single_thread_with_hashmap_stats())
    // });

    // group_comparing_data_structures.bench_function("store_all_temps", |b| {
    //     b.iter(|| read_by_single_thread_with_hashmap())
    // });
    // group_comparing_data_structures.bench_function("using_btree", |b| {
    //     b.iter(|| read_by_single_thread_with_btree())
    // });

    // group_comparing_data_structures.bench_function("fast_hasher", |b| {
    //     b.iter(|| read_by_single_thread_with_fast_hasher())
    // });

    // group_comparing_data_structures.finish();

    // let mut group_comparing_run = c.benchmark_group("comparing_run");
    // group_comparing_run.bench_function("by_multi_threads_with_broadcast", |b| {
    //     b.iter(|| read_by_threads_with_broadcast_channels())
    // });
    // group_comparing_run.bench_function("by_multi_threads_with_mpsc", |b| {
    //     b.iter(|| read_by_threads_with_mpsc_channels())
    // });
    // group_comparing_run.bench_function("by_multi_threads", |b| {
    //     b.iter(|| read_by_threads_shared_data())
    // });
    // group_comparing_run.bench_function("by_multi_threads_with_btree", |b| b.iter(|| run()));

    // group_comparing_run.finish();

    let mut group_city_and_temp_parse = c.benchmark_group("city_and_temp_parse");

    group_city_and_temp_parse.bench_function("extract_city_temp_with_parser", |b| {
        b.iter(|| extract_city_temp_with_parser(black_box("Adana;32.1")))
    });

    group_city_and_temp_parse.bench_function("extract_city_temp", |b| {
        b.iter(|| extract_city_temp(black_box("Adana;32.1")))
    });

    group_city_and_temp_parse.finish();

    // c.bench_function("run()", |b| b.iter(|| run()));
}
criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
