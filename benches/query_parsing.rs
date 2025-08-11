use std::hint::black_box;

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};

fn query_parsing_benchmarks(c: &mut Criterion) {
    let mut group = c.benchmark_group("Query Parsing");

    let test_queries = vec![
        ("simple_select", "SELECT id FROM users"),
        (
            "cte_and_join",
            r"
            WITH jobs AS (
                SELECT id, job, product_id FROM job_queue WHERE is_active = true
            )
            SELECT *
            FROM products
            JOIN product ON product.id = products.product_id
            JOIN jobs ON jobs.product_id = product.id
        ",
        ),
    ];

    // Benchmark our AST conversion
    for (name, sql) in &test_queries {
        group.bench_with_input(BenchmarkId::new("sql_query_parse", name), sql, |b, &sql| {
            b.iter(|| black_box(pg_query::parse(black_box(sql)).expect("SQL should parse")))
        });
    }

    group.finish();
}

criterion_group!(benches, query_parsing_benchmarks);
criterion_main!(benches);
