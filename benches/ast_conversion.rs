#![allow(clippy::unwrap_used)]

use std::hint::black_box;

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use pgcache_lib::query::ast::{
    QueryBody, SelectColumns, SelectNode, TableNode, query_expr_convert,
};

/// Helper to extract SelectNode from a SQL string
fn parse_select_node(sql: &str) -> SelectNode {
    let pg_ast = pg_query::parse(sql).expect("SQL should parse");
    let query_expr = query_expr_convert(&pg_ast).unwrap();
    match query_expr.body {
        QueryBody::Select(node) => node,
        QueryBody::Values(_) | QueryBody::SetOp(_) => panic!("expected SELECT"),
    }
}

fn ast_conversion_benchmarks(c: &mut Criterion) {
    let mut group = c.benchmark_group("AST Conversion");

    // Test queries of varying complexity (using only supported WHERE clause patterns)
    let test_queries = vec![
        ("simple_select", "SELECT id FROM users"),
        ("select_star", "SELECT * FROM products"),
        // (
        //     "with_where",
        //     "SELECT id, name FROM users WHERE active = true",
        // ),
        // (
        //     "simple_and",
        //     "SELECT * FROM users WHERE name = 'john' AND active = true",
        // ),
        // (
        //     "with_alias",
        //     "SELECT u.id, u.name FROM users u WHERE u.active = true",
        // ),
        // (
        //     "column_alias",
        //     "SELECT id as user_id, name as full_name FROM users",
        // ),
        // ("qualified_table", "SELECT id FROM public.users"),
        // (
        //     "multiple_tables",
        //     "SELECT u.id, p.title FROM users u, posts p WHERE u.id = p.user_id",
        // ),
        // (
        //     "comparison_ops",
        //     "SELECT * FROM users WHERE age >= 18 AND score <= 100",
        // ),
        // (
        //     "simple_or",
        //     "SELECT * FROM users WHERE active = true OR verified = true",
        // ),
    ];

    // Benchmark our AST conversion
    for (name, sql) in &test_queries {
        let pg_ast = pg_query::parse(sql).expect("SQL should parse");

        group.bench_with_input(
            BenchmarkId::new("query_expr_convert", name),
            &pg_ast,
            |b, ast| b.iter(|| black_box(query_expr_convert(black_box(ast)).unwrap())),
        );
    }

    group.finish();
}

fn ast_helper_methods_benchmarks(c: &mut Criterion) {
    let mut group = c.benchmark_group("AST Helper Methods");

    let test_cases = vec![
        ("SELECT id FROM users", "simple_select"),
        (
            "SELECT u.id, u.name FROM users u WHERE u.active = true",
            "with_alias",
        ),
        (
            "SELECT * FROM users u, posts p WHERE u.id = p.user_id",
            "multiple_tables",
        ),
    ];

    for (sql, name) in &test_cases {
        let node = parse_select_node(sql);

        group.bench_with_input(
            BenchmarkId::new("is_single_table", name),
            &node,
            |b, node| b.iter(|| black_box(node.is_single_table())),
        );

        group.bench_with_input(
            BenchmarkId::new("has_where_clause", name),
            &node,
            |b, node| b.iter(|| black_box(node.where_clause.is_some())),
        );

        group.bench_with_input(
            BenchmarkId::new("is_select_star", name),
            &node,
            |b, node| b.iter(|| black_box(matches!(node.columns, SelectColumns::All))),
        );

        group.bench_with_input(BenchmarkId::new("tables", name), &node, |b, node| {
            b.iter(|| black_box(node.nodes::<TableNode>()))
        });
    }

    group.finish();
}

fn ast_cloning_benchmarks(c: &mut Criterion) {
    let mut group = c.benchmark_group("AST Cloning");

    let test_cases = vec![
        ("simple_select", "SELECT id FROM users"),
        ("select_star", "SELECT * FROM products"),
        // (
        //     "with_where",
        //     "SELECT id, name FROM users WHERE active = true",
        // ),
        // (
        //     "simple_and",
        //     "SELECT * FROM users WHERE name = 'john' AND active = true",
        // ),
        // (
        //     "with_alias",
        //     "SELECT u.id, u.name FROM users u WHERE u.active = true",
        // ),
        // (
        //     "column_alias",
        //     "SELECT id as user_id, name as full_name FROM users",
        // ),
        // (
        //     "simple_or",
        //     "SELECT * FROM users WHERE active = true OR verified = true",
        // ),
    ];

    for (name, sql) in &test_cases {
        let node = parse_select_node(sql);

        group.bench_with_input(
            BenchmarkId::new("select_node_clone", name),
            &node,
            |b, node| b.iter(|| black_box(node.clone())),
        );
    }

    // Test deep vs shallow operations with a simpler but still complex query
    let complex_sql =
        "SELECT u.id, u.name FROM users u WHERE u.active = true AND u.verified = true";
    let complex_node = parse_select_node(complex_sql);

    group.bench_function("complex_select_node_clone", |b| {
        b.iter(|| black_box(complex_node.clone()))
    });

    // Test partial cloning scenarios that might be common
    group.bench_function("where_clause_clone", |b| {
        b.iter(|| {
            if let Some(where_clause) = &complex_node.where_clause {
                black_box(where_clause.clone());
            }
        })
    });

    group.finish();
}

criterion_group!(
    benches,
    ast_conversion_benchmarks,
    ast_helper_methods_benchmarks,
    ast_cloning_benchmarks
);
criterion_main!(benches);
