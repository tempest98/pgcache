use std::hint::black_box;

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use pgcache_lib::query::ast::{TableNode, sql_query_convert};
use pgcache_lib::query::parse::query_where_clause_parse;

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
            BenchmarkId::new("sql_query_convert", name),
            &pg_ast,
            |b, ast| b.iter(|| black_box(sql_query_convert(black_box(ast)).unwrap())),
        );
    }

    // Benchmark just the WHERE clause parsing (our existing approach)
    for (name, sql) in &test_queries {
        let pg_ast = pg_query::parse(sql).expect("SQL should parse");

        group.bench_with_input(
            BenchmarkId::new("where_clause_parse", name),
            &pg_ast,
            |b, ast| b.iter(|| black_box(query_where_clause_parse(black_box(ast)).unwrap())),
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
        let pg_ast = pg_query::parse(sql).expect("SQL should parse");
        let ast = sql_query_convert(&pg_ast).unwrap();

        // Benchmark helper methods
        group.bench_with_input(BenchmarkId::new("is_single_table", name), &ast, |b, ast| {
            b.iter(|| black_box(ast.is_single_table()))
        });

        group.bench_with_input(
            BenchmarkId::new("has_where_clause", name),
            &ast,
            |b, ast| b.iter(|| black_box(ast.has_where_clause())),
        );

        group.bench_with_input(BenchmarkId::new("is_select_star", name), &ast, |b, ast| {
            b.iter(|| black_box(ast.is_select_star()))
        });

        group.bench_with_input(BenchmarkId::new("tables", name), &ast, |b, ast| {
            b.iter(|| black_box(ast.nodes::<TableNode>()))
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
        let pg_ast = pg_query::parse(sql).expect("SQL should parse");
        let ast = sql_query_convert(&pg_ast).unwrap();

        // Benchmark cloning our AST
        group.bench_with_input(BenchmarkId::new("ast_clone", name), &ast, |b, ast| {
            b.iter(|| black_box(ast.clone()))
        });
    }

    // Test deep vs shallow operations with a simpler but still complex query
    let complex_sql =
        "SELECT u.id, u.name FROM users u WHERE u.active = true AND u.verified = true";
    let complex_pg_ast = pg_query::parse(complex_sql).expect("SQL should parse");
    let complex_ast = sql_query_convert(&complex_pg_ast).unwrap();

    group.bench_function("complex_ast_clone", |b| {
        b.iter(|| black_box(complex_ast.clone()))
    });

    // Test partial cloning scenarios that might be common
    group.bench_function("ast_where_clause_clone", |b| {
        b.iter(|| {
            if let Some(where_clause) = complex_ast.where_clause() {
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
