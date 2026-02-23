#![allow(clippy::unwrap_used)]

use std::hint::black_box;

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use ecow::EcoString;
use iddqd::BiHashMap;
use tokio_postgres::types::Type;

use pgcache_lib::catalog::{ColumnMetadata, TableMetadata};
use pgcache_lib::query::ast::{query_expr_convert, query_expr_fingerprint};
use pgcache_lib::query::transform::predicate_pushdown_apply;
use pgcache_lib::query::resolved::query_expr_resolve;

// ---------------------------------------------------------------------------
// Large query (real-world UNION subquery with window function, aggregates,
// ~65 output columns). Used to benchmark the resolve step where
// ResolvedColumnNode and ColumnMetadata are heavily cloned.
// ---------------------------------------------------------------------------

const LARGE_SQL: &str = r#"
SELECT
  acq_inst.id,
  acq_inst.num_j as num_aa,
  acq_inst.int_a,
  acq_inst.date_a,
  acq_inst.num_a,
  acq_inst.text_a,
  acq_inst.text_b,
  acq_inst.text_c,
  acq_inst.text_d,
  acq_inst.uuid_a,
  acq_inst.text_f,
  acq_inst.bool_f,
  acq_inst.uuid_c,
  acq_inst.uuid_d,
  acq_inst.num_h,
  acq_inst.uuid_b,
  acq_inst.bool_b,
  acq_inst.int_d,
  acq_inst.uuid_e,
  acq_inst.text_g,
  acq_inst.uuid_f,
  acq_inst.uuid_g,
  acq_inst.int_c,
  acq_inst.text_e,
  acq_inst.num_b,
  acq_inst.num_c,
  acq_inst.num_d,
  acq_inst.num_e,
  acq_inst.num_f,
  acq_inst.num_g,
  acq_inst.bool_a,
  acq_inst.uuid_h,
  acq_inst.date_b,
  acq_inst.date_c,
  acq_inst.date_d,
  acq_inst.num_i,
  acq_inst.uuid_i,
  acq_inst.timestamp_a,
  acq_inst.uuid_j,
  acq_inst.bool_c,
  acq_inst.text_h,
  acq_inst.bool_d,
  acq_inst.bool_e,
  acq_inst.text_i,
  acq_inst.text_j,
  acq_inst.text_n,
  acq_inst.text_o,
  acq_inst.text_p,
  acq_inst.text_q,
  acq_inst.text_r,
  acq_inst.text_s,
  acq_inst.text_t,
  acq_inst.timestamp_b,
  acq_inst.timestamp_c,
  acq_inst.timestamp_d,
  acq_inst.text_u,
  acq_inst.text_v,
  acq_inst.text_w,
  acq_inst.int_e,
  acq_inst.int_b,
  acq_inst.text_k,
  acq_inst.text_l,
  acq_inst.uuid_k,
  acq_inst.text_m,
  acq_inst.int_f,
  string_agg(
    distinct acq_inst.name,
    ', '
    order by
      acq_inst.name
  ) as name,
  count(distinct acq_inst.uuid_l) as int_g,
  false as bool_g,
  false as bool_h,
  sum(
    case
      when (
        acq_inst.text_f in ('ACQUITTED', 'CONCILIATED')
        or acq_inst.date_a >= date_trunc('day', now())
      )
      and acq_inst.text_c = 'REVENUE' then acq_inst.num_a
      when (
        acq_inst.text_f in ('ACQUITTED', 'CONCILIATED')
        or acq_inst.date_a >= date_trunc('day', now())
      )
      and acq_inst.text_c = 'EXPENSE' then acq_inst.num_a * -1
      else 0
    end
  ) over (
    order by
      acq_inst.date_a asc,
      acq_inst.text_f asc,
      acq_inst.timestamp_a asc,
      acq_inst.text_c asc,
      acq_inst.id asc
  ) as num_bal
from
  (
    select
      pay.id,
      null as int_a,
      pay.date_a,
      pay.num_a,
      'NONE' as text_a,
      'ACQUITTANCE' as text_b,
      ev.text_b as text_c,
      inst.text_a as text_d,
      ev.uuid_a,
      1 as int_b,
      inst.num_f as num_b,
      inst.num_c as num_c,
      inst.num_e as num_d,
      inst.num_d as num_e,
      inst.num_a as num_f,
      inst.num_b as num_g,
      true as bool_a,
      null as uuid_b,
      null as int_c,
      ev.text_a as text_e,
      pay.text_a as text_f,
      null as uuid_c,
      null as uuid_d,
      null as num_h,
      false as bool_b,
      null as int_d,
      ev.uuid_b as uuid_e,
      ent.name as text_g,
      null as uuid_f,
      ev.id as uuid_g,
      1 as int_e,
      pay.uuid_b as uuid_h,
      inst.date_a as date_b,
      ev.date_a as date_c,
      inst.date_b as date_d,
      null as num_i,
      pay.uuid_a as uuid_i,
      pay.timestamp_a,
      null as uuid_j,
      false as bool_c,
      ev.num_a as num_j,
      facc.text_a as text_h,
      false as bool_d,
      false as bool_e,
      pay.text_b as text_i,
      ev.text_c as text_j,
      null as text_k,
      null as text_l,
      null as text_m,
      null as uuid_k,
      null as text_n,
      null as text_o,
      null as text_p,
      null as text_q,
      null as text_r,
      null as text_s,
      null as text_t,
      null as timestamp_b,
      null as timestamp_c,
      null as timestamp_d,
      null as text_u,
      null as text_v,
      null as text_w,
      null as int_f,
      null as bool_f,
      cat.id as uuid_l,
      cat.name as name
    from
      tb_payments pay
      join tb_installments inst on inst.id = pay.uuid_a
      and inst.tenant_id = 1528
      join tb_events ev on ev.id = inst.uuid_a
      and ev.tenant_id = 1528
      join tb_event_categories ec on ec.uuid_a = ev.id
      and ec.tenant_id = 1528
      join tb_categories cat on cat.id = ec.uuid_b
      left join tb_accounts facc on pay.uuid_b = facc.id
      and facc.tenant_id = 1528
      left join tb_entities ent on ev.uuid_a = ent.id
      and ent.tenant_id = 1528
    where
      pay.tenant_id = 1528
      and pay.timestamp_b is null
      and inst.timestamp_a is null
      and ev.timestamp_b is null

    union

    select
      inst.id,
      null as int_a,
      inst.date_b as date_a,
      inst.num_g as num_a,
      'NONE' as text_a,
      'INSTALLMENT' as text_b,
      ev.text_b as text_c,
      inst.text_a as text_d,
      ev.uuid_a,
      1 as int_b,
      inst.num_f,
      inst.num_c,
      inst.num_e,
      inst.num_d,
      inst.num_a,
      inst.num_b,
      false as bool_a,
      null as uuid_b,
      null as int_c,
      ev.text_a as text_e,
      inst.text_b as text_f,
      null as uuid_c,
      null as uuid_d,
      null as num_h,
      false as bool_b,
      null as int_d,
      ev.uuid_b as uuid_e,
      ent.name as text_g,
      null as uuid_f,
      ev.id as uuid_g,
      1 as int_e,
      null as uuid_h,
      inst.date_a as date_b,
      ev.date_a as date_c,
      inst.date_b as date_d,
      null as num_i,
      null as uuid_i,
      inst.timestamp_b as timestamp_a,
      null as uuid_j,
      false as bool_c,
      ev.num_a as num_j,
      null as text_h,
      false as bool_d,
      false as bool_e,
      null as text_i,
      ev.text_c as text_j,
      null as text_k,
      null as text_l,
      null as text_m,
      null as uuid_k,
      null as text_n,
      null as text_o,
      null as text_p,
      null as text_q,
      null as text_r,
      null as text_s,
      null as text_t,
      null as timestamp_b,
      null as timestamp_c,
      null as timestamp_d,
      null as text_u,
      null as text_v,
      null as text_w,
      null as int_f,
      null as bool_f,
      cat.id as uuid_l,
      cat.name as name
    from
      tb_installments inst
      join tb_events ev on ev.id = inst.uuid_a
      and ev.tenant_id = 1528
      join tb_event_categories ec on ec.uuid_a = ev.id
      and ec.tenant_id = 1528
      join tb_categories cat on cat.id = ec.uuid_b
      left join tb_entities ent on ev.uuid_a = ent.id
      and ent.tenant_id = 1528
    where
      inst.tenant_id = 1528
      and inst.timestamp_a is null
      and ev.timestamp_b is null
      and (
        inst.num_g > 0
      )
  ) acq_inst
where
  1 = 1
  and acq_inst.date_a >= '2026-01-01'
  and acq_inst.date_a <= '2026-12-31'
group by
  acq_inst.id,
  acq_inst.num_j,
  acq_inst.int_a,
  acq_inst.date_a,
  acq_inst.num_a,
  acq_inst.text_a,
  acq_inst.text_b,
  acq_inst.text_c,
  acq_inst.text_d,
  acq_inst.uuid_a,
  acq_inst.text_f,
  acq_inst.bool_f,
  acq_inst.uuid_c,
  acq_inst.uuid_d,
  acq_inst.num_h,
  acq_inst.uuid_b,
  acq_inst.bool_b,
  acq_inst.int_d,
  acq_inst.uuid_e,
  acq_inst.text_g,
  acq_inst.uuid_g,
  acq_inst.uuid_f,
  acq_inst.int_c,
  acq_inst.text_e,
  acq_inst.num_b,
  acq_inst.num_c,
  acq_inst.num_d,
  acq_inst.num_e,
  acq_inst.num_f,
  acq_inst.num_g,
  acq_inst.bool_a,
  acq_inst.uuid_h,
  acq_inst.date_b,
  acq_inst.date_c,
  acq_inst.date_d,
  acq_inst.num_i,
  acq_inst.uuid_i,
  acq_inst.timestamp_a,
  acq_inst.uuid_j,
  acq_inst.bool_c,
  acq_inst.text_h,
  acq_inst.bool_d,
  acq_inst.bool_e,
  acq_inst.text_i,
  acq_inst.text_j,
  acq_inst.text_n,
  acq_inst.text_o,
  acq_inst.text_p,
  acq_inst.text_q,
  acq_inst.text_r,
  acq_inst.text_s,
  acq_inst.text_t,
  acq_inst.timestamp_b,
  acq_inst.timestamp_c,
  acq_inst.timestamp_d,
  acq_inst.text_u,
  acq_inst.text_v,
  acq_inst.text_w,
  acq_inst.int_e,
  acq_inst.int_b,
  acq_inst.text_k,
  acq_inst.text_l,
  acq_inst.uuid_k,
  acq_inst.text_m,
  acq_inst.int_f
order by
  acq_inst.date_a asc,
  acq_inst.text_f asc,
  acq_inst.timestamp_a asc
limit
  100 offset 0
"#;

// ---------------------------------------------------------------------------
// Test catalog
// ---------------------------------------------------------------------------

fn table_metadata_build(name: &str, oid: u32, column_defs: &[(&str, Type, bool)]) -> TableMetadata {
    let mut columns = BiHashMap::new();
    let mut pk_cols = Vec::new();
    for (i, &(col_name, ref data_type, is_pk)) in column_defs.iter().enumerate() {
        if is_pk {
            pk_cols.push(col_name.to_owned());
        }
        columns.insert_overwrite(ColumnMetadata {
            name: col_name.into(),
            position: (i + 1) as i16,
            type_oid: 25,
            data_type: data_type.clone(),
            type_name: "text".into(),
            cache_type_name: "text".into(),
            is_primary_key: is_pk,
        });
    }
    TableMetadata {
        relation_oid: oid,
        name: name.into(),
        schema: "public".into(),
        columns,
        primary_key_columns: pk_cols,
        indexes: Vec::new(),
    }
}

/// Build a catalog with users, orders, and products tables.
fn test_catalog_build() -> BiHashMap<TableMetadata> {
    let mut tables = BiHashMap::new();

    tables.insert_overwrite(table_metadata_build(
        "users",
        1001,
        &[
            ("id", Type::INT4, true),
            ("name", Type::TEXT, false),
            ("email", Type::TEXT, false),
            ("active", Type::BOOL, false),
        ],
    ));

    tables.insert_overwrite(table_metadata_build(
        "orders",
        1002,
        &[
            ("id", Type::INT4, true),
            ("user_id", Type::INT4, false),
            ("total", Type::NUMERIC, false),
            ("status", Type::TEXT, false),
        ],
    ));

    tables.insert_overwrite(table_metadata_build(
        "products",
        1003,
        &[
            ("id", Type::INT4, true),
            ("name", Type::TEXT, false),
            ("price", Type::NUMERIC, false),
        ],
    ));

    tables
}

/// Build a TableMetadata with per-column type names.
///
/// `cols` is `(column_name, postgres_type, type_name, is_primary_key)`.
/// `type_name` is stored verbatim as both `type_name` and `cache_type_name`,
/// which means long names like `"timestamp with time zone"` (24 bytes) will
/// exercise ecow's heap path post-migration.
fn typed_table_build(name: &str, oid: u32, cols: &[(&str, Type, &str, bool)]) -> TableMetadata {
    let mut columns = BiHashMap::new();
    let mut pk_cols = Vec::new();
    for (i, (col_name, data_type, type_name, is_pk)) in cols.iter().enumerate() {
        if *is_pk {
            pk_cols.push(col_name.to_string());
        }
        columns.insert_overwrite(ColumnMetadata {
            name: EcoString::from(*col_name),
            position: (i + 1) as i16,
            type_oid: 0,
            data_type: data_type.clone(),
            type_name: EcoString::from(*type_name),
            cache_type_name: EcoString::from(*type_name),
            is_primary_key: *is_pk,
        });
    }
    TableMetadata {
        relation_oid: oid,
        name: name.into(),
        schema: "public".into(),
        columns,
        primary_key_columns: pk_cols,
        indexes: Vec::new(),
    }
}

// type_name strings used in large_catalog_build.
// Strings > 15 bytes will land on ecow's refcounted heap post-migration.
const TN_UUID: &str = "uuid"; // 4 bytes
const TN_TEXT: &str = "text"; // 4 bytes
const TN_DATE: &str = "date"; // 4 bytes
const TN_NUMERIC: &str = "numeric"; // 7 bytes
const TN_INTEGER: &str = "integer"; // 7 bytes
const TN_TSTZ: &str = "timestamp with time zone"; // 24 bytes — exceeds ecow inline

/// Build a catalog matching the 7 tables referenced in LARGE_SQL.
///
/// Timestamp columns use `"timestamp with time zone"` (24 bytes) so the
/// benchmark exercises ecow's refcounted heap path alongside the inline path
/// used by shorter names like `"uuid"` or `"text"`.
fn large_catalog_build() -> BiHashMap<TableMetadata> {
    let mut tables = BiHashMap::new();

    tables.insert_overwrite(typed_table_build(
        "tb_payments",
        2001,
        &[
            ("id", Type::UUID, TN_UUID, true),
            ("date_a", Type::DATE, TN_DATE, false),
            ("num_a", Type::NUMERIC, TN_NUMERIC, false),
            ("text_a", Type::TEXT, TN_TEXT, false),
            ("uuid_a", Type::UUID, TN_UUID, false),
            ("uuid_b", Type::UUID, TN_UUID, false),
            ("text_b", Type::TEXT, TN_TEXT, false),
            ("timestamp_a", Type::TIMESTAMPTZ, TN_TSTZ, false),
            ("tenant_id", Type::INT4, TN_INTEGER, false),
            ("timestamp_b", Type::TIMESTAMPTZ, TN_TSTZ, false),
        ],
    ));

    tables.insert_overwrite(typed_table_build(
        "tb_installments",
        2002,
        &[
            ("id", Type::UUID, TN_UUID, true),
            ("uuid_a", Type::UUID, TN_UUID, false),
            ("tenant_id", Type::INT4, TN_INTEGER, false),
            ("text_a", Type::TEXT, TN_TEXT, false),
            ("num_f", Type::NUMERIC, TN_NUMERIC, false),
            ("num_c", Type::NUMERIC, TN_NUMERIC, false),
            ("num_e", Type::NUMERIC, TN_NUMERIC, false),
            ("num_d", Type::NUMERIC, TN_NUMERIC, false),
            ("num_a", Type::NUMERIC, TN_NUMERIC, false),
            ("num_b", Type::NUMERIC, TN_NUMERIC, false),
            ("date_a", Type::DATE, TN_DATE, false),
            ("date_b", Type::DATE, TN_DATE, false),
            ("timestamp_a", Type::TIMESTAMPTZ, TN_TSTZ, false),
            ("timestamp_b", Type::TIMESTAMPTZ, TN_TSTZ, false),
            ("num_g", Type::NUMERIC, TN_NUMERIC, false),
            ("text_b", Type::TEXT, TN_TEXT, false),
        ],
    ));

    tables.insert_overwrite(typed_table_build(
        "tb_events",
        2003,
        &[
            ("id", Type::UUID, TN_UUID, true),
            ("text_b", Type::TEXT, TN_TEXT, false),
            ("uuid_a", Type::UUID, TN_UUID, false),
            ("text_a", Type::TEXT, TN_TEXT, false),
            ("date_a", Type::DATE, TN_DATE, false),
            ("num_a", Type::NUMERIC, TN_NUMERIC, false),
            ("uuid_b", Type::UUID, TN_UUID, false),
            ("text_c", Type::TEXT, TN_TEXT, false),
            ("tenant_id", Type::INT4, TN_INTEGER, false),
            ("timestamp_b", Type::TIMESTAMPTZ, TN_TSTZ, false),
        ],
    ));

    tables.insert_overwrite(typed_table_build(
        "tb_event_categories",
        2004,
        &[
            ("uuid_a", Type::UUID, TN_UUID, false),
            ("uuid_b", Type::UUID, TN_UUID, false),
            ("tenant_id", Type::INT4, TN_INTEGER, false),
        ],
    ));

    tables.insert_overwrite(typed_table_build(
        "tb_categories",
        2005,
        &[
            ("id", Type::UUID, TN_UUID, true),
            ("name", Type::TEXT, TN_TEXT, false),
        ],
    ));

    tables.insert_overwrite(typed_table_build(
        "tb_accounts",
        2006,
        &[
            ("id", Type::UUID, TN_UUID, true),
            ("text_a", Type::TEXT, TN_TEXT, false),
            ("tenant_id", Type::INT4, TN_INTEGER, false),
        ],
    ));

    tables.insert_overwrite(typed_table_build(
        "tb_entities",
        2007,
        &[
            ("id", Type::UUID, TN_UUID, true),
            ("name", Type::TEXT, TN_TEXT, false),
            ("tenant_id", Type::INT4, TN_INTEGER, false),
        ],
    ));

    tables
}

// ---------------------------------------------------------------------------
// Query corpus
// ---------------------------------------------------------------------------

/// Representative queries at three complexity levels.
fn test_queries() -> Vec<(&'static str, &'static str)> {
    vec![
        // Simple: single table, basic WHERE
        (
            "simple",
            "SELECT id, name, email FROM users WHERE active = true AND id = 1",
        ),
        // Join: multi-table with alias and join conditions
        (
            "join",
            "SELECT u.id, u.name, o.total, o.status \
             FROM users u \
             JOIN orders o ON o.user_id = u.id \
             WHERE u.active = true AND o.status = 'pending'",
        ),
        // Subquery + UNION: pushdown-eligible
        (
            "union_subquery",
            "SELECT * FROM (\
                 SELECT id, name FROM users WHERE active = true \
                 UNION ALL \
                 SELECT id, name FROM products\
             ) sub WHERE sub.id = 1",
        ),
    ]
}

// ---------------------------------------------------------------------------
// Benchmarks
// ---------------------------------------------------------------------------

/// Parse: pg_query::parse + query_expr_convert (end-to-end SQL → AST)
fn bench_parse(c: &mut Criterion) {
    let mut group = c.benchmark_group("parse");

    for (label, sql) in test_queries() {
        group.bench_with_input(BenchmarkId::from_parameter(label), sql, |b, sql| {
            b.iter(|| {
                let pg = pg_query::parse(black_box(sql)).unwrap();
                black_box(query_expr_convert(&pg).unwrap())
            })
        });
    }

    group.finish();
}

/// Resolve: query_expr_resolve against catalog
fn bench_resolve(c: &mut Criterion) {
    let tables = test_catalog_build();
    let search_path = &["public"];

    let mut group = c.benchmark_group("resolve");

    for (label, sql) in test_queries() {
        let pg = pg_query::parse(sql).unwrap();
        let ast = query_expr_convert(&pg).unwrap();

        group.bench_with_input(BenchmarkId::from_parameter(label), &ast, |b, ast| {
            b.iter(|| black_box(query_expr_resolve(black_box(ast), &tables, search_path).unwrap()))
        });
    }

    group.finish();
}

/// Pushdown: predicate_pushdown_apply on resolved queries
fn bench_pushdown(c: &mut Criterion) {
    let tables = test_catalog_build();
    let search_path = &["public"];

    let mut group = c.benchmark_group("pushdown");

    for (label, sql) in test_queries() {
        let pg = pg_query::parse(sql).unwrap();
        let ast = query_expr_convert(&pg).unwrap();
        let resolved = query_expr_resolve(&ast, &tables, search_path).unwrap();

        group.bench_with_input(
            BenchmarkId::from_parameter(label),
            &resolved,
            |b, resolved| b.iter(|| black_box(predicate_pushdown_apply(resolved.clone()))),
        );
    }

    group.finish();
}

/// Fingerprint: query_expr_fingerprint on parsed ASTs
fn bench_fingerprint(c: &mut Criterion) {
    let mut group = c.benchmark_group("fingerprint");

    for (label, sql) in test_queries() {
        let pg = pg_query::parse(sql).unwrap();
        let ast = query_expr_convert(&pg).unwrap();

        group.bench_with_input(BenchmarkId::from_parameter(label), &ast, |b, ast| {
            b.iter(|| black_box(query_expr_fingerprint(black_box(ast))))
        });
    }

    group.finish();
}

/// Benchmark the full pipeline for LARGE_SQL as a single group.
///
/// Three sub-benchmarks in the "large" group:
/// - `parse`:   pg_query::parse + query_expr_convert (SQL bytes → AST)
/// - `resolve`: query_expr_resolve (AST + catalog → resolved tree)
/// - `pushdown`: predicate_pushdown_apply on a pre-resolved clone
///
/// The resolve step is the primary target for the SSO evaluation:
/// it creates one ResolvedColumnNode per output column (~65 here) and
/// clones ColumnMetadata for each, so it exercises the full String
/// allocation profile that ecow / compact_str aim to improve.
fn bench_large_query(c: &mut Criterion) {
    let tables = large_catalog_build();
    let search_path = &["public"];

    let mut group = c.benchmark_group("large");

    group.bench_function("parse", |b| {
        b.iter(|| {
            let pg = pg_query::parse(black_box(LARGE_SQL)).unwrap();
            black_box(query_expr_convert(&pg).unwrap())
        })
    });

    let pg = pg_query::parse(LARGE_SQL).unwrap();
    let ast = query_expr_convert(&pg).unwrap();

    group.bench_function("resolve", |b| {
        b.iter(|| black_box(query_expr_resolve(black_box(&ast), &tables, search_path).unwrap()))
    });

    let resolved = query_expr_resolve(&ast, &tables, search_path).unwrap();

    group.bench_function("pushdown", |b| {
        b.iter(|| black_box(predicate_pushdown_apply(resolved.clone())))
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_parse,
    bench_resolve,
    bench_pushdown,
    bench_fingerprint,
    bench_large_query,
);
criterion_main!(benches);
