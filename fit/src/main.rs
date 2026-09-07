//! pgcache-fit command line: reads the trace file and prints the report.

use std::path::PathBuf;

use anyhow::Context;
use clap::{Parser, Subcommand};
use pgcache_fit::hitrate::ReplayConfig;
use pgcache_fit::input::{TraceFormat, trace_format_detect};
use pgcache_fit::{Analysis, check_run, hitrate_run, report, trace_analyze};
use pgcache_lib::settings::DEFAULT_ADMISSION_THRESHOLD;

const OUT_OF_SCOPE: &str = "\
Runs pgcache's query-analysis pipeline offline, in schema-less mode: the \
catalog is synthesized from the query corpus itself and every report carries \
an explicit assumptions block.

Not simulated in v0 (planned extensions): write-driven invalidation, schema \
dump input (--schema), live-database catalogs, time-windowed hit rates.";

#[derive(Parser)]
#[command(name = "pgcache-fit", version, about, long_about = OUT_OF_SCOPE)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Classify statements as cacheable vs passthrough, with reasons
    Check {
        /// Query list or trace: .sql file, postgres log (csvlog/stderr), or
        /// pg_stat_statements CSV
        input: PathBuf,
        /// Emit the report (and per-statement verdicts) as JSON
        #[arg(long)]
        json: bool,
        /// List every statement grouped by verdict (cacheable, passthrough
        /// by reason, writes by table, utility)
        #[arg(long)]
        statements: bool,
        /// Override input format auto-detection
        #[arg(long, value_enum)]
        format: Option<TraceFormat>,
    },
    /// [experimental] Replay a trace and estimate the ceiling on hit rate
    /// under an infinite cache (write-driven invalidation not yet simulated)
    Hitrate {
        /// Statement trace: postgres log (csvlog/stderr) or .sql file
        input: PathBuf,
        /// Emit the report as JSON
        #[arg(long)]
        json: bool,
        /// Override input format auto-detection
        #[arg(long, value_enum)]
        format: Option<TraceFormat>,
        /// pgcache's admission_threshold: a query registers on its Nth
        /// sighting and is forwarded before that
        #[arg(long, default_value_t = DEFAULT_ADMISSION_THRESHOLD)]
        admission_threshold: u32,
    },
}

fn file_analyze(path: &PathBuf, format_override: Option<TraceFormat>) -> anyhow::Result<Analysis> {
    let content =
        std::fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
    let format = format_override.unwrap_or_else(|| trace_format_detect(path, &content));
    trace_analyze(&content, format).with_context(|| format!("analyzing {}", path.display()))
}

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Command::Check {
            input,
            json,
            statements,
            format,
        } => {
            let analysis = file_analyze(&input, format)?;
            let report = check_run(&analysis);
            if json {
                println!("{}", serde_json::to_string_pretty(&report)?);
            } else {
                print!("{}", report::check_report_render(&report, statements));
            }
        }
        Command::Hitrate {
            input,
            json,
            format,
            admission_threshold,
        } => {
            let analysis = file_analyze(&input, format)?;
            let config = ReplayConfig {
                admission_threshold,
            };
            let report = hitrate_run(&analysis, config)?;
            if json {
                println!("{}", serde_json::to_string_pretty(&report)?);
            } else {
                print!("{}", report::hitrate_report_render(&report));
            }
        }
    }
    Ok(())
}
