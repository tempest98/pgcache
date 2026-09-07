//! Browser entry point for pgcache-fit: one JSON request in, one JSON
//! response out, over a C ABI called from emscripten's JS glue. See
//! `smoke.mjs` for the calling convention.

use std::ffi::{CStr, CString, c_char};
use std::panic::{self, AssertUnwindSafe};
use std::path::Path;

use pgcache_fit::hitrate::ReplayConfig;
use pgcache_fit::input::{TraceFormat, trace_format_detect};
use pgcache_fit::report::{check_report_render, hitrate_report_render};
use pgcache_fit::{check_run, hitrate_run, trace_analyze};
use pgcache_lib::settings::DEFAULT_ADMISSION_THRESHOLD;
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
enum Mode {
    Check,
    Hitrate,
}

#[derive(Deserialize)]
struct Request {
    mode: Mode,
    content: String,
    /// Used for format detection when `format` is absent (extension sniff).
    #[serde(default)]
    filename: String,
    #[serde(default)]
    format: Option<TraceFormat>,
    /// `check`: include the per-statement listing in `text`.
    #[serde(default)]
    statements: bool,
    /// `hitrate`: pgcache's admission_threshold.
    #[serde(default = "admission_threshold_default")]
    admission_threshold: u32,
}

fn admission_threshold_default() -> u32 {
    DEFAULT_ADMISSION_THRESHOLD
}

#[derive(Serialize)]
struct Response {
    ok: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    format: Option<TraceFormat>,
    #[serde(skip_serializing_if = "Option::is_none")]
    report: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    text: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

impl Response {
    fn error(message: String) -> Self {
        Response {
            ok: false,
            format: None,
            report: None,
            text: None,
            error: Some(message),
        }
    }
}

fn request_run(request: &Request) -> anyhow::Result<Response> {
    let format = request
        .format
        .unwrap_or_else(|| trace_format_detect(Path::new(&request.filename), &request.content));
    let analysis = trace_analyze(&request.content, format)?;
    let (report, text) = match request.mode {
        Mode::Check => {
            let report = check_run(&analysis);
            let text = check_report_render(&report, request.statements);
            (serde_json::to_value(&report)?, text)
        }
        Mode::Hitrate => {
            let config = ReplayConfig {
                admission_threshold: request.admission_threshold,
            };
            let report = hitrate_run(&analysis, config)?;
            let text = hitrate_report_render(&report);
            (serde_json::to_value(&report)?, text)
        }
    };
    Ok(Response {
        ok: true,
        format: Some(format),
        report: Some(report),
        text: Some(text),
        error: None,
    })
}

fn response_build(request_json: &str) -> Response {
    let request: Request = match serde_json::from_str(request_json) {
        Ok(request) => request,
        Err(e) => return Response::error(format!("invalid request: {e}")),
    };
    match panic::catch_unwind(AssertUnwindSafe(|| request_run(&request))) {
        Ok(Ok(response)) => response,
        Ok(Err(e)) => Response::error(format!("{e:#}")),
        Err(_) => Response::error("internal error: analysis panicked".to_owned()),
    }
}

/// Run one request. `request` is NUL-terminated UTF-8 JSON. The returned
/// buffer is NUL-terminated JSON owned by this module; release it with
/// [`fit_free`]. Never returns null.
///
/// # Safety
/// `request` must be null or point to a NUL-terminated string that outlives
/// the call.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn fit_run(request: *const c_char) -> *mut c_char {
    let request = if request.is_null() {
        Err("null request".to_owned())
    } else {
        unsafe { CStr::from_ptr(request) }
            .to_str()
            .map_err(|e| format!("invalid request: {e}"))
    };
    let response = match request {
        Ok(request) => response_build(request),
        Err(message) => Response::error(message),
    };
    let json = serde_json::to_string(&response)
        .unwrap_or_else(|_| r#"{"ok":false,"error":"response serialization failed"}"#.to_owned());
    // serde_json escapes U+0000, so the JSON never carries an interior NUL.
    CString::new(json).map_or_else(
        |_| CString::from(c"{\"ok\":false,\"error\":\"response contained NUL\"}").into_raw(),
        CString::into_raw,
    )
}

/// Release a buffer returned by [`fit_run`].
///
/// # Safety
/// `response` must be null or a pointer previously returned by [`fit_run`]
/// that has not been freed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn fit_free(response: *mut c_char) {
    if !response.is_null() {
        drop(unsafe { CString::from_raw(response) });
    }
}

fn main() {}
