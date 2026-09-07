#!/usr/bin/env bash
# Build the browser bundle of pgcache-fit and copy it into the Zola site.
#
# Needs: `rustup target add wasm32-unknown-emscripten`, emscripten (`emcc` on
# PATH, python3 >= 3.10), and a libclang built with the WebAssembly target
# (Xcode's is not; MacPorts clang-21 is).
#
#   fit/wasm/build.sh            # release build -> site/static/fit/
#   fit/wasm/build.sh debug
#   FIT_WASM_OUT=/tmp/x fit/wasm/build.sh
set -euo pipefail

here=$(cd "$(dirname "$0")" && pwd)
root=$(cd "$here/../.." && pwd)
out_dir=${FIT_WASM_OUT:-$root/../site/static/fit}
profile=${1:-release}

command -v emcc >/dev/null || { echo "emcc not on PATH" >&2; exit 1; }

# MacPorts' emcc shim runs whichever python3 is first on PATH; Xcode's is 3.9.
if ! python3 -c 'import sys; sys.exit(sys.version_info < (3, 10))'; then
    newer=$(ls /opt/local/bin/python3.1[0-9] 2>/dev/null | sort -V | tail -1 || true)
    [ -n "$newer" ] || { echo "emscripten needs python3 >= 3.10 on PATH" >&2; exit 1; }
    shim=$(mktemp -d)
    ln -s "$newer" "$shim/python3"
    export PATH="$shim:$PATH"
fi

if [ -z "${LIBCLANG_PATH:-}" ]; then
    LIBCLANG_PATH=$(ls -d /opt/local/libexec/llvm-*/lib 2>/dev/null | sort -V | tail -1 || true)
fi
if [ ! -e "${LIBCLANG_PATH:-/nonexistent}/libclang.dylib" ] && [ ! -e "${LIBCLANG_PATH:-/nonexistent}/libclang.so" ]; then
    echo "set LIBCLANG_PATH to a libclang built with the WebAssembly target" >&2
    exit 1
fi
export LIBCLANG_PATH

sysroot=${EMSCRIPTEN_SYSROOT:-$(em-config CACHE)/sysroot}
# clang defaults wasm32 functions to hidden visibility and bindgen silently
# drops hidden functions; the sysroot gives it libc headers for the triple.
export BINDGEN_EXTRA_CLANG_ARGS_wasm32_unknown_emscripten="--sysroot=$sysroot -isystem $sysroot/include -fvisibility=default"

# Rust links the emscripten target with wasm exceptions; libpg_query's
# setjmp/longjmp must be lowered the same way or emscripten_longjmp is
# undefined at link.
export CFLAGS_wasm32_unknown_emscripten="-fwasm-exceptions"

# ES module with an async factory, usable from a module Worker (and node for
# smoke.mjs). 8 MB stack: emscripten's 64 KB default overflows in the parser.
link_args=(
    -sMODULARIZE=1
    -sEXPORT_ES6=1
    -sEXPORT_NAME=createPgcacheFit
    -sENVIRONMENT=web,worker,node
    -sALLOW_MEMORY_GROWTH=1
    -sSTACK_SIZE=8388608
    -sINVOKE_RUN=0
    -sEXPORTED_FUNCTIONS=_fit_run,_fit_free,_malloc,_free
    -sEXPORTED_RUNTIME_METHODS=UTF8ToString,stringToUTF8,lengthBytesUTF8
)
RUSTFLAGS=""
for arg in "${link_args[@]}"; do RUSTFLAGS+=" -C link-arg=$arg"; done
export RUSTFLAGS

case "$profile" in
    release) cargo build --manifest-path "$root/Cargo.toml" -p pgcache-fit-wasm --target wasm32-unknown-emscripten --release ;;
    debug) cargo build --manifest-path "$root/Cargo.toml" -p pgcache-fit-wasm --target wasm32-unknown-emscripten ;;
    *) echo "profile must be release or debug" >&2; exit 1 ;;
esac

built="$root/target/wasm32-unknown-emscripten/$profile"
mkdir -p "$out_dir"
cp "$built/pgcache_fit_wasm.js" "$built/pgcache_fit_wasm.wasm" "$out_dir/"

if command -v node >/dev/null; then
    node "$here/smoke.mjs" "$out_dir/pgcache_fit_wasm.js"
fi
for f in "$out_dir"/pgcache_fit_wasm.js "$out_dir"/pgcache_fit_wasm.wasm; do
    printf '%s: %s bytes (%s gzipped)\n' "$(basename "$f")" "$(wc -c < "$f" | tr -d ' ')" "$(gzip -c "$f" | wc -c | tr -d ' ')"
done
