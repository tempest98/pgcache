// Node smoke test for the emscripten bundle: node smoke.mjs <path/to/pgcache_fit_wasm.js>
// Also documents the calling convention the site's worker uses.
import { pathToFileURL } from 'node:url';

const modulePath = process.argv[2];
if (!modulePath) {
  console.error('usage: node smoke.mjs <pgcache_fit_wasm.js>');
  process.exit(2);
}
const { default: createPgcacheFit } = await import(pathToFileURL(modulePath));
const fit = await createPgcacheFit();

function fitRun(request) {
  const json = JSON.stringify(request);
  const bytes = fit.lengthBytesUTF8(json) + 1;
  const input = fit._malloc(bytes);
  fit.stringToUTF8(json, input, bytes);
  const output = fit._fit_run(input);
  fit._free(input);
  const response = JSON.parse(fit.UTF8ToString(output));
  fit._fit_free(output);
  return response;
}

const content = [
  'SELECT id, name FROM users WHERE id = 1042;',
  'SELECT id, name FROM users WHERE id = 1043;',
  'SELECT now();',
  'UPDATE users SET name = $1 WHERE id = 7;',
  'BEGIN;',
].join('\n');

// Errors first: libpg_query recovers via longjmp, and the module must stay
// usable afterwards.
const bad = fitRun({ mode: 'check', content: 'SELECT FROM WHERE (;', filename: 'bad.sql' });
const empty = fitRun({ mode: 'check', content: '-- nothing', filename: 'empty.sql' });
const malformed = fitRun({ mode: 'nope', content: '' });
const check = fitRun({ mode: 'check', content, filename: 'trace.sql', statements: true });
const hitrate = fitRun({ mode: 'hitrate', content, filename: 'trace.sql', admission_threshold: 1 });

const failures = [];
if (!check.ok || check.format !== 'sql' || !check.text.startsWith('pgcache-fit check')) failures.push(['check', check]);
if (!hitrate.ok || !hitrate.text.startsWith('pgcache-fit hitrate')) failures.push(['hitrate', hitrate]);
if (typeof bad.ok !== 'boolean') failures.push(['bad-sql', bad]);
if (empty.ok || !/no statements found/.test(empty.error)) failures.push(['empty', empty]);
if (malformed.ok || !/invalid request/.test(malformed.error)) failures.push(['malformed', malformed]);
for (const [name, response] of failures) console.error('FAIL', name, JSON.stringify(response).slice(0, 400));
if (failures.length) process.exit(1);
console.log('smoke ok:', check.text.split('\n')[0], '|', hitrate.text.split('\n')[0]);
