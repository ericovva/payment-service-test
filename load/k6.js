import http from 'k6/http';
import { check, sleep } from 'k6';
import exec from 'k6/execution';
import { Counter, Rate, Trend } from 'k6/metrics';

// One iteration = one request. Do not confuse ingress RPS with settled payments/s.
const rate = Number(__ENV.RATE || 100);
const duration = __ENV.DURATION || '30s';
const maxBacklog = Number(__ENV.MAX_BACKLOG || 5000);
if (!Number.isInteger(maxBacklog) || maxBacklog < 1) throw new Error('MAX_BACKLOG must be a positive integer');
const baseURL = (__ENV.BASE_URL || 'http://127.0.0.1:8000').replace(/\/$/, '');
const webhookURL = __ENV.WEBHOOK_URL || 'http://webhook:8080/payments';
const apiKey = __ENV.API_KEY;
const runID = __ENV.RUN_ID || `${Date.now()}-${Math.random().toString(16).slice(2)}`;

function hostOf(url) {
  // Deliberately allow a narrow URL syntax, never embedded credentials or IPv6 ambiguity.
  const match = /^https?:\/\/([a-zA-Z0-9.-]+)(?::[0-9]+)?(?:\/[^\s]*)?$/.exec(url);
  if (!match) throw new Error('Use an http(s) URL without credentials or ambiguous host syntax');
  return match[1].toLowerCase();
}
function validateTarget(url, allowedEnv, localHosts) {
  const host = hostOf(url);
  if (localHosts.includes(host)) return;
  const allowed = (__ENV[allowedEnv] || '').split(',').map((s) => s.trim().toLowerCase());
  if (__ENV.ALLOW_REMOTE_TARGETS !== 'true' || !allowed.includes(host)) {
    throw new Error(`Remote destination blocked: ${host}; explicitly authorize isolated targets`);
  }
}
validateTarget(baseURL, 'ALLOWED_API_HOSTS', ['localhost', '127.0.0.1', 'api']);
validateTarget(webhookURL, 'ALLOWED_WEBHOOK_HOSTS', ['localhost', '127.0.0.1', 'webhook']);
if (!apiKey || apiKey.length < 16) throw new Error('API_KEY (at least 16 characters) is required');
if (!Number.isInteger(rate) || rate < 10) throw new Error('RATE must be an integer >= 10');
if (rate > 1000 && __ENV.CONFIRM_HIGH_LOAD !== 'true') {
  throw new Error('RATE > 1000 needs CONFIRM_HIGH_LOAD=true on an isolated test environment');
}

const unexpectedErrors = new Rate('unexpected_errors');
const admittedDuration = new Trend('admitted_request_duration', true);
const admissionRejected = new Counter('admission_rejected');
const acceptedPayments = new Counter('accepted_payments');
const freshRate = Math.floor(rate * 0.7);
const replayRate = Math.floor(rate * 0.2);
const getRate = rate - freshRate - replayRate;
function scenario(fn, rps) {
  return {
    executor: 'constant-arrival-rate', exec: fn, rate: rps, timeUnit: '1s', duration,
    preAllocatedVUs: Number(__ENV.PREALLOCATED_VUS || Math.max(10, Math.ceil(rps * 0.1))),
    maxVUs: Number(__ENV.MAX_VUS || Math.max(50, Math.ceil(rps * 0.5))),
    gracefulStop: '10s',
  };
}
export const options = {
  scenarios: {
    fresh_payments: scenario('freshPayment', freshRate),
    idempotent_replays: scenario('replayPayment', replayRate),
    payment_reads: scenario('readPayment', getRate),
    backlog_control: {
      executor: 'constant-vus', exec: 'monitorBacklog', vus: 1, duration, gracefulStop: '1s',
      tags: { traffic: 'control' },
    },
  },
  thresholds: {
    unexpected_errors: ['rate<0.01'],
    admitted_request_duration: ['p(95)<200', 'p(99)<500'],
    'admitted_request_duration{scenario:fresh_payments}': ['p(95)<200', 'p(99)<500'],
    'admitted_request_duration{scenario:idempotent_replays}': ['p(95)<200', 'p(99)<500'],
    'admitted_request_duration{scenario:payment_reads}': ['p(95)<200', 'p(99)<500'],
    dropped_iterations: ['count==0'],
    // Capacity pass requires zero controlled rejections by default. Admission behavior
    // may be tested separately, but its successful run is NOT proof of capacity.
    admission_rejected: [`count<=${Number(__ENV.MAX_ADMISSION_REJECTIONS || 0)}`],
    checks: ['rate>0.99'],
  },
  discardResponseBodies: false,
};
const headers = { 'X-API-Key': apiKey, 'Content-Type': 'application/json' };
const body = JSON.stringify({
  amount: '10.00', currency: 'RUB', description: 'Isolated load test',
  metadata: { source: 'k6' }, webhook_url: webhookURL,
});
function post(key, requestName) {
  return http.post(`${baseURL}/api/v1/payments`, body, {
    headers: { ...headers, 'Idempotency-Key': key },
    tags: { name: requestName }, timeout: '5s', redirects: 0,
    responseCallback: http.expectedStatuses(202, 429),
  });
}
export function setup() {
  assertBacklog();
  const seedKey = `load-seed-${runID}`;
  const result = post(seedKey, 'setup_seed');
  if (result.status !== 202) {
    throw new Error(`Seed creation failed (status ${result.status}); check key/allowlist/admission`);
  }
  const id = result.json('payment_id');
  if (!id) throw new Error('Seed response lacks payment_id');
  return { seedKey, id, runID };
}
function record(result, expectedStatus, allowAdmission = false) {
  const rejected = allowAdmission && result.status === 429;
  const ok = result.status === expectedStatus;
  admissionRejected.add(rejected ? 1 : 0);
  unexpectedErrors.add(!ok && !rejected);
  if (ok) admittedDuration.add(result.timings.duration);
  check(result, { 'expected status or controlled admission rejection': () => ok || rejected });
}
export function freshPayment(data) {
  const key = `load-${data.runID}-${exec.scenario.iterationInTest}`;
  const result = post(key, 'POST fresh payment');
  record(result, 202, true);
  acceptedPayments.add(result.status === 202 ? 1 : 0);
}
export function replayPayment(data) {
  const result = post(data.seedKey, 'POST idempotent replay');
  // Replays are existing work and should not be blocked by new-payment admission.
  record(result, 202);
  if (result.status === 202) {
    check(result, { 'replay preserves payment ID': (r) => r.json('payment_id') === data.id });
  }
}
export function readPayment(data) {
  const result = http.get(`${baseURL}/api/v1/payments/${data.id}`, {
    headers, tags: { name: 'GET payment' }, timeout: '5s', redirects: 0,
    responseCallback: http.expectedStatuses(200),
  });
  record(result, 200);
}

function assertBacklog() {
  const response = http.get(`${baseURL}/internal/backlog`, {
    headers, tags: { name: 'GET backlog control', traffic: 'control' },
    timeout: '5s', redirects: 0, responseCallback: http.expectedStatuses(200),
  });
  if (response.status !== 200) {
    exec.test.abort(`Backlog safety monitor unavailable (HTTP ${response.status})`);
    return;
  }
  let snapshot;
  try { snapshot = response.json(); } catch (_) {
    exec.test.abort('Backlog safety monitor returned invalid JSON');
    return;
  }
  if (!Number.isFinite(snapshot.pending_payments) || !Number.isFinite(snapshot.pending_outbox)) {
    exec.test.abort('Backlog safety monitor lacks numeric counters');
    return;
  }
  if (snapshot.pending_payments > maxBacklog || snapshot.pending_outbox > maxBacklog) {
    exec.test.abort(`Backlog exceeded MAX_BACKLOG=${maxBacklog}; stopping new test traffic`);
  }
}
export function monitorBacklog() {
  assertBacklog();
  sleep(5);
}
