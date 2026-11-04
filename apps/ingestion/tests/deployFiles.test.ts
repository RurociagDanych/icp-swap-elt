import assert from 'node:assert/strict';
import { test } from 'node:test';
import { spawnSync } from 'node:child_process';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

const appDir = path.resolve(import.meta.dirname, '..');
const read = (f: string) => fs.readFileSync(path.join(appDir, f), 'utf8');

test('M11: the image runs as the non-root node user and flags the unpinned base image', () => {
  const docker = read('Dockerfile');
  assert.match(docker, /^USER node$/m);
  // USER must come after the last COPY/RUN so build steps keep working, and before CMD.
  assert.ok(docker.lastIndexOf('USER node') > docker.lastIndexOf('RUN '));
  assert.ok(docker.lastIndexOf('USER node') < docker.indexOf('CMD '));
  assert.match(docker, /digest.*NOT VERIFIED/is);
});

test('L6: .dockerignore keeps env files, tests, scripts and terraform state out of the build context', () => {
  const entries = read('.dockerignore').split('\n').map((l) => l.trim());
  for (const e of ['.env', '.env.*', '*.env', 'tests', 'scripts', '.aws', '*.tfstate']) {
    assert.ok(entries.includes(e), `missing ${e}`);
  }
});

// Drives the real script with stub aws/docker/terraform binaries.
function runScript(args: string[], env: Record<string, string> = {}) {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'icp-script-'));
  const bin = path.join(dir, 'bin');
  const tf = path.join(dir, 'tf');
  fs.mkdirSync(bin);
  fs.mkdirSync(tf);
  const log = path.join(dir, 'calls.log');
  const stub = (name: string, body: string) => {
    fs.writeFileSync(path.join(bin, name), `#!/usr/bin/env bash\necho "${name} $*" >> "${log}"\n${body}\n`, { mode: 0o755 });
  };
  stub('terraform', `
case "$*" in
  *"output -json effective_subnet_ids"*) echo '["subnet-1"]';;
  *"output -raw aws_region"*) printf 'eu-central-1';;
  *"output -raw ecr_repository_url"*) printf '123.dkr.ecr.eu-central-1.amazonaws.com/repo';;
  *"output -raw assign_public_ip"*) printf 'true';;
  *) printf 'value';;
esac`);
  // Drain stdin so 'aws ... | docker login --password-stdin' never hits SIGPIPE under pipefail.
  stub('docker', 'cat >/dev/null 2>&1 || true');
  stub('aws', `
case "$*" in
  *get-login-password*) echo pw;;
  *run-task*) echo "$STUB_RUN_TASK_JSON";;
esac`);
  const res = spawnSync('bash', [path.join(appDir, 'scripts/aws_build_push_and_run.sh'), '--terraform-dir', tf, ...args], {
    encoding: 'utf8',
    env: { ...process.env, PATH: `${bin}:${process.env.PATH}`, STUB_RUN_TASK_JSON: '{"tasks":[{"taskArn":"t"}],"failures":[]}', ...env },
  });
  const calls = fs.existsSync(log) ? fs.readFileSync(log, 'utf8') : '';
  return { status: res.status, stdout: res.stdout, stderr: res.stderr, calls };
}

test('M12: docker build targets linux/amd64 by default and the platform is overridable', () => {
  const def = runScript(['--build-only']);
  assert.equal(def.status, 0, def.stderr);
  assert.match(def.calls, /docker build --platform linux\/amd64 /);
  const arm = runScript(['--build-only', '--platform', 'linux/arm64']);
  assert.match(arm.calls, /docker build --platform linux\/arm64 /);
});

test('L2: run-task failures make the script exit non-zero', () => {
  const bad = runScript(['--run-only'], {
    STUB_RUN_TASK_JSON: '{"tasks":[],"failures":[{"arn":"x","reason":"RESOURCE:FARGATE"}]}',
  });
  assert.notEqual(bad.status, 0);
  assert.match(bad.stderr, /RESOURCE:FARGATE/);
  const ok = runScript(['--run-only']);
  assert.equal(ok.status, 0, ok.stderr);
});

test('L2: numeric options and the tag are validated before anything runs', () => {
  const invalid = [
    ['--page-size', 'abc'], ['--page-size', '0'], ['--page-size', '1001'],
    ['--concurrency', '0'], ['--concurrency', '21'],
    ['--overlap', '-1'], ['--overlap', 'x'],
    ['--backfill-pages-per-file', '0'], ['--backfill-floor', '-5'], ['--backfill-floor', '1.5'],
    ['--tag', 'bad tag'],
  ];
  for (const opt of invalid) {
    const r = runScript(['--run-only', ...opt]);
    assert.notEqual(r.status, 0, `${opt.join(' ')} should be rejected`);
    assert.doesNotMatch(r.calls, /run-task/, `${opt.join(' ')} must fail before run-task`);
  }
});

test('L2: the mutable latest tag is flagged, --overlap is no longer forwarded to the app', () => {
  const r = runScript(['--run-only', '--mode', 'canister', '--overlap', '10']);
  assert.equal(r.status, 0, r.stderr);
  assert.match(r.stderr, /latest.*mutable|mutable.*latest/i);
  assert.match(r.stderr, /--overlap.*(deprecated|ignored)/i);
  assert.doesNotMatch(r.calls, /--overlap/);
});
