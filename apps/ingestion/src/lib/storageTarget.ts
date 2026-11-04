import fs from 'node:fs';
import path from 'node:path';
import { DeleteObjectCommand, GetObjectCommand, HeadObjectCommand, PutObjectCommand, S3Client } from '@aws-sdk/client-s3';
import type { Config } from './config.js';
import { LocalCsvSink, S3CsvSink, type CsvSink, type SinkStats } from './csvSink.js';
import { withRetry, type WarnFn } from './retry.js';
import { defaultState, loadState, parseState, saveState, type IngestionState } from './state.js';

export type ManifestEntry = {
  storageId: string;
  total: number;
  written: number;
  sink: SinkStats;
};

export type Manifest = {
  schemaVersion: 1;
  runId: string;
  mode: Config['mode'];
  generatedAt: string;
  entries: ManifestEntry[];
  totals: { files: number; rows: number; bytes: number };
};

// A crashed run (e.g. SIGKILL mid-flight) leaves its lock behind; locks older
// than this are considered stale and stolen. Must exceed the longest plausible run.
export const LOCK_TTL_MS = 6 * 60 * 60 * 1000;
// A live run rewrites its lock's startedAt at this interval, so the TTL only ever expires
// locks of runs that stopped heartbeating (crashed), however long a healthy run takes.
export const LOCK_HEARTBEAT_MS = 15 * 60 * 1000;

export type TargetOptions = { heartbeatMs?: number; warn?: WarnFn };

function startHeartbeat(ms: number, tick: () => Promise<void>, warn: WarnFn): NodeJS.Timeout | undefined {
  if (!(ms > 0)) return undefined;
  const timer = setInterval(() => {
    tick().catch((err) => warn(`run lock heartbeat failed: ${String((err as Error)?.message || err)}`));
  }, ms);
  timer.unref();
  return timer;
}

function lockOwner(body: string | undefined): string | undefined {
  if (!body) return undefined;
  try {
    const runId = (JSON.parse(body) as Partial<RunLockBody>).runId;
    return typeof runId === 'string' ? runId : undefined;
  } catch {
    return undefined;
  }
}

type RunLockBody = { runId: string; startedAt: string };

function lockIsStale(body: string | undefined): boolean {
  if (!body) return true;
  try {
    const lock = JSON.parse(body) as Partial<RunLockBody>;
    const startedAt = Date.parse(lock.startedAt ?? '');
    return !Number.isFinite(startedAt) || Date.now() - startedAt > LOCK_TTL_MS;
  } catch {
    return true;
  }
}

export function modePathSegment(mode: Config['mode']): string {
  switch (mode) {
    case 'backfill': return 'rest/backfill';
    case 'incremental': return 'rest/incremental';
    case 'sync': return 'rest/incremental';
    default: return 'canister'; // 'canister' | 'full'
  }
}

export function buildManifest(cfg: Pick<Config, 'runId' | 'mode'>, entries: ManifestEntry[]): Manifest {
  return {
    schemaVersion: 1,
    runId: cfg.runId,
    mode: cfg.mode,
    generatedAt: new Date().toISOString(),
    entries,
    totals: {
      files: entries.length,
      rows: entries.reduce((s, e) => s + e.written, 0),
      bytes: entries.reduce((s, e) => s + e.sink.bytes, 0),
    },
  };
}

export interface StorageTarget {
  // Local full loads resume by skipping completed canisters; S3 runs write to a
  // fresh runId prefix, so skipping would leave holes in that run's file set.
  readonly supportsResumeSkip: boolean;
  loadState(): Promise<IngestionState>;
  saveState(state: IngestionState): Promise<void>;
  // `mode` is the EFFECTIVE mode of the caller (a sync run writes backfill and incremental
  // files); it selects the path segment. Defaults to the target's own mode.
  createSink(fileName: string, columns: readonly string[], mode?: Config['mode']): CsvSink;
  // True when the file a previous run (runId) wrote for `mode` was fully committed.
  hasCommittedFile(runId: string, fileName: string, mode: Config['mode']): Promise<boolean>;
  // S3 key prefixes this run created sinks under (empty for local runs).
  writtenPrefixes(): string[];
  writeManifest(manifest: Manifest): Promise<void>;
  acquireRunLock(): Promise<boolean>;
  // Heartbeat: extend the lock while we still own it; throws (and blocks later saves) if lost.
  refreshRunLock(): Promise<void>;
  // Deletes the lock only when its body still names this run.
  releaseRunLock(): Promise<void>;
}

export class LocalTarget implements StorageTarget {
  readonly supportsResumeSkip = true;
  private readonly outDir: string;
  private readonly stateFile: string;
  private readonly lockFile: string;
  private readonly runId: string;
  private readonly heartbeatMs: number;
  private readonly warn: WarnFn;
  private heartbeat?: NodeJS.Timeout;
  private lockLost = false;

  constructor(cfg: Pick<Config, 'outDir' | 'stateFile' | 'runId'>, opts: TargetOptions = {}) {
    this.outDir = cfg.outDir;
    this.stateFile = cfg.stateFile;
    this.lockFile = path.join(path.dirname(cfg.stateFile), 'run.lock');
    this.runId = cfg.runId;
    this.heartbeatMs = opts.heartbeatMs ?? LOCK_HEARTBEAT_MS;
    this.warn = opts.warn ?? console.warn;
    fs.mkdirSync(this.outDir, { recursive: true });
    fs.mkdirSync(path.dirname(this.stateFile), { recursive: true });
  }

  async loadState(): Promise<IngestionState> {
    return loadState(this.stateFile);
  }

  async saveState(state: IngestionState): Promise<void> {
    if (this.lockLost) throw new Error('Run lock was lost; refusing to write state');
    saveState(this.stateFile, state);
  }

  createSink(fileName: string, columns: readonly string[], _mode?: Config['mode']): CsvSink {
    return new LocalCsvSink(path.join(this.outDir, fileName), columns);
  }

  async hasCommittedFile(_runId: string, fileName: string, _mode: Config['mode']): Promise<boolean> {
    return fs.existsSync(path.join(this.outDir, fileName));
  }

  writtenPrefixes(): string[] {
    return [];
  }

  async writeManifest(manifest: Manifest): Promise<void> {
    const file = path.join(this.outDir, `manifest_${manifest.mode}_${manifest.runId}.json`);
    fs.writeFileSync(file, `${JSON.stringify(manifest, null, 2)}\n`);
  }

  private lockBody(): string {
    return JSON.stringify({ runId: this.runId, startedAt: new Date().toISOString() } satisfies RunLockBody);
  }

  async acquireRunLock(): Promise<boolean> {
    if (!this.createLock()) return false;
    this.heartbeat = startHeartbeat(this.heartbeatMs, () => this.refreshRunLock(), this.warn);
    return true;
  }

  private createLock(): boolean {
    try {
      fs.writeFileSync(this.lockFile, this.lockBody(), { flag: 'wx' });
      return true;
    } catch (err) {
      if ((err as NodeJS.ErrnoException).code !== 'EEXIST') throw err;
    }
    const existing = fs.existsSync(this.lockFile) ? fs.readFileSync(this.lockFile, 'utf8') : undefined;
    if (!lockIsStale(existing)) return false;

    // Steal by renaming the stale file away (atomic: only one racer can move it), confirm
    // what we moved is the stale lock we judged, then create ours exclusively.
    const aside = `${this.lockFile}.stale-${this.runId}-${process.pid}`;
    try {
      fs.renameSync(this.lockFile, aside);
    } catch (err) {
      if ((err as NodeJS.ErrnoException).code === 'ENOENT') return false;
      throw err;
    }
    const moved = fs.readFileSync(aside, 'utf8');
    if (moved !== existing) {
      // Someone replaced the stale lock between our read and rename: put theirs back.
      try { fs.linkSync(aside, this.lockFile); } catch { /* a newer lock already exists */ }
      fs.rmSync(aside, { force: true });
      return false;
    }
    fs.rmSync(aside, { force: true });
    try {
      fs.writeFileSync(this.lockFile, this.lockBody(), { flag: 'wx' });
      return true;
    } catch (err) {
      if ((err as NodeJS.ErrnoException).code === 'EEXIST') return false;
      throw err;
    }
  }

  async refreshRunLock(): Promise<void> {
    const body = fs.existsSync(this.lockFile) ? fs.readFileSync(this.lockFile, 'utf8') : undefined;
    if (lockOwner(body) !== this.runId) {
      this.lockLost = true;
      throw new Error(`Run lock lost: ${this.lockFile} is no longer owned by ${this.runId}`);
    }
    const tmp = `${this.lockFile}.${process.pid}.tmp`;
    fs.writeFileSync(tmp, this.lockBody());
    fs.renameSync(tmp, this.lockFile);
  }

  async releaseRunLock(): Promise<void> {
    if (this.heartbeat) clearInterval(this.heartbeat);
    this.heartbeat = undefined;
    const body = fs.existsSync(this.lockFile) ? fs.readFileSync(this.lockFile, 'utf8') : undefined;
    if (body === undefined) return;
    if (lockOwner(body) !== this.runId) {
      this.warn(`run lock belongs to another run; not deleting ${this.lockFile}`);
      return;
    }
    fs.rmSync(this.lockFile, { force: true });
  }
}

export class S3Target implements StorageTarget {
  readonly supportsResumeSkip = false;
  private readonly bucket: string;
  private readonly prefix: string;
  private readonly mode: Config['mode'];
  private readonly runId: string;
  private readonly heartbeatMs: number;
  private readonly prefixesWritten = new Set<string>();
  private heartbeat?: NodeJS.Timeout;
  private lockLost = false;

  constructor(
    private readonly s3: S3Client,
    cfg: Pick<Config, 's3Bucket' | 's3Prefix' | 'mode' | 'runId'>,
    private readonly warn: WarnFn = console.warn,
    opts: TargetOptions = {},
  ) {
    if (!cfg.s3Bucket) throw new Error('S3Target requires s3Bucket');
    this.bucket = cfg.s3Bucket;
    this.prefix = cfg.s3Prefix;
    this.mode = cfg.mode;
    this.runId = cfg.runId;
    this.heartbeatMs = opts.heartbeatMs ?? LOCK_HEARTBEAT_MS;
  }

  private get stateKey(): string {
    return `${this.prefix}/state/state.json`;
  }

  private get lockKey(): string {
    return `${this.prefix}/locks/run.lock`;
  }

  private runKey(fileName: string, mode: Config['mode'] = this.mode, runId: string = this.runId): string {
    return `${this.prefix}/${modePathSegment(mode)}/${runId}/${fileName}`;
  }

  // Missing key -> undefined; any other failure is retried (4xx other than 429 excepted).
  private async readObject(key: string): Promise<{ body: string; etag?: string } | undefined> {
    return withRetry(async () => {
      try {
        const res = await this.s3.send(new GetObjectCommand({ Bucket: this.bucket, Key: key }));
        return { body: (await res.Body?.transformToString()) ?? '', etag: res.ETag };
      } catch (err) {
        if ((err as { name?: string }).name === 'NoSuchKey') return undefined;
        throw err;
      }
    }, `read ${key}`, this.warn);
  }

  async loadState(): Promise<IngestionState> {
    const obj = await this.readObject(this.stateKey);
    if (!obj) return defaultState();
    // An existing-but-empty state object must not silently restart ingestion from scratch.
    if (obj.body.trim() === '') {
      throw new Error(`State object s3://${this.bucket}/${this.stateKey} exists but is empty; refusing to start from the default state`);
    }
    return parseState(obj.body);
  }

  async saveState(state: IngestionState): Promise<void> {
    if (this.lockLost) throw new Error('Run lock was lost; refusing to write state');
    const body = `${JSON.stringify(state, null, 2)}\n`;
    await withRetry(
      () =>
        this.s3.send(
          new PutObjectCommand({
            Bucket: this.bucket,
            Key: this.stateKey,
            Body: body,
            ContentType: 'application/json',
          })
        ),
      'saveState',
      this.warn
    );
  }

  createSink(fileName: string, columns: readonly string[], mode: Config['mode'] = this.mode): CsvSink {
    const key = this.runKey(fileName, mode);
    this.prefixesWritten.add(key.slice(0, key.lastIndexOf('/') + 1));
    return new S3CsvSink(this.s3, this.bucket, key, `s3://${this.bucket}/${key}`, columns);
  }

  async hasCommittedFile(runId: string, fileName: string, mode: Config['mode']): Promise<boolean> {
    const key = this.runKey(fileName, mode, runId);
    return withRetry(async () => {
      try {
        await this.s3.send(new HeadObjectCommand({ Bucket: this.bucket, Key: key }));
        return true;
      } catch (err) {
        const name = (err as { name?: string }).name;
        if (name === 'NotFound' || name === 'NoSuchKey') return false;
        throw err;
      }
    }, `head ${key}`, this.warn);
  }

  writtenPrefixes(): string[] {
    return [...this.prefixesWritten].sort();
  }

  async writeManifest(manifest: Manifest): Promise<void> {
    // The manifest sits next to the files of the mode it describes (a sync run has two).
    const key = this.runKey(`manifest_${manifest.mode}.json`, manifest.mode);
    await withRetry(
      () =>
        this.s3.send(
          new PutObjectCommand({
            Bucket: this.bucket,
            Key: key,
            Body: `${JSON.stringify(manifest, null, 2)}\n`,
            ContentType: 'application/json',
          })
        ),
      'writeManifest',
      this.warn
    );
  }

  private putLock(condition: { IfNoneMatch?: string; IfMatch?: string }) {
    const body = JSON.stringify({ runId: this.runId, startedAt: new Date().toISOString() } satisfies RunLockBody);
    return withRetry(
      () =>
        this.s3.send(
          new PutObjectCommand({
            Bucket: this.bucket,
            Key: this.lockKey,
            Body: body,
            ContentType: 'application/json',
            ...condition,
          })
        ),
      'put run lock',
      this.warn
    );
  }

  private static isPreconditionFailure(err: unknown): boolean {
    const name = (err as { name?: string }).name;
    return name === 'PreconditionFailed' || name === 'ConditionalRequestConflict';
  }

  async acquireRunLock(): Promise<boolean> {
    if (!(await this.tryAcquire())) return false;
    this.heartbeat = startHeartbeat(this.heartbeatMs, () => this.refreshRunLock(), this.warn);
    return true;
  }

  private async tryAcquire(): Promise<boolean> {
    for (let attempt = 0; attempt < 3; attempt++) {
      try {
        await this.putLock({ IfNoneMatch: '*' });
        return true;
      } catch (err) {
        if (!S3Target.isPreconditionFailure(err)) throw err;
      }
      const existing = await this.readObject(this.lockKey);
      if (!existing) continue; // released between our PUT and GET: try to create again
      if (!lockIsStale(existing.body)) return false;
      if (!existing.etag) throw new Error('Cannot steal a stale run lock safely: the lock object has no ETag');
      try {
        // Conditional on the exact stale object we read: if another run stole it first the
        // ETag no longer matches and we lose the race instead of overwriting their lock.
        await this.putLock({ IfMatch: existing.etag });
        return true;
      } catch (err) {
        if (S3Target.isPreconditionFailure(err)) return false;
        throw err;
      }
    }
    return false;
  }

  async refreshRunLock(): Promise<void> {
    const existing = await this.readObject(this.lockKey);
    if (!existing || lockOwner(existing.body) !== this.runId || !existing.etag) {
      this.lockLost = true;
      throw new Error(`Run lock lost: s3://${this.bucket}/${this.lockKey} is no longer owned by ${this.runId}`);
    }
    try {
      await this.putLock({ IfMatch: existing.etag });
    } catch (err) {
      if (S3Target.isPreconditionFailure(err)) {
        this.lockLost = true;
        throw new Error(`Run lock lost: s3://${this.bucket}/${this.lockKey} changed during refresh`);
      }
      throw err;
    }
  }

  async releaseRunLock(): Promise<void> {
    if (this.heartbeat) clearInterval(this.heartbeat);
    this.heartbeat = undefined;
    try {
      const existing = await this.readObject(this.lockKey);
      if (!existing) return;
      if (lockOwner(existing.body) !== this.runId) {
        this.warn(`run lock belongs to another run; not deleting s3://${this.bucket}/${this.lockKey}`);
        return;
      }
      await this.s3.send(new DeleteObjectCommand({
        Bucket: this.bucket,
        Key: this.lockKey,
        ...(existing.etag ? { IfMatch: existing.etag } : {}),
      }));
    } catch (err) {
      this.warn(`failed to release run lock: ${String((err as Error)?.message || err)}`);
    }
  }
}

export function createStorageTarget(cfg: Config, warn: WarnFn = console.warn): StorageTarget {
  return cfg.s3Bucket ? new S3Target(new S3Client({}), cfg, warn) : new LocalTarget(cfg, { warn });
}
