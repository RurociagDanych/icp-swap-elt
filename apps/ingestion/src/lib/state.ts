import fs from 'node:fs';

export type CanisterState = {
  lastTotal: number;
  lastRun?: string;
  recentHashes: string[];
  completed?: boolean; // archival/full-complete marker
  // Newest tx timestamp (epoch SECONDS) seen in this canister; persisted so the canister↔REST
  // seam can be computed from all canisters even when a resume skips completed ones.
  maxTimestampSec?: number;
};

export type RestState = {
  backfillComplete?: boolean;
  backfillCursor?: { endSnapshot: number; nextPage: number };
  backfillFloor?: number;
  incrementalWatermark?: number;
  recentTxHashes: string[];
  // Written BEFORE an incremental file is committed and cleared after state is saved. If a
  // crash lands in between, the next run finds it and (if the file did commit) folds these
  // hashes/watermark in so the rows are not re-emitted.
  pendingIncremental?: PendingIncremental;
};

export type PendingIncremental = { runId: string; fileName: string; hashes: string[]; maxSeen: number };

export type IngestionState = {
  mode: 'full' | 'incremental' | 'canister' | 'backfill' | 'sync';
  lastRunAt?: string;
  latestStorageId?: string;
  canisters: Record<string, CanisterState>;
  canisterArchiveComplete?: boolean;
  canisterMaxTxTime?: number;
  rest?: RestState;
};

export const defaultState = (): IngestionState => ({
  mode: 'full',
  canisters: {},
});

export const defaultRestState = (): RestState => ({ recentTxHashes: [] });

// Cap on remembered tx hashes: must comfortably exceed the largest --overlap
// anyone would pass, while keeping state.json and run memory small.
export const RECENT_HASHES_LIMIT = 5000;

export function pushBounded(target: string[], values: string[], limit = RECENT_HASHES_LIMIT): void {
  for (const value of values) target.push(value);
  if (target.length > limit) target.splice(0, target.length - limit);
}

const MODES = ['full', 'incremental', 'canister', 'backfill', 'sync'];

function fail(what: string): never {
  throw new Error(`Invalid state: ${what}`);
}

const isObj = (v: unknown): v is Record<string, unknown> => typeof v === 'object' && v !== null && !Array.isArray(v);
const isNum = (v: unknown): v is number => typeof v === 'number' && Number.isFinite(v);
const isStrArr = (v: unknown): v is string[] => Array.isArray(v) && v.every((x) => typeof x === 'string');

function checkOptional(obj: Record<string, unknown>, key: string, ok: (v: unknown) => boolean, where: string): void {
  if (obj[key] !== undefined && !ok(obj[key])) fail(`${where}.${key} has the wrong type`);
}

function validateRest(rest: unknown): void {
  if (!isObj(rest)) fail('rest must be an object');
  if (!isStrArr(rest.recentTxHashes)) fail('rest.recentTxHashes must be a string array');
  checkOptional(rest, 'backfillComplete', (v) => typeof v === 'boolean', 'rest');
  checkOptional(rest, 'backfillFloor', isNum, 'rest');
  checkOptional(rest, 'incrementalWatermark', isNum, 'rest');
  const cursor = rest.backfillCursor;
  if (cursor !== undefined && !(isObj(cursor) && isNum(cursor.endSnapshot) && isNum(cursor.nextPage))) {
    fail('rest.backfillCursor must be {endSnapshot, nextPage}');
  }
  const pending = rest.pendingIncremental;
  if (pending !== undefined) {
    if (!(isObj(pending) && typeof pending.runId === 'string' && typeof pending.fileName === 'string'
      && isStrArr(pending.hashes) && isNum(pending.maxSeen))) {
      fail('rest.pendingIncremental must be {runId, fileName, hashes, maxSeen}');
    }
  }
}

function validateState(parsed: unknown): void {
  if (!isObj(parsed)) fail('top level must be an object');
  if (parsed.mode !== undefined && !MODES.includes(parsed.mode as string)) fail(`unknown mode ${String(parsed.mode)}`);
  checkOptional(parsed, 'lastRunAt', (v) => typeof v === 'string', 'state');
  checkOptional(parsed, 'latestStorageId', (v) => typeof v === 'string', 'state');
  checkOptional(parsed, 'canisterArchiveComplete', (v) => typeof v === 'boolean', 'state');
  checkOptional(parsed, 'canisterMaxTxTime', isNum, 'state');
  if (parsed.canisters !== undefined) {
    if (!isObj(parsed.canisters)) fail('canisters must be an object');
    for (const [id, c] of Object.entries(parsed.canisters)) {
      if (!isObj(c)) fail(`canisters.${id} must be an object`);
      if (!isNum(c.lastTotal)) fail(`canisters.${id}.lastTotal must be a number`);
      if (!isStrArr(c.recentHashes)) fail(`canisters.${id}.recentHashes must be a string array`);
      checkOptional(c, 'completed', (v) => typeof v === 'boolean', `canisters.${id}`);
      checkOptional(c, 'lastRun', (v) => typeof v === 'string', `canisters.${id}`);
      checkOptional(c, 'maxTimestampSec', isNum, `canisters.${id}`);
    }
  }
  if (parsed.rest !== undefined) validateRest(parsed.rest);
}

export function parseState(text: string): IngestionState {
  const raw: unknown = JSON.parse(text);
  validateState(raw);
  const parsed = raw as Partial<IngestionState>;
  const state: IngestionState = {
    ...defaultState(),
    ...parsed,
    canisters: parsed.canisters ?? {},
  };

  // backward compatibility from old schema
  const anyParsed = parsed as any;
  if (!parsed.canisters && anyParsed?.latestStorageId) {
    state.canisters[anyParsed.latestStorageId] = {
      lastTotal: Number(anyParsed.latestStorageTotal ?? 0),
      recentHashes: Array.isArray(anyParsed.recentHashes) ? anyParsed.recentHashes : [],
      lastRun: anyParsed.lastRunAt,
    };
  }

  return state;
}

export function loadState(path: string): IngestionState {
  if (!fs.existsSync(path)) return defaultState();
  return parseState(fs.readFileSync(path, 'utf8'));
}

// Write-then-rename so a crash never leaves a truncated state file behind.
export function saveState(path: string, state: IngestionState): void {
  const tmp = `${path}.${process.pid}.tmp`;
  fs.writeFileSync(tmp, `${JSON.stringify(state, null, 2)}\n`, 'utf8');
  fs.renameSync(tmp, path);
}
