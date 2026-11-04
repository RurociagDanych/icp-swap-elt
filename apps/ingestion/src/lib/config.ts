import path from 'node:path';

export type Config = {
  host: string;
  baseStorageCanisterId: string;
  pageSize: number;
  mode: 'canister' | 'backfill' | 'incremental' | 'sync' | 'full';
  outDir: string;
  stateFile: string;
  logFile: string;
  concurrency: number;
  s3Bucket?: string;
  s3Prefix: string;
  runId: string;
  restBaseUrl: string;
  restPageSize: number;
  backfillOverlapMs: number;
  incrementalOverlapMs: number;
  backfillFloor?: number;
  backfillPagesPerFile: number;
  actionTypes: string;
  // Event bus that receives IngestionRunFinished at the end of an S3 run; unset = no event.
  eventBus?: string;
  // Non-fatal configuration notices; the entrypoint logs them at warn level.
  warnings: string[];
};

export const MODES = ['sync', 'canister', 'full', 'backfill', 'incremental'] as const;

export const KNOWN_FLAGS = [
  'help', 'no-event-bus', 'mode', 'host', 'base-storage', 'page-size', 'overlap', 'concurrency',
  's3-bucket', 's3-prefix', 'out-dir', 'state-file', 'log-file',
  'rest-base-url', 'rest-page-size', 'backfill-overlap-ms', 'incremental-overlap-ms',
  'backfill-floor', 'backfill-pages-per-file', 'action-types', 'event-bus',
] as const;

export const USAGE = `Usage: node dist/index.js [--mode sync|canister|full|backfill|incremental] [options]

Options (see README.md "CLI (ingestion)" for defaults and ranges):
  --host, --base-storage, --page-size, --overlap, --concurrency        canister mode
  --rest-base-url, --rest-page-size, --action-types                    REST API
  --backfill-overlap-ms, --incremental-overlap-ms, --backfill-floor,
  --backfill-pages-per-file                                            backfill / incremental
  --s3-bucket, --s3-prefix                                             S3 target (omit for local output)
  --event-bus                                                          EventBridge bus for IngestionRunFinished (env EVENT_BUS_NAME); required with --s3-bucket
  --no-event-bus                                                       explicit opt-out: S3 run without announcing the run
  --out-dir, --state-file, --log-file                                  local paths
  --help                                                               print this text and exit
`;

export function parseArgs(args: string[] = process.argv.slice(2)): Config {
  for (const a of args) {
    if (a.startsWith('--') && !(KNOWN_FLAGS as readonly string[]).includes(a.slice(2))) {
      throw new Error(`Unknown flag ${a}. Run with --help for the list of flags.`);
    }
  }
  const get = (name: string, fallback?: string) => {
    const idx = args.findIndex((a) => a === `--${name}`);
    if (idx === -1) return fallback;
    const value = args[idx + 1];
    if (value === undefined || value.startsWith('--')) {
      throw new Error(`Missing value for --${name}`);
    }
    return value;
  };
  const warnings: string[] = [];

  const modeRaw = get('mode', 'sync') || 'sync';
  if (!(MODES as readonly string[]).includes(modeRaw)) {
    throw new Error(`Invalid --mode=${modeRaw}. Allowed: ${MODES.join(', ')}`);
  }
  const mode = modeRaw as Config['mode'];
  const s3Bucket = get('s3-bucket', process.env.S3_BUCKET);
  const s3PrefixRaw = get('s3-prefix', process.env.S3_PREFIX || 'icp-swap-elt')!;
  // Keys are built as `${prefix}/...`, so strip surrounding slashes to avoid `//` keys.
  const s3Prefix = s3PrefixRaw.replace(/^\/+|\/+$/g, '');
  if (!s3Prefix || s3Prefix.includes('//')) {
    throw new Error(`Invalid --s3-prefix=${s3PrefixRaw}. Must be a non-empty key prefix without empty path segments`);
  }
  const runId = new Date().toISOString().replace(/[:.]/g, '-');

  const pageSize = Number(get('page-size', '1000'));
  const overlap = Number(get('overlap', '50'));
  if (args.includes('--overlap')) {
    warnings.push('--overlap is deprecated and ignored: the canister archive is a one-time full load and has no overlap re-fetch');
  }
  const concurrency = Number(get('concurrency', '5'));

  const restPageSize = Number(get('rest-page-size', '100'));
  const backfillOverlapMs = Number(get('backfill-overlap-ms', '3600000'));
  const incrementalOverlapMs = Number(get('incremental-overlap-ms', '300000'));
  const backfillFloorRaw = get('backfill-floor');
  const backfillFloor = backfillFloorRaw === undefined ? undefined : Number(backfillFloorRaw);
  const backfillPagesPerFile = Number(get('backfill-pages-per-file', '50'));

  if (!Number.isFinite(pageSize) || pageSize < 1 || pageSize > 1000) {
    throw new Error(`Invalid --page-size=${pageSize}. Allowed range: 1..1000`);
  }
  if (!Number.isFinite(overlap) || overlap < 0) {
    throw new Error(`Invalid --overlap=${overlap}. Must be >= 0`);
  }
  if (!Number.isFinite(concurrency) || concurrency < 1 || concurrency > 20) {
    throw new Error(`Invalid --concurrency=${concurrency}. Allowed range: 1..20`);
  }
  if (!Number.isFinite(restPageSize) || restPageSize < 1 || restPageSize > 100) {
    throw new Error(`Invalid --rest-page-size=${restPageSize}. Allowed range: 1..100`);
  }
  if (!Number.isFinite(backfillOverlapMs) || backfillOverlapMs < 0) {
    throw new Error(`Invalid --backfill-overlap-ms=${backfillOverlapMs}. Must be >= 0`);
  }
  if (!Number.isFinite(incrementalOverlapMs) || incrementalOverlapMs < 0) {
    throw new Error(`Invalid --incremental-overlap-ms=${incrementalOverlapMs}. Must be >= 0`);
  }
  if (backfillFloor !== undefined && (!Number.isFinite(backfillFloor) || backfillFloor < 0)) {
    throw new Error(`Invalid --backfill-floor=${backfillFloorRaw}. Must be a non-negative epoch-ms`);
  }
  if (!Number.isFinite(backfillPagesPerFile) || backfillPagesPerFile < 1) {
    throw new Error(`Invalid --backfill-pages-per-file=${backfillPagesPerFile}. Must be >= 1`);
  }

  const eventBus = get('event-bus', process.env.EVENT_BUS_NAME) || undefined;
  const noEventBus = args.includes('--no-event-bus');
  if (noEventBus && eventBus) {
    throw new Error('--no-event-bus conflicts with --event-bus / EVENT_BUS_NAME');
  }
  if (s3Bucket && !eventBus) {
    if (!noEventBus) {
      throw new Error(
        'An S3 run needs an event bus to announce its output to the transformation stage: set --event-bus (or EVENT_BUS_NAME), or pass --no-event-bus to opt out explicitly'
      );
    }
    warnings.push('--no-event-bus: this S3 run will NOT announce its output; the transformation stage will not be triggered');
  }

  return {
    host: get('host', 'https://ic0.app')!,
    baseStorageCanisterId: get('base-storage', 'g54jq-hiaaa-aaaag-qck5q-cai')!,
    pageSize,
    mode,
    outDir: path.resolve(get('out-dir', './out')!),
    stateFile: path.resolve(get('state-file', './out/state.json')!),
    logFile: path.resolve(get('log-file', './out/ingestion.log')!),
    concurrency,
    s3Bucket: s3Bucket || undefined,
    s3Prefix,
    runId,
    restBaseUrl: get('rest-base-url', 'https://api.icpswap.com/info')!,
    restPageSize,
    backfillOverlapMs,
    incrementalOverlapMs,
    backfillFloor,
    backfillPagesPerFile,
    actionTypes: get('action-types', 'Swap,AddLiquidity,DecreaseLiquidity,Claim')!,
    eventBus,
    warnings,
  };
}
