import fs from 'node:fs';
import path from 'node:path';

export type LogFields = Record<string, unknown>;

export type Logger = {
  info(msg: string, fields?: LogFields): void;
  warn(msg: string, fields?: LogFields): void;
  error(msg: string, fields?: LogFields): void;
};

const bigintSafe = (_key: string, value: unknown) => (typeof value === 'bigint' ? value.toString() : value);

export function createLogger(options: { base?: LogFields; logFile?: string; write?: (line: string) => void } = {}): Logger {
  const write = options.write ?? ((line: string) => process.stdout.write(line));
  if (options.logFile) fs.mkdirSync(path.dirname(options.logFile), { recursive: true });

  const emit = (level: 'info' | 'warn' | 'error', msg: string, fields?: LogFields) => {
    const line = `${JSON.stringify({ ts: new Date().toISOString(), level, msg, ...options.base, ...fields }, bigintSafe)}\n`;
    write(line);
    if (options.logFile) fs.appendFileSync(options.logFile, line);
  };

  return {
    info: (msg, fields) => emit('info', msg, fields),
    warn: (msg, fields) => emit('warn', msg, fields),
    error: (msg, fields) => emit('error', msg, fields),
  };
}
