import { createHash } from 'node:crypto';
import { once } from 'node:events';
import fs from 'node:fs';
import { PassThrough } from 'node:stream';
import type { S3Client } from '@aws-sdk/client-s3';
import { Upload } from '@aws-sdk/lib-storage';

export type CsvValue = string | number;
export type CsvRecord = Record<string, CsvValue>;

export function escapeCsvValue(value: CsvValue): string {
  const text = String(value ?? '');
  if (/[",\r\n]/.test(text)) return `"${text.replace(/"/g, '""')}"`;
  return text;
}

export type SinkStats = {
  location: string;
  rows: number;
  bytes: number;
  sha256: string;
};

export interface CsvSink {
  location: string;
  append(rows: ReadonlyArray<CsvRecord>, writeHeaderIfNeeded?: boolean): Promise<void>;
  close(): Promise<SinkStats>;
}

abstract class BaseCsvSink implements CsvSink {
  private headerWritten = false;
  private rows = 0;
  private bytes = 0;
  private hash = createHash('sha256');
  private closed = false;

  constructor(public location: string, private readonly columns: readonly string[]) {}

  async append(rows: ReadonlyArray<CsvRecord>, writeHeaderIfNeeded = false): Promise<void> {
    this.ensureOpen();

    if (!this.headerWritten && writeHeaderIfNeeded) {
      await this.writeTrackedLine(`${this.columns.join(',')}\n`, true);
      this.headerWritten = true;
    }

    for (const row of rows) {
      const line = `${this.columns.map((c) => escapeCsvValue(row[c])).join(',')}\n`;
      await this.writeTrackedLine(line, false);
    }
  }

  async close(): Promise<SinkStats> {
    this.ensureOpen();
    if (!this.closed) {
      await this.finish();
      this.closed = true;
    }

    return {
      location: this.location,
      rows: this.rows,
      bytes: this.bytes,
      sha256: this.hash.digest('hex'),
    };
  }

  protected abstract writeRaw(line: string): Promise<void>;

  protected async finish(): Promise<void> {}

  private ensureOpen(): void {
    if (this.closed) {
      throw new Error(`Sink is already closed: ${this.location}`);
    }
  }

  private async writeTrackedLine(line: string, isHeader: boolean): Promise<void> {
    await this.writeRaw(line);
    if (!isHeader) this.rows += 1;
    this.bytes += Buffer.byteLength(line, 'utf8');
    this.hash.update(line, 'utf8');
  }
}

export class LocalCsvSink extends BaseCsvSink {
  private handle?: fs.promises.FileHandle;

  constructor(location: string, columns: readonly string[]) {
    super(location, columns);
  }

  // Data goes to a temp file that is renamed on close, so the final path only ever
  // holds a complete file (resume logic treats its existence as "committed").
  private get tempPath(): string {
    return `${this.location}.tmp`;
  }

  protected override async writeRaw(line: string): Promise<void> {
    if (!this.handle) {
      this.handle = await fs.promises.open(this.tempPath, 'w');
    }
    await this.handle.write(line, undefined, 'utf8');
  }

  protected override async finish(): Promise<void> {
    if (this.handle) {
      await this.handle.close();
      this.handle = undefined;
      await fs.promises.rename(this.tempPath, this.location);
    }
  }
}

export class StreamingCsvSink extends BaseCsvSink {
  private readonly stream = new PassThrough();
  private readonly uploadDone: Promise<void>;
  private readonly failureSignal: Promise<never>;
  private failed = false;
  private failure: unknown;

  constructor(location: string, columns: readonly string[], startUpload: (stream: PassThrough) => Promise<void>) {
    super(location, columns);
    // Destroying the stream with an error would emit 'error'; nobody else listens.
    this.stream.on('error', () => {});
    this.uploadDone = startUpload(this.stream);
    // Observe the upload outcome immediately so a failure can never be an unhandled
    // rejection, and so writers blocked on backpressure are released.
    this.failureSignal = new Promise<never>((_, reject) => {
      this.uploadDone.catch((err) => {
        this.failed = true;
        this.failure = err;
        this.stream.destroy();
        reject(err);
      });
    });
    this.failureSignal.catch(() => {});
  }

  protected override async writeRaw(line: string): Promise<void> {
    if (this.failed) throw this.failure;
    if (this.stream.write(line)) return;
    const stop = new AbortController();
    try {
      await Promise.race([once(this.stream, 'drain', { signal: stop.signal }), this.failureSignal]);
    } finally {
      stop.abort();
    }
  }

  protected override async finish(): Promise<void> {
    if (this.failed) throw this.failure;
    this.stream.end();
    await this.uploadDone;
  }
}

export class S3CsvSink extends StreamingCsvSink {
  constructor(s3: S3Client, bucket: string, key: string, location: string, columns: readonly string[]) {
    super(location, columns, async (stream) => {
      const upload = new Upload({
        client: s3,
        params: {
          Bucket: bucket,
          Key: key,
          Body: stream,
          ContentType: 'text/csv',
        },
      });

      await upload.done();
    });
  }
}
