/**
 * ResultSet - typed, iterable query results.
 */

// eslint-disable-next-line @typescript-eslint/no-explicit-any
type AnyRow = any[];

export interface ResultSetOptions {
  columns: string[];
  rows: AnyRow[];
  rowCount?: number;
  totalCount?: number;
  truncated?: boolean;
  executionTimeMs?: number;
  rowProvenance?: string[];
  hasEphemeral?: boolean;
  ephemeralSources?: string[];
  warnings?: string[];
}

/**
 * Container for query results.
 * Supports iteration, indexing, and conversion to various formats.
 */
export class ResultSet implements Iterable<Record<string, unknown>> {
  readonly columns: string[];
  readonly rows: AnyRow[];
  readonly rowCount: number;
  readonly totalCount: number;
  readonly truncated: boolean;
  readonly executionTimeMs: number;
  readonly rowProvenance?: string[];
  hasEphemeral: boolean;
  ephemeralSources: string[];
  warnings: string[];

  constructor(opts: ResultSetOptions) {
    this.columns = opts.columns;
    this.rows = opts.rows;
    this.rowCount = opts.rowCount ?? opts.rows.length;
    this.totalCount = opts.totalCount ?? this.rowCount;
    this.truncated = opts.truncated ?? false;
    this.executionTimeMs = opts.executionTimeMs ?? 0;
    this.rowProvenance = opts.rowProvenance;
    this.hasEphemeral = opts.hasEphemeral ?? false;
    this.ephemeralSources = opts.ephemeralSources ?? [];
    this.warnings = opts.warnings ?? [];
  }

  /** Number of result rows. */
  get length(): number {
    return this.rowCount;
  }

  /** True if there are any results. */
  get isEmpty(): boolean {
    return this.rowCount === 0;
  }

  /** Get a single row as a keyed object. */
  get(index: number): Record<string, unknown> {
    return this.rowToObj(this.rows[index]);
  }

  /** Get the first row or undefined if empty. */
  first(): Record<string, unknown> | undefined {
    if (this.rows.length === 0) return undefined;
    return this.rowToObj(this.rows[0]);
  }

  /** Return the single value from a 1x1 result. */
  scalar(): unknown {
    if (this.rows.length === 0 || this.rows[0].length === 0) {
      throw new Error('No results to extract scalar from');
    }
    return this.rows[0][0];
  }

  /** Convert all rows to a list of keyed objects. */
  toDicts(): Array<Record<string, unknown>> {
    return this.rows.map((row) => this.rowToObj(row));
  }

  /** Convert all rows to a list of tuples (arrays). */
  toTuples(): AnyRow[] {
    return this.rows.map((row) => [...row]);
  }

  /** Iterate over rows as keyed objects. */
  [Symbol.iterator](): Iterator<Record<string, unknown>> {
    let i = 0;
    const self = this;
    return {
      next(): IteratorResult<Record<string, unknown>> {
        if (i < self.rows.length) {
          return { value: self.rowToObj(self.rows[i++]), done: false };
        }
        return { value: undefined, done: true };
      },
    };
  }

  private rowToObj(row: AnyRow): Record<string, unknown> {
    const obj: Record<string, unknown> = {};
    for (let i = 0; i < this.columns.length && i < row.length; i++) {
      obj[this.columns[i]] = row[i];
    }
    return obj;
  }
}
