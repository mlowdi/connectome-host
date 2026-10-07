import { constants, openSync, closeSync, readFileSync, readSync, fstatSync, lstatSync, writeFileSync, mkdirSync, type BigIntStats } from 'node:fs';
import { createHash } from 'node:crypto';
import { dirname, resolve, join } from 'node:path';

export const CUTOFF = '2026-10-04T22:00:00.000Z';
export type Decision = 'include' | 'exclude' | 'candidate' | 'quarantine';
export interface SourceDate { start: string; end?: string; precision: 'day' | 'range' | 'millisecond' }
export interface SourceSpec {
  id: string; kind: 'note' | 'claude-code' | 'omp' | 'handoff'; path: string;
  scope: string; decision: Decision; reason: string; timezone: string;
  sessionId?: string; assistantAliases?: string[]; author?: string; date?: SourceDate;
  coveredDateRange?: SourceDate; branchHeads?: string[]; recordIds?: string[];
  mediaRoot?: string; active?: boolean;
}
export interface ExclusionDecision {
  version: 1; originFingerprint: string; originSessionId: string;
  selectedSummaryIds: string[]; eventKeys: string[]; reason: string;
  rawSourceCount?: number; logicalEventCount?: number;
  wholeEventExclusionHasNoAdditionalNativeShards?: boolean; newCandidateRequired?: boolean;
  existingSummariesMustNotBeReusedWithoutCompleteDependencyEvidence?: boolean;
  missingAcceptedPreimageCount?: number; reviewProseMustNotEnterLiveContext?: boolean;
}
export interface FilteredManifest {
  version: 1; originFingerprint: string; originSessionId: string;
  decisionHash: string; omissions: 'omissions.json'; lineage: 'derivation.json';
}
export interface SourceManifest { version: 1; cutoff: typeof CUTOFF; sources: SourceSpec[]; derivation?: FilteredManifest }
export interface EventLineage {
  key: string; payloadHash: string; source: string; recordIds: string[]; sourceLines: number[];
}
export interface OmissionReceipt {
  version: 1; originFingerprint: string; originSessionId: string; decisionHash: string; decision: ExclusionDecision;
  events: Array<EventLineage & { dispositionIndexes: number[] }>;
  records: Array<{ index: number; source: string; recordId: string; line: number; hash: string; previousState: string }>;
  excludedLogicalEvents: number; excludedDispositionRecords: number; contributingSources: number;
}
export interface DerivationReceipt {
  version: 1;
  origin: { fingerprint: string; sessionId: string; eventCount: number; hashes: Record<string, string> };
  events: EventLineage[];
  sources: Array<{ id: string; originEventCount: number; retainedEventCount: number; excludedEventCount: number }>;
  originDispositionsCanonicalHash: string;
  retainedEligibleEvents: number; excludedLogicalEvents: number; excludedDispositionRecords: number;
  originals: 'parent-only'; nativeState: 'fresh-empty'; summaryReuse: false;
}
export interface MediaReference {
  kind: string; mime: string; hash?: string; bytes?: number; snapshot?: string;
  status: 'validated' | 'missing' | 'unsupported' | 'invalid'; recordId: string; block: string;
}
export interface Disposition {
  source: string; recordId: string; line: number; hash: string;
  state: string; reason: string; blocks: Array<{ index: string; hash: string; state: string }>;
  exclusion?: { receipt: 'omissions.json'; decisionHash: string; eventKeys: string[]; previousState: string; previousReason: string };
}
export interface HistoricalEvent {
  key: string; payloadHash: string; source: string; kind: SourceSpec['kind'];
  recordIds: string[]; sourceLines: number[]; originalTimestamp: unknown;
  timestampMs: number; precision: string; timezone: string; indexingAnchor: string;
  authoredDate?: SourceDate; coveredDateRange?: SourceDate;
  role: string; voice: string; participant: string; parentId: string | null;
  streamId: string; observedStreamRecordIds: string[]; partialStream: boolean;
  branchHeads: string[]; selectedHeads: string[]; sourceBranch?: string; outcome: string;
  ancestryGap: boolean; parentInCorpus: boolean; gapReason: string; provenance: string; tools: Array<{ kind: string; id: string; name?: string }>;
  media: MediaReference[]; text: string;
}
export interface SourceSnapshot {
  id: string; kind: string; path: string; scope: string; sessionId?: string; sourceBranch?: string; decision: Decision; reason: string;
  hash?: string; snapshot?: string; bytes?: number; byteEnd?: number; lineEnd?: number;
  inode?: string; active?: boolean; lastId?: string; lastParentId?: string | null; partialTailBytes?: number; partialTailHash?: string; partialTailSnapshot?: string;
  recordCount: number; eventCount: number; projectionCount: number; error?: string;
  lineage?: { originFingerprint: string; originSessionId: string; originEventCount: number; excludedEventCount: number; originals: 'parent-only' };
}
export const sha256 = (value: string | Buffer): string => createHash('sha256').update(value).digest('hex');
export function canonical(value: unknown): string {
  if (Array.isArray(value)) return `[${value.map(canonical).join(',')}]`;
  if (value && typeof value === 'object') return `{${Object.entries(value).filter(([, v]) => v !== undefined).sort(([a], [b]) => a.localeCompare(b)).map(([k, v]) => `${JSON.stringify(k)}:${canonical(v)}`).join(',')}}`;
  return JSON.stringify(value) ?? 'null';
}
export const xml = (value: string): string => value.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;').replace(/'/g, '&apos;');
const object = (v: unknown): Record<string, unknown> => v && typeof v === 'object' && !Array.isArray(v) ? v as Record<string, unknown> : {};
const parentOf = (record: Record<string, unknown>): string | null => {
  const value = record.parentUuid ?? record.parentId;
  return typeof value === 'string' ? value : null;
};
const hasMalformedParent = (record: Record<string, unknown>): boolean => ['parentUuid', 'parentId'].some(key =>
  Object.hasOwn(record, key) && record[key] !== null && (typeof record[key] !== 'string' || !record[key].trim()));

/** No environment interpolation, globs, directory walks or implicit eligibility. */
export function loadManifest(path: string): SourceManifest {
  const raw = JSON.parse(readFileSync(path, 'utf8'));
  if (raw?.derivation !== undefined) throw new Error('Derived manifests cannot be raw-prepared; derive the complete reviewed union from the immutable original parent');
  if (raw.version !== 1 || raw.cutoff !== CUTOFF || !Array.isArray(raw.sources)) throw new Error('Expected version 1 manifest with the fixed raw-dialogue cutoff');
  const ids = new Set<string>(); const paths = new Set<string>();
  for (const source of raw.sources as SourceSpec[]) {
    if (!/^[A-Za-z0-9_.-]+$/.test(source.id) || ids.has(source.id)) throw new Error('Source ids must be unique safe names');
    ids.add(source.id);
    if (!['note', 'claude-code', 'omp', 'handoff'].includes(source.kind) || !['include', 'exclude', 'candidate', 'quarantine'].includes(source.decision)) throw new Error(`Unsupported manifest entry ${source.id}`);
    if (![source.path, source.scope, source.reason, source.timezone].every(v => typeof v === 'string' && v.length > 0)) throw new Error(`Incomplete source contract ${source.id}`);
    new Intl.DateTimeFormat('en', { timeZone: source.timezone });
    source.path = resolve(dirname(resolve(path)), source.path);
    if (paths.has(source.path)) throw new Error('One explicit manifest entry per original file');
    paths.add(source.path);
    // These are metadata/auth stores, not root dialogue, even if named .jsonl.
    if (/(?:^|\/)(?:auth\.json|config\.json|sessions-index\.json|history\.jsonl|subagents|sidecars|blobs)(?:\/|$)/i.test(source.path)) throw new Error(`Not a conversation root: ${source.id}`);
    if (source.active && source.kind !== 'omp') throw new Error('active prefix capture applies only to OMP');
    if (source.mediaRoot) source.mediaRoot = resolve(dirname(resolve(path)), source.mediaRoot);
    if (source.decision === 'include') {
      if (source.kind === 'note') {
        if (!source.author || !source.date) throw new Error(`Note ${source.id} needs authored date and voice`);
        noteAnchor(source.date, source.timezone);
        if (source.date.start.slice(0, 10) > '2026-10-05') throw new Error('Initial authored-note scope ends October 5');
        if (source.coveredDateRange) noteAnchor(source.coveredDateRange, source.timezone);
      } else if (!source.sessionId || !Array.isArray(source.assistantAliases) || !source.assistantAliases.every(a => typeof a === 'string')) throw new Error(`Conversation ${source.id} needs sessionId and explicit assistantAliases (empty is unattributed)`);
    }
    if (source.recordIds && (!Array.isArray(source.recordIds) || !source.recordIds.every(id => typeof id === 'string'))) throw new Error('recordIds must be strings');
    if (source.branchHeads && (!Array.isArray(source.branchHeads) || !source.branchHeads.every(id => typeof id === 'string'))) throw new Error('branchHeads must be strings');
  }
  return raw;
}
function exactTime(value: unknown): number {
  const time = typeof value === 'number' ? value : typeof value === 'string' && /(?:Z|[+-]\d\d:\d\d)$/.test(value) ? Date.parse(value) : NaN;
  if (!Number.isInteger(time) || Math.abs(time) > 8_640_000_000_000_000) throw new Error('Missing or invalid explicit source timestamp');
  return time;
}
/** Midnight in the supplied zone is an indexing convention, never an event clock. */
export function noteAnchor(date: SourceDate, timezone: string): number {
  if (date.precision === 'millisecond') return exactTime(date.start);
  if (!['day', 'range'].includes(date.precision) || !/^\d{4}-\d\d-\d\d$/.test(date.start) || (date.end && (!/^\d{4}-\d\d-\d\d$/.test(date.end) || date.end < date.start))) throw new Error('Invalid authored/covered date range');
  const base = Date.parse(`${date.start}T00:00:00Z`);
  if (!Number.isFinite(base) || new Date(base).toISOString().slice(0, 10) !== date.start) throw new Error('Invalid note day');
  const formatter = new Intl.DateTimeFormat('sv-SE', { timeZone: timezone, year: 'numeric', month: '2-digit', day: '2-digit', hour: '2-digit', minute: '2-digit', second: '2-digit', hourCycle: 'h23' });
  let anchor = base;
  for (let i = 0; i < 3; i++) {
    const parts = Object.fromEntries(formatter.formatToParts(anchor).map(p => [p.type, p.value]));
    const local = Date.parse(`${parts.year}-${parts.month}-${parts.day}T${parts.hour}:${parts.minute}:${parts.second}Z`);
    anchor += base - local;
  }
  return anchor;
}
interface CapturedPrefix { bytes: Buffer; inode: string; tail: number; tailBytes: Buffer }
function hashPrefix(fd: number, end: number): string {
  const digest = createHash('sha256'); const buffer = Buffer.allocUnsafe(Math.min(1_048_576, Math.max(1, end)));
  for (let offset = 0; offset < end;) {
    const n = readSync(fd, buffer, 0, Math.min(buffer.length, end - offset), offset);
    if (!n) throw new Error('Source shrank during prefix verification');
    digest.update(buffer.subarray(0, n)); offset += n;
  }
  return digest.digest('hex');
}
const sameSourceVersion = (a: BigIntStats, b: BigIntStats): boolean =>
  a.isFile() && b.isFile() && a.dev === b.dev && a.ino === b.ino && a.size === b.size && a.mtimeNs === b.mtimeNs && a.ctimeNs === b.ctimeNs;

export function verifySourcePrefix(source: SourceSnapshot): 'unchanged' | 'grown' | 'rewritten-or-replaced' | 'unreadable' {
  for (let attempt = 0; attempt < 3; attempt++) {
    let fd: number | undefined;
    try {
      fd = openSync(source.path, constants.O_RDONLY | constants.O_NOFOLLOW);
      const before = fstatSync(fd, { bigint: true }); const end = source.byteEnd ?? 0;
      if (!before.isFile() || `${before.dev}:${before.ino}` !== source.inode || before.size < BigInt(end)) return 'rewritten-or-replaced';
      const hash = hashPrefix(fd, end);
      const after = fstatSync(fd, { bigint: true }); const current = lstatSync(source.path, { bigint: true });
      // A post-hash rewrite changes ctime even if mtime is restored. Growth
      // DURING the read is retried; stable later suffix growth remains legal.
      if (!sameSourceVersion(before, after) || !sameSourceVersion(after, current)) continue;
      if (hash !== source.hash) return 'rewritten-or-replaced';
      return after.size > BigInt(end) ? 'grown' : 'unchanged';
    } catch { return 'unreadable'; }
    finally { if (fd !== undefined) closeSync(fd); }
  }
  return 'rewritten-or-replaced';
}
function capture(source: SourceSpec): CapturedPrefix {
  for (let attempt = 0; attempt < 3; attempt++) {
    const fd = openSync(source.path, constants.O_RDONLY | constants.O_NOFOLLOW);
    try {
      const before = fstatSync(fd, { bigint: true });
      if (!before.isFile()) throw new Error('Source must be a regular nonsymlink file');
      const raw = readFileSync(fd);
      const end = source.active ? raw.lastIndexOf(10) + 1 : raw.length;
      const bytes = raw.subarray(0, end);
      const digest = hashPrefix(fd, end);
      const after = fstatSync(fd, { bigint: true }); const current = lstatSync(source.path, { bigint: true });
      if (sameSourceVersion(before, after) && sameSourceVersion(after, current) && BigInt(raw.length) === before.size && digest === sha256(bytes)) {
        return { bytes, inode: `${before.dev}:${before.ino}`, tail: raw.length - end, tailBytes: raw.subarray(end) };
      }
    } finally { closeSync(fd); }
  }
  throw new Error('Source identity or content version changed during three capture attempts');
}

// Explicit redaction is applied before XML rendering and structured tool data.
// Originals and per-block hashes remain private authoritative recovery sources.
export function redactText(text: string): { text: string; redactions: number } {
  let redactions = 0;
  const patterns = [
    /-----BEGIN [^-]*PRIVATE KEY-----[\s\S]*?-----END [^-]*PRIVATE KEY-----/g,
    /\b(?:sk-[A-Za-z0-9_-]{16,}|gh[pousr]_[A-Za-z0-9]{20,}|xox[baprs]-[A-Za-z0-9-]+)\b/g,
    /\beyJ[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\b/g,
    /((?:api[_\s-]?key|access[_\s-]?token|refresh[_\s-]?token|authorization|password|secret|account[_\s-]?id)["']?\s*[=:]\s*["']?)(?:Bearer\s+)?[^\s"',;}]+/gi,
    /data:[^\s;,]+;base64,[A-Za-z0-9+/=\r\n]+/g,
    /\b[A-Za-z0-9+/]{256,}={0,2}\b/g,
  ];
  for (const pattern of patterns) text = text.replace(pattern, (...args: unknown[]) => { redactions++; return typeof args[1] === 'string' ? `${args[1]}[REDACTED]` : '[REDACTED]'; });
  return { text, redactions };
}
function safeIdentifier(value: unknown): string {
  const id = String(value ?? 'unlinked');
  return redactText(id).redactions ? `redacted-id:${sha256(id)}` : id;
}
export function safeData(value: unknown): unknown {
  if (typeof value === 'string') return redactText(value).text;
  if (Array.isArray(value)) return value.map(safeData);
  if (value && typeof value === 'object') return Object.fromEntries(Object.entries(value).map(([key, val]) => [key, /thinking|reasoning|signature|secret|token|password|authorization|api.?key|account.?id|base64/i.test(key) ? '[REDACTED]' : safeData(val)]));
  return value;
}
function mediaReference(block: Record<string, unknown>, source: SourceSpec, recordId: string, index: string, root: string): MediaReference {
  const data = object(block.source); const kind = String(block.type ?? 'media');
  const mime = String(block.media_type ?? block.mimeType ?? block.mime_type ?? data.media_type ?? '');
  const ref: MediaReference = { kind, mime, status: 'unsupported', recordId, block: index };
  const encoded = data.data ?? block.data;
  const hash = block.hash ?? block.blobHash ?? data.hash;
  let bytes: Buffer | undefined;
  if (typeof encoded === 'string' && /^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$/.test(encoded)) bytes = Buffer.from(encoded, 'base64');
  else if (typeof hash === 'string' && /^[a-f0-9]{64}$/.test(hash) && source.mediaRoot) {
    ref.hash = hash;
    try {
      const fd = openSync(join(source.mediaRoot, hash), constants.O_RDONLY | constants.O_NOFOLLOW);
      try { if (!fstatSync(fd).isFile()) throw new Error('Not a media file'); bytes = readFileSync(fd); } finally { closeSync(fd); }
    } catch { ref.status = 'missing'; return ref; }
    if (sha256(bytes) !== hash) { ref.status = 'invalid'; return ref; }
  } else { ref.status = typeof hash === 'string' || data.url || block.url || encoded ? 'missing' : 'unsupported'; return ref; }
  if (!bytes || !/^(image\/(png|jpeg|gif|webp)|audio\/[a-z0-9.+-]+|video\/[a-z0-9.+-]+|application\/pdf)$/i.test(mime)) { ref.status = 'invalid'; return ref; }
  // Validate signatures, not just declared MIME, before retaining a typed image.
  const imageValid = !mime.startsWith('image/') || (mime === 'image/png' ? bytes.subarray(0, 8).equals(Buffer.from([137,80,78,71,13,10,26,10])) : mime === 'image/jpeg' ? bytes[0] === 255 && bytes[1] === 216 : mime === 'image/gif' ? bytes.subarray(0, 3).toString() === 'GIF' : bytes.subarray(0, 4).toString() === 'RIFF' && bytes.subarray(8, 12).toString() === 'WEBP');
  if (!imageValid) { ref.status = 'invalid'; return ref; }
  ref.hash = sha256(bytes); ref.bytes = bytes.length; ref.status = 'validated'; ref.snapshot = `sources/media/${ref.hash}`;
  writeFileSync(join(root, ref.snapshot), bytes, { mode: 0o600, flag: 'w' });
  return ref;
}
const CONTROL: Record<string, true> = { session: true, model_change: true, thinking_level_change: true, custom: true, custom_message: true, title_change: true, compaction: true, branch_summary: true, 'file-history-snapshot': true, 'last-prompt': true, 'custom-title': true, 'agent-name': true, progress: true, system: true, queue_operation: true, 'queue-operation': true };

export function normalizeSources(manifest: SourceManifest, root: string): { events: HistoricalEvent[]; dispositions: Disposition[]; sources: SourceSnapshot[] } {
  if (manifest.derivation !== undefined) throw new Error('Derived manifests cannot be raw-prepared; derive the complete reviewed union from the immutable original parent');
  mkdirSync(join(root, 'sources/media'), { recursive: true, mode: 0o700 });
  const events: HistoricalEvent[] = []; const dispositions: Disposition[] = []; const sources: SourceSnapshot[] = [];
  for (const spec of manifest.sources) {
    const report: SourceSnapshot = { id: spec.id, kind: spec.kind, path: spec.path, scope: spec.scope, sessionId: spec.sessionId, decision: spec.decision, reason: spec.reason, active: spec.active, recordCount: 0, eventCount: 0, projectionCount: 0 };
    sources.push(report);
    let captured: CapturedPrefix;
    try { captured = capture(spec); } catch { report.error = 'unreadable-or-unstable-source'; continue; }
    report.hash = sha256(captured.bytes); report.bytes = captured.bytes.length; report.byteEnd = captured.bytes.length; report.inode = captured.inode; report.partialTailBytes = captured.tail;
    report.snapshot = `sources/${spec.id}.original`;
    writeFileSync(join(root, report.snapshot), captured.bytes, { mode: 0o600, flag: 'wx' });
    if (captured.tail) {
      report.partialTailHash = sha256(captured.tailBytes); report.partialTailSnapshot = `sources/${spec.id}.partial-tail.original`;
      writeFileSync(join(root, report.partialTailSnapshot), captured.tailBytes, { mode: 0o600, flag: 'wx' });
    }
    const sourceEvents: HistoricalEvent[] = [];
    const parsed: Array<{ record: Record<string, unknown>; line: number; audit: Disposition }> = [];
    const add = (record: Record<string, unknown>, line: number, raw: string): void => {
      const id = String(record.uuid ?? record.id ?? record.sourceRecordId ?? `line-${line}`);
      const audit: Disposition = { source: spec.id, recordId: id, line, hash: sha256(raw), state: 'pending', reason: spec.reason, blocks: [] };
      dispositions.push(audit); parsed.push({ record, line, audit }); report.recordCount++;
      report.lastId = id; report.lastParentId = parentOf(record);
    };
    if (spec.kind === 'note') add({ id: 'note', content: captured.bytes.toString('utf8') }, 1, captured.bytes.toString('utf8'));
    else if (spec.kind === 'handoff') {
      try {
        const handoff = JSON.parse(captured.bytes.toString('utf8'));
        if (handoff.version !== 1 || !Array.isArray(handoff.records) || (handoff.sourceSession && handoff.sourceSession !== spec.sessionId) || (handoff.sourceBranch !== undefined && typeof handoff.sourceBranch !== 'string')) throw new Error('Unsupported handoff');
        report.sourceBranch = handoff.sourceBranch;
        for (const [index, record] of handoff.records.entries()) add(object(record), index + 1, canonical(record));
      } catch { report.error = 'unsupported-handoff'; }
    } else {
      const lines = captured.bytes.toString('utf8').split('\n');
      if (lines.at(-1) === '') lines.pop();
      report.lineEnd = lines.length;
      for (const [index, raw] of lines.entries()) {
        try { const record = JSON.parse(raw); if (!record || typeof record !== 'object' || Array.isArray(record)) throw new Error('Not a record'); add(record, index + 1, raw); }
        catch { report.recordCount++; dispositions.push({ source: spec.id, recordId: `line-${index + 1}`, line: index + 1, hash: sha256(raw), state: 'quarantine', reason: 'malformed-or-unsupported-record', blocks: [] }); }
      }
    }
    report.lineEnd ??= parsed.length;
    if (captured.tail) dispositions.push({ source: spec.id, recordId: 'partial-tail', line: report.lineEnd + 1, hash: report.partialTailHash!, state: 'exclude', reason: `active-incomplete-newline-tail:${captured.tail}-bytes`, blocks: [] });
    const invalidIds = new Map<string, string>();
    const duplicateAudits = new Set<Disposition>();
    const byId = new Map<string, typeof parsed>();
    for (const item of parsed) {
      if (hasMalformedParent(item.record)) invalidIds.set(item.audit.recordId, 'malformed-source-parent-id');
      const occurrences = byId.get(item.audit.recordId) ?? []; occurrences.push(item); byId.set(item.audit.recordId, occurrences);
    }
    // OMP/handoff IDs name stable acts, not streaming updates. Conflicts
    // quarantine EVERY occurrence; identical repeats retain one act plus an
    // explicit per-record disposition. Only Claude message.id groups stream.
    if (spec.kind === 'omp' || spec.kind === 'handoff') for (const [id, occurrences] of byId) {
      if (occurrences.length < 2) continue;
      const payload = canonical(occurrences[0].record);
      if (occurrences.some(item => canonical(item.record) !== payload)) invalidIds.set(id, 'conflicting-duplicate-source-id');
      else for (const item of occurrences.slice(1)) duplicateAudits.add(item.audit);
    }
    const parentById = new Map<string, string | null>(); const children = new Set<string>();
    for (const { record, audit } of parsed) {
      if (invalidIds.has(audit.recordId) || duplicateAudits.has(audit)) continue;
      const parent = parentOf(record); parentById.set(audit.recordId, parent); if (parent) children.add(parent);
    }
    const observedHeads = [...parentById.keys()].filter(id => !children.has(id));
    const headsById = new Map<string, string[]>();
    for (const head of observedHeads) {
      const visited = new Set<string>(); let cursor: string | null | undefined = head;
      while (cursor && !visited.has(cursor)) {
        visited.add(cursor);
        const heads = headsById.get(cursor) ?? []; heads.push(head); headsById.set(cursor, heads);
        cursor = parentById.get(cursor);
      }
    }
    const annotatedTimes = new Map<string, unknown>();
    for (const { record } of parsed) {
      if (spec.kind === 'omp' && record.customType === 'user-timestamp' && typeof record.parentId === 'string') {
        const data = object(record.data); const time = data.timestampMs ?? data.timestamp;
        try { exactTime(time); annotatedTimes.set(record.parentId, time); } catch { /* control disposition retains invalid annotation source */ }
      }
    }
    const streamGroups = new Map<string, HistoricalEvent>();
    const invalidStreamKeys = new Set<string>();
    const observedStreams = new Map<string, string[]>();
    const selectedStreams = new Set<string>();
    const streamOf = (record: Record<string, unknown>, id: string): string => {
      const message = object(record.message);
      return String(spec.kind === 'claude-code' && message.role === 'assistant' ? message.id ?? id : id);
    };
    for (const { record, audit } of parsed) {
      const stream = streamOf(record, audit.recordId);
      const ids = observedStreams.get(stream) ?? []; ids.push(audit.recordId); observedStreams.set(stream, ids);
      if (spec.recordIds?.includes(audit.recordId)) selectedStreams.add(stream);
    }
    for (const { record, line, audit } of parsed) {
      const message = object(record.message);
      const role = spec.kind === 'note' ? 'authored-note' : spec.kind === 'handoff' ? String(object(record.metadata).originalParticipant ?? record.participant) : String(message.role ?? record.type ?? 'unknown');
      const rawContent = spec.kind === 'note' ? record.content : spec.kind === 'handoff' ? record.content : message.content;
      const blocks: unknown[] = typeof rawContent === 'string' ? [{ type: 'text', text: rawContent }] : Array.isArray(rawContent) ? rawContent : [];
      const isControl = spec.kind !== 'note' && spec.kind !== 'handoff' && (CONTROL[String(record.type)] || record.isCompactSummary || record.isMeta || record.display === false || !['user', 'assistant', 'tool', 'toolResult'].includes(role));
      if (isControl) { audit.state = 'projection'; audit.reason = 'control-or-copied-history-projection'; report.projectionCount++; }
      else if (spec.decision !== 'include') { audit.state = spec.decision; }
      else audit.state = 'include';
      if (record.sessionId && spec.sessionId && record.sessionId !== spec.sessionId) { audit.state = 'quarantine'; audit.reason = 'manifest-session-mismatch'; }
      if (spec.recordIds && !selectedStreams.has(streamOf(record, audit.recordId))) { audit.state = 'exclude'; audit.reason = 'outside-explicit-record-selection'; }
      let timestampMs = 0;
      try { timestampMs = spec.kind === 'note' ? noteAnchor(spec.date!, spec.timezone) : exactTime(annotatedTimes.get(audit.recordId) ?? record.timestamp ?? message.timestamp); }
      catch { if (audit.state === 'include') { audit.state = 'quarantine'; audit.reason = 'missing-or-invalid-source-time'; } }
      if (audit.state === 'include' && spec.kind !== 'note' && spec.kind !== 'handoff' && timestampMs >= Date.parse(manifest.cutoff)) { audit.state = 'exclude'; audit.reason = 'raw-dialogue-cutoff-todays-setup-recon'; }
      if (invalidIds.has(audit.recordId)) { audit.state = 'quarantine'; audit.reason = invalidIds.get(audit.recordId)!; }
      else if (duplicateAudits.has(audit)) { audit.state = 'duplicate-identical-record'; audit.reason = 'identical-stable-source-id-and-payload'; }
      const foreign = !!record.isSidechain || !!record.agentId || !!message.agentId;
      const alias = String(record.participant ?? message.participant ?? role);
      const voice = spec.kind === 'note' ? spec.author! : role === 'assistant' && !foreign && spec.assistantAliases?.includes(alias) ? 'liv' : role === 'user' ? 'user' : foreign ? `foreign:${record.agentId ?? message.agentId ?? alias}` : alias;
      const parentId = parentOf(record);
      const streamId = String(spec.kind === 'claude-code' && role === 'assistant' ? message.id ?? audit.recordId : audit.recordId);
      const event: HistoricalEvent = {
        key: `${spec.id}:${streamId}`, payloadHash: '', source: spec.id, kind: spec.kind,
        recordIds: [audit.recordId], sourceLines: [line], originalTimestamp: annotatedTimes.has(audit.recordId) ? { recordTimestamp: record.timestamp ?? message.timestamp, userTimestamp: annotatedTimes.get(audit.recordId) } : record.timestamp ?? message.timestamp ?? spec.date,
        timestampMs, precision: spec.kind === 'note' ? spec.date?.precision ?? 'unknown' : 'millisecond', timezone: spec.timezone,
        indexingAnchor: spec.kind === 'note' && spec.date?.precision !== 'millisecond' ? 'derived-authored-day-start-not-event-clock' : 'explicit-source-clock',
        ...(spec.kind === 'note' ? { authoredDate: spec.date, coveredDateRange: spec.coveredDateRange } : {}),
        role, voice, participant: spec.kind === 'note' ? 'Historical Note' : voice === 'liv' ? 'liv' : role === 'user' ? 'user' : role === 'toolResult' || role === 'tool' ? 'Historical Tool' : 'Historical Actor',
        parentId, streamId, observedStreamRecordIds: observedStreams.get(streamId) ?? [audit.recordId], partialStream: false,
        branchHeads: headsById.get(audit.recordId) ?? [], selectedHeads: spec.branchHeads ?? [], sourceBranch: report.sourceBranch,
        outcome: 'observed-source-record-not-inferred-outcome', ancestryGap: !!parentId && !parentById.has(parentId), parentInCorpus: false, gapReason: 'pending-source-projection',
        provenance: foreign ? 'sidechain-or-foreign-agent' : 'manifest-selected-parent-source', tools: [], media: [], text: '',
      };
      if (role === 'toolResult' || role === 'tool') event.tools.push({ kind: 'result', id: safeIdentifier(message.toolCallId ?? message.tool_use_id ?? record.toolCallId), name: typeof message.toolName === 'string' ? message.toolName : undefined });
      const texts: string[] = [];
      const visit = (blockValue: unknown, index: string): void => {
        const block = object(blockValue); const type = String(block.type ?? 'unknown');
        const disposition = { index, hash: sha256(canonical(blockValue)), state: 'unsupported' };
        audit.blocks.push(disposition);
        if (/thinking|reasoning|signature/.test(type)) { disposition.state = 'nonportable-thinking-excluded'; return; }
        if (type === 'text') {
          const redacted = redactText(String(block.text ?? '')); disposition.state = redacted.redactions ? `text-redacted:${redacted.redactions}` : 'text';
          texts.push(xml(redacted.text));
        } else if (type === 'tool_use' || type === 'toolCall') {
          disposition.state = 'inert-tool-call'; event.tools.push({ kind: 'call', id: safeIdentifier(block.id), name: String(block.name ?? 'unknown') });
          texts.push(`<historical-tool-call>${xml(canonical(safeData({ id: block.id, name: block.name, arguments: block.input ?? block.arguments })))}</historical-tool-call>`);
        } else if (type === 'tool_result' || type === 'toolResult') {
          disposition.state = 'inert-tool-result'; event.tools.push({ kind: 'result', id: safeIdentifier(block.tool_use_id ?? block.toolCallId) });
          texts.push(`<historical-tool-result link="${xml(safeIdentifier(block.tool_use_id ?? block.toolCallId))}">`);
          const nested = typeof block.content === 'string' ? [{ type: 'text', text: block.content }] : Array.isArray(block.content) ? block.content : [];
          nested.forEach((child, j) => visit(child, `${index}.${j}`)); texts.push('</historical-tool-result>');
        } else if (/image|audio|video|document|file/.test(type)) {
          const ref = mediaReference(block, spec, audit.recordId, index, root); event.media.push(ref); disposition.state = `typed-media:${ref.status}`;
          texts.push(`<historical-media kind="${xml(ref.kind)}" mime="${xml(ref.mime)}" status="${ref.status}" hash="${ref.hash ?? ''}"/>`);
        }
      };
      blocks.forEach((block, i) => visit(block, String(i)));
      if (audit.state !== 'include') continue;
      if (!blocks.length) {
        // Explicit no-output OMP assistant stop/aborted/error outcomes are
        // distinct controls. The record hash/line and immutable original retain
        // parent, clocks and outcome; no text or thinking is invented for replay.
        if (spec.kind === 'omp' && record.type === 'message' && message.role === 'assistant' && Array.isArray(rawContent) && (message.stopReason === 'stop' || message.stopReason === 'aborted' || message.stopReason === 'error')) {
          audit.state = 'projection'; audit.reason = `empty-assistant-${message.stopReason}-control-projection`; report.projectionCount++;
        } else { audit.state = 'quarantine'; audit.reason = 'missing-or-unsupported-message-content'; }
        continue;
      }
      if (!texts.length && !event.media.length && !event.tools.length) { audit.state = 'exclude'; audit.reason = 'no-delivered-portable-content'; continue; }
      const joined = texts.join('\n');
      // Known resume wrappers are copied projections; ordinary quotations inside
      // real turns remain text with their source role and escaped XML intact.
      if (record.isCompactSummary || /^This session is being continued from a previous conversation|^\[Conversation history (?:summary|resume)\]/.test(joined)) { audit.state = 'projection'; audit.reason = 'copied-resume-history'; report.projectionCount++; continue; }
      event.text = joined;
      if (event.tools.length && role === 'user' && !blocks.some(b => object(b).type === 'text' && String(object(b).text ?? '').trim())) { event.role = 'tool-transport'; event.voice = 'historical-tool'; event.participant = 'Historical Tool'; }
      if (invalidStreamKeys.has(event.key)) { audit.state = 'quarantine'; audit.reason = 'logical-source-key-conflict'; invalidIds.set(audit.recordId, audit.reason); continue; }
      const previous = streamGroups.get(event.key);
      if (previous) {
        if (spec.kind !== 'claude-code' || role !== 'assistant' || typeof message.id !== 'string' || !message.id || previous.role !== event.role || previous.voice !== event.voice) {
          audit.state = 'quarantine'; audit.reason = 'logical-source-key-conflict';
          for (const item of parsed) if (previous.recordIds.includes(item.audit.recordId)) { item.audit.state = 'quarantine'; item.audit.reason = audit.reason; invalidIds.set(item.audit.recordId, audit.reason); }
          sourceEvents.splice(sourceEvents.indexOf(previous), 1); streamGroups.delete(event.key); invalidStreamKeys.add(event.key); invalidIds.set(audit.recordId, audit.reason);
          continue;
        }
        // Claude streaming snapshots can be cumulative or incremental. Only
        // exact logical stream groups coalesce; distinct records never dedupe.
        if (event.text.startsWith(previous.text)) previous.text = event.text;
        else if (!previous.text.startsWith(event.text)) previous.text += `\n${event.text}`;
        previous.recordIds.push(...event.recordIds); previous.sourceLines.push(line);
        previous.media.push(...event.media); previous.tools.push(...event.tools);
        previous.branchHeads = [...new Set([...previous.branchHeads, ...event.branchHeads])];
        audit.state = 'coalesced-stream-blocks';
      } else { streamGroups.set(event.key, event); sourceEvents.push(event); }
    }
    const importedIds = new Set(sourceEvents.flatMap(event => event.recordIds));
    for (const event of sourceEvents) {
      event.parentInCorpus = event.parentId === null || importedIds.has(event.parentId);
      event.gapReason = event.ancestryGap ? 'missing-source-ancestry' : !event.parentInCorpus ? 'parent-is-outside-selection-or-a-control-projection' : 'none';
      const ancestors = new Set<string>(); let ancestor = event.parentId;
      while (ancestor && !ancestors.has(ancestor)) {
        ancestors.add(ancestor);
        const invalid = invalidIds.get(ancestor);
        if (invalid) { event.ancestryGap = true; event.gapReason = `quarantined-ancestor:${invalid}`; break; }
        ancestor = parentById.get(ancestor) ?? null;
      }
      event.partialStream = event.observedStreamRecordIds.some(id => !event.recordIds.includes(id));
      // Linkage and precision travel as data, never active tool definitions.
      event.text = `<historical-observation metadata="${xml(canonical(safeData({ source: event.source, kind: event.kind, recordIds: event.recordIds, observedStreamRecordIds: event.observedStreamRecordIds, partialStream: event.partialStream, role: event.role, voice: event.voice, sourceTimestamp: event.originalTimestamp, precision: event.precision, authoredDate: event.authoredDate, coveredDateRange: event.coveredDateRange, indexingAnchor: event.indexingAnchor, parentId: event.parentId, ancestryGap: event.ancestryGap, parentInCorpus: event.parentInCorpus, gapReason: event.gapReason, branchHeads: event.branchHeads, selectedHeads: event.selectedHeads, sourceBranch: event.sourceBranch, outcome: event.outcome, provenance: event.provenance, tools: event.tools })))}">\n${event.text}\n</historical-observation>`;
      event.payloadHash = sha256(canonical({ ...event, payloadHash: undefined }));
      events.push(event);
    }
    report.eventCount = sourceEvents.length;
  }
  // Handoff is the final direct-dialogue bridge, after all authored notes.
  events.sort((a, b) => Number(a.kind === 'handoff') - Number(b.kind === 'handoff') || a.timestampMs - b.timestampMs || a.source.localeCompare(b.source) || a.sourceLines[0] - b.sourceLines[0]);
  return { events, dispositions, sources };
}
