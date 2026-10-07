import { constants, existsSync, mkdirSync, chmodSync, lstatSync, realpathSync, readFileSync, writeFileSync, openSync, closeSync, fsyncSync, renameSync, readdirSync, statSync } from 'node:fs';
import { dirname, join, resolve } from 'node:path';
import { randomUUID } from 'node:crypto';
import { spawn, type ChildProcessWithoutNullStreams } from 'node:child_process';
import { JsStore } from '@animalabs/chronicle';
import { ARCHIVAL_MEMORY_LOCAL_CAP_CODE, ArchivalCyberPolicyFallbackHalt, getMintRequestByHash, ContextManager, AutobiographicalStrategy, type StoredMessage, type SummaryEntry, type MessageStoreView, type MessageId } from '@animalabs/context-manager';
import { Membrane, MembraneError, OpenAIResponsesFormatter, type ProviderAdapter, type NormalizedRequest, type ProviderRequest, type NormalizedResponse, type CompleteOptions, type MembraneConfig } from '@animalabs/membrane';
import { SessionManager } from '../../src/session-manager.js';
import type { Recipe } from '../../src/recipe.js';
import { CodexSubscriptionAdapter } from '../../src/codex-subscription-adapter.js';
import { ARCHIVAL_MEMORY_MODEL, ARCHIVAL_CYBER_POLICY_FALLBACK_MODEL, isArchivalMemoryModel, validateArchivalServedModel, ArchivalMemoryBudgetError, readArchivalMemoryProvenance, enforceArchivalMemoryBudget, type ArchivalMemoryAccounting } from '../../src/archival-memory-budget.js';
import { types } from 'node:util';
import { canonical, sha256, xml, loadManifest, normalizeSources, verifySourcePrefix, safeData, type HistoricalEvent, type SourceSnapshot, type Disposition, type SourceManifest, type ExclusionDecision, type EventLineage, type OmissionReceipt, type DerivationReceipt, type MediaReference } from './backfill-normalize.js';

export const MODEL = ARCHIVAL_MEMORY_MODEL;
export const ARCHIVAL_ROUTING_POLICY = {
  scope: 'native-archival', primaryModel: MODEL, fallbackModel: ARCHIVAL_CYBER_POLICY_FALLBACK_MODEL,
  trigger: 'cyber_policy', maxFallbackAttempts: 1,
} as const;
export const NAMESPACE = 'agents/liv';
export const PROFILE = {
  type: 'autobiographical', compressionModel: MODEL, summaryParticipant: 'liv',
  targetChunkTokens: 24000, summaryTargetTokens: 1536, mergeThreshold: 4, l1HoldbackChunks: 0,
  adaptiveResolution: true, foldingStrategy: 'kv-stable', enforceBudget: true,
  headWindowTokens: 512, recentWindowTokens: 150000, maxMessageTokens: 0,
  compressionMaxTokens: 8192, compressionContextBudgetTokens: 600512, compressionRecallBudgetTokens: 200000,
} as const;
export const AGENT_PROFILE = { name: 'liv', provider: 'openai-codex', model: MODEL, contextBudgetTokens: 604608, maxTokens: 4096, maxStreamTokens: 2418432 } as const;
interface Mapping { payloadHash: string; nativeIds: string[]; textHash: string; timestampMs: number; ranges: Array<{ id: string; start: number; end: number }> }
interface Checkpoint { version: 1; fingerprint: string; nextEvent: number; mapping: Record<string, Mapping>; batches: Array<{ start: number; end: number; lastId: string }> }
interface Inventory {
  version: 1; sessionId: string; namespace: string; fingerprint: string;
  manifestHash: string; eventsHash: string; dispositionsHash: string; sourcesHash: string;
  recipeHash: string; groundHash: string; framingHash: string; profileHash: string;
  omissionsHash?: string; derivationHash?: string;
  eventCount: number; sourcePaths: Record<string, string>; prepared: string;
}
interface Inspection {
  fingerprint: string; nativeMessages: number; imported: number; nextEvent: number;
  leaves: number; merges: number; l1Queue: number; mergeQueue: number; unsealed: number;
  quarantine: number; mergeQuarantine: number; sourceLinksValid: boolean; dateMin?: number; dateMax?: number;
  effectiveProfile: Record<string, unknown>;
  summaries: Array<{ id: string; level: number; sourceIds: string[]; sourceRange: { first: string; last: string }; mergedInto?: string; model?: string; archivalCyberPolicyFallback?: NonNullable<SummaryEntry['provenance']>['archivalCyberPolicyFallback'] }>;
  nativeFiles: Record<string, string>; checkpointHash: string; updated: string;
}
export interface PrepareOptions { manifest: string; recipe: string; framing: string; out: string }
export interface DeriveOptions { parent: string; excludeEvents: string; framing: string; out: string }
interface PreparedState {
  inventory: Inventory; events: HistoricalEvent[]; checkpoint: Checkpoint; ground: string; framing: string; recipe: Recipe;
  manifest: SourceManifest; sources: SourceSnapshot[]; dispositions: Disposition[];
  omissions?: OmissionReceipt; derivation?: DerivationReceipt;
}
export interface ApplyOptions {
  apply: boolean; maxEvents?: number; maxSteps?: number;
  /** Test-only structural adapter injection; no CLI flag or delivered memories. */
  adapterFactory?: () => ProviderAdapter & { dispose(): void };
  /** Test-only interruption seam, invoked after fsynced native writes. */
  interrupt?: (point: 'append-before-checkpoint' | 'checkpoint-before-inspection' | 'tick-before-inspection', cm: ContextManager) => void;
}
export function durableJson(path: string, value: unknown): void {
  const tmp = `${path}.${randomUUID()}.tmp`;
  const fd = openSync(tmp, constants.O_WRONLY | constants.O_CREAT | constants.O_EXCL | constants.O_NOFOLLOW, 0o600);
  try { writeFileSync(fd, JSON.stringify(value, null, 2) + '\n'); fsyncSync(fd); } finally { closeSync(fd); }
  renameSync(tmp, path);
  const parent = openSync(dirname(path), constants.O_RDONLY); try { fsyncSync(parent); } finally { closeSync(parent); }
}
function regular(path: string): void {
  const st = lstatSync(path);
  if (!st.isFile() || st.isSymbolicLink()) throw new Error('Private candidate artifact must be a regular nonsymlink file');
}
function rootPath(input: string): string {
  const root = resolve(input);
  if (!lstatSync(root).isDirectory() || lstatSync(root).isSymbolicLink() || realpathSync(root) !== root) throw new Error('Candidate root must be an explicit real nonsymlink directory');
  if ((statSync(root).mode & 0o077) !== 0) throw new Error('Candidate root must be private (mode 0700)');
  regular(join(root, 'instance.json'));
  return root;
}
function jsonFile<T>(path: string): T { regular(path); return JSON.parse(readFileSync(path, 'utf8')) as T; }
function bytesHash(path: string): string { regular(path); return sha256(readFileSync(path)); }
const HASH_FILES = {
  manifestHash: 'manifest.json', eventsHash: 'events.jsonl', dispositionsHash: 'dispositions.json', sourcesHash: 'sources.json',
  recipeHash: 'recipe.json', groundHash: 'ground.md', framingHash: 'framing.md',
  omissionsHash: 'omissions.json', derivationHash: 'derivation.json',
} as const;
function inventoryHashes(inventory: Inventory): Record<string, string> {
  return Object.fromEntries([...Object.keys(HASH_FILES), 'profileHash'].filter(key => inventory[key as keyof Inventory] !== undefined).map(key => [key, inventory[key as keyof Inventory] as string]));
}
function validateDecision(value: unknown): ExclusionDecision {
  const fail = (): never => { throw new Error('Invalid version 1 exact-event exclusion decision'); };
  if (!value || typeof value !== 'object' || Array.isArray(value)) return fail();
  const decision = value as ExclusionDecision;
  const counts = ['rawSourceCount', 'logicalEventCount', 'missingAcceptedPreimageCount'] as const;
  const flags = ['wholeEventExclusionHasNoAdditionalNativeShards', 'newCandidateRequired', 'existingSummariesMustNotBeReusedWithoutCompleteDependencyEvidence', 'reviewProseMustNotEnterLiveContext'] as const;
  const allowed = new Set(['version', 'originFingerprint', 'originSessionId', 'selectedSummaryIds', 'eventKeys', 'reason', ...counts, ...flags]);
  const uniqueStrings = (items: unknown): items is string[] => Array.isArray(items) && items.length > 0 && items.every(item => typeof item === 'string' && item.trim().length > 0 && item === item.trim() && !/[\u0000-\u001f\u007f]/.test(item)) && new Set(items).size === items.length;
  if (Object.keys(decision).some(key => !allowed.has(key)) || decision.version !== 1 || !/^[a-f0-9]{64}$/.test(decision.originFingerprint) || !/^[a-f0-9]{8}$/.test(decision.originSessionId) || !uniqueStrings(decision.selectedSummaryIds) || !uniqueStrings(decision.eventKeys) || typeof decision.reason !== 'string' || !decision.reason.trim()) return fail();
  for (const key of counts) if (decision[key] !== undefined && (!Number.isSafeInteger(decision[key]) || decision[key]! < (key === 'missingAcceptedPreimageCount' ? 0 : 1))) return fail();
  for (const key of flags) if (decision[key] !== undefined && decision[key] !== true) return fail();
  if (decision.logicalEventCount !== undefined && decision.logicalEventCount !== decision.eventKeys.length) return fail();
  return decision;
}
const eventLineage = (event: EventLineage): EventLineage => ({ key: event.key, payloadHash: event.payloadHash, source: event.source, recordIds: event.recordIds, sourceLines: event.sourceLines });
function omissionCoverage(events: EventLineage[], dispositions: Disposition[]): OmissionReceipt['events'] {
  return events.map(event => {
    if (!event.recordIds.length || event.recordIds.length !== event.sourceLines.length) throw new Error('Excluded event has incomplete disposition lineage');
    const dispositionIndexes: number[] = [];
    dispositions.forEach((row, index) => { if (row.source === event.source && event.recordIds.includes(row.recordId)) dispositionIndexes.push(index); });
    for (let i = 0; i < event.recordIds.length; i++) if (!dispositionIndexes.some(index => dispositions[index].recordId === event.recordIds[i] && dispositions[index].line === event.sourceLines[i])) throw new Error('Excluded event has incomplete disposition lineage');
    return { ...eventLineage(event), dispositionIndexes };
  });
}
function mediaPath(root: string, media: MediaReference): string {
  if (media.status !== 'validated' || !media.hash || !/^[a-f0-9]{64}$/.test(media.hash) || media.snapshot !== `sources/media/${media.hash}` || !Number.isSafeInteger(media.bytes) || media.bytes! < 0) throw new Error('Invalid retained media receipt');
  const path = join(root, media.snapshot); regular(path);
  if (realpathSync(path) !== path || statSync(path).size !== media.bytes || bytesHash(path) !== media.hash) throw new Error('Retained media path/hash mismatch');
  return path;
}
function validateFiltered(state: PreparedState): void {
  const { inventory, manifest, events, sources, dispositions, checkpoint, omissions, derivation } = state;
  const directive = manifest.derivation;
  const fail = (): never => { throw new Error('Persistent event exclusion/derivation receipt mismatch; new candidate required'); };
  if (!directive || !omissions || !derivation || !inventory.omissionsHash || !inventory.derivationHash) return fail();
  const decision = validateDecision(omissions.decision); const decisionHash = sha256(canonical(decision));
  if (canonical(directive) !== canonical({ version: 1, originFingerprint: decision.originFingerprint, originSessionId: decision.originSessionId, decisionHash, omissions: 'omissions.json', lineage: 'derivation.json' }) || omissions.version !== 1 || omissions.originFingerprint !== decision.originFingerprint || omissions.originSessionId !== decision.originSessionId || omissions.decisionHash !== decisionHash || derivation.version !== 1 || derivation.origin.fingerprint !== decision.originFingerprint || derivation.origin.sessionId !== decision.originSessionId || inventory.sessionId === decision.originSessionId) return fail();
  const originHashes = derivation.origin.hashes;
  const originalKeys = [...Object.keys(HASH_FILES).filter(key => key !== 'omissionsHash' && key !== 'derivationHash'), 'profileHash'].sort();
  if (Object.keys(originHashes).sort().join(',') !== originalKeys.join(',') || Object.values(originHashes).some(hash => !/^[a-f0-9]{64}$/.test(hash)) || sha256(canonical(originHashes)) !== decision.originFingerprint || originHashes.groundHash !== inventory.groundHash || originHashes.profileHash !== inventory.profileHash || derivation.originals !== 'parent-only' || derivation.nativeState !== 'fresh-empty' || derivation.summaryReuse !== false) return fail();
  const excluded = new Set(decision.eventKeys); const originKeys = new Set<string>();
  for (const event of derivation.events) {
    if (canonical(event) !== canonical(eventLineage(event)) || typeof event.key !== 'string' || originKeys.has(event.key) || !/^[a-f0-9]{64}$/.test(event.payloadHash) || !Array.isArray(event.recordIds) || !Array.isArray(event.sourceLines)) return fail();
    originKeys.add(event.key);
  }
  if (decision.eventKeys.some(key => !originKeys.has(key)) || derivation.events.length !== derivation.origin.eventCount || canonical(events.map(eventLineage)) !== canonical(derivation.events.filter(event => !excluded.has(event.key)))) return fail();
  const omitted = derivation.events.filter(event => excluded.has(event.key));
  const coverage = omissionCoverage(omitted, dispositions);
  const indexes = new Set(coverage.flatMap(event => event.dispositionIndexes));
  const rowKeys = new Map<number, string[]>();
  for (const event of coverage) for (const index of event.dispositionIndexes) rowKeys.set(index, [...(rowKeys.get(index) ?? []), event.key]);
  if (canonical(coverage) !== canonical(omissions.events) || omissions.excludedLogicalEvents !== excluded.size || omissions.excludedDispositionRecords !== indexes.size || omissions.contributingSources !== new Set(omitted.map(event => event.source)).size || derivation.excludedLogicalEvents !== excluded.size || derivation.excludedDispositionRecords !== indexes.size || derivation.retainedEligibleEvents !== events.length) return fail();
  const records: OmissionReceipt['records'] = []; const originalRows: Disposition[] = [];
  dispositions.forEach((row, index) => {
    const { exclusion, ...original } = row;
    if (indexes.has(index)) {
      if (!exclusion || row.state !== 'exclude' || row.reason !== decision.reason || canonical(exclusion) !== canonical({ receipt: 'omissions.json', decisionHash, eventKeys: rowKeys.get(index), previousState: exclusion.previousState, previousReason: exclusion.previousReason }) || typeof exclusion.previousState !== 'string' || typeof exclusion.previousReason !== 'string') return fail();
      if (events.some(event => event.source === row.source && event.recordIds.includes(row.recordId))) return fail();
      original.state = exclusion.previousState; original.reason = exclusion.previousReason;
      records.push({ index, source: row.source, recordId: row.recordId, line: row.line, hash: row.hash, previousState: exclusion.previousState });
    } else if (exclusion) return fail();
    originalRows.push(original);
  });
  if (canonical(records) !== canonical(omissions.records) || sha256(canonical(originalRows)) !== derivation.originDispositionsCanonicalHash) return fail();
  if (sources.length !== manifest.sources.length || sources.length !== derivation.sources.length || new Set(sources.map(source => source.id)).size !== sources.length) return fail();
  for (const [index, source] of sources.entries()) {
    const counts = { id: source.id, originEventCount: derivation.events.filter(event => event.source === source.id).length, retainedEventCount: events.filter(event => event.source === source.id).length, excludedEventCount: omitted.filter(event => event.source === source.id).length };
    if (source.snapshot !== undefined || source.partialTailSnapshot !== undefined || source.inode !== undefined || source.active !== false || canonical(counts) !== canonical(derivation.sources[index]) || source.eventCount !== counts.retainedEventCount || canonical(source.lineage) !== canonical({ originFingerprint: decision.originFingerprint, originSessionId: decision.originSessionId, originEventCount: counts.originEventCount, excludedEventCount: counts.excludedEventCount, originals: 'parent-only' }) || manifest.sources[index].id !== source.id || manifest.sources[index].path !== source.path || manifest.sources[index].active !== false || manifest.sources[index].mediaRoot !== undefined) return fail();
  }
  const eventByKey = new Map(events.map(event => [event.key, event]));
  if (!checkpoint.mapping || typeof checkpoint.mapping !== 'object' || Array.isArray(checkpoint.mapping)) return fail();
  for (const [key, entry] of Object.entries(checkpoint.mapping)) {
    const event = eventByKey.get(key);
    if (!event || excluded.has(key) || !entry || entry.payloadHash !== event.payloadHash || entry.timestampMs !== event.timestampMs || !Array.isArray(entry.nativeIds) || !Array.isArray(entry.ranges) || !entry.nativeIds.length || entry.ranges.length !== entry.nativeIds.length || new Set(entry.nativeIds).size !== entry.nativeIds.length) return fail();
    let end = 0;
    for (const [index, range] of entry.ranges.entries()) { if (range.id !== entry.nativeIds[index] || range.start !== end || !Number.isInteger(range.end) || range.end <= end || range.end > event.text.length) return fail(); end = range.end; }
    if (entry.textHash !== sha256(event.text.slice(0, end))) return fail();
  }
}
function load(root: string): PreparedState {
  const inventory = jsonFile<Inventory>(join(root, 'instance.json'));
  if (inventory.version !== 1 || inventory.namespace !== NAMESPACE || !/^[a-f0-9]{8}$/.test(inventory.sessionId)) throw new Error('Invalid native candidate identity');
  for (const [key, file] of Object.entries(HASH_FILES)) {
    const hash = inventory[key as keyof Inventory];
    if ((key === 'omissionsHash' || key === 'derivationHash') && hash === undefined) continue;
    if (typeof hash !== 'string' || !/^[a-f0-9]{64}$/.test(hash) || bytesHash(join(root, file)) !== hash) throw new Error(`Prepared ${file} changed: rebuild/new-candidate required; committed chronological history cannot be rewritten`);
  }
  if (inventory.profileHash !== sha256(canonical({ strategy: PROFILE, agent: AGENT_PROFILE }))) throw new Error('Backfill profile changed; new candidate required');
  if (inventory.fingerprint !== sha256(canonical(inventoryHashes(inventory)))) throw new Error('Prepared canonical fingerprint mismatch');
  const events = readFileSync(join(root, 'events.jsonl'), 'utf8').split('\n').filter(Boolean).map(line => JSON.parse(line) as HistoricalEvent);
  const keys = new Set<string>();
  for (const event of events) {
    if (typeof event.key !== 'string' || keys.has(event.key) || event.payloadHash !== sha256(canonical({ ...event, payloadHash: undefined }))) throw new Error('Source key conflict in immutable normalized corpus');
    keys.add(event.key);
  }
  if (inventory.eventCount !== events.length) throw new Error('Prepared event count mismatch');
  const checkpoint = jsonFile<Checkpoint>(join(root, 'checkpoint.json'));
  if (checkpoint.version !== 1 || checkpoint.fingerprint !== inventory.fingerprint || !Number.isInteger(checkpoint.nextEvent) || checkpoint.nextEvent < 0 || checkpoint.nextEvent > events.length) throw new Error('Checkpoint/corpus fingerprint mismatch');
  const state: PreparedState = { inventory, events, checkpoint, ground: readFileSync(join(root, 'ground.md'), 'utf8'), framing: readFileSync(join(root, 'framing.md'), 'utf8'), recipe: jsonFile<Recipe>(join(root, 'recipe.json')), manifest: jsonFile<SourceManifest>(join(root, 'manifest.json')), sources: jsonFile<SourceSnapshot[]>(join(root, 'sources.json')), dispositions: jsonFile<Disposition[]>(join(root, 'dispositions.json')) };
  const filtered = state.manifest.derivation !== undefined || inventory.omissionsHash !== undefined || inventory.derivationHash !== undefined || state.sources.some(source => source.lineage !== undefined) || state.dispositions.some(row => row.exclusion !== undefined) || existsSync(join(root, 'omissions.json')) || existsSync(join(root, 'derivation.json'));
  if (filtered) {
    state.omissions = jsonFile<OmissionReceipt>(join(root, 'omissions.json')); state.derivation = jsonFile<DerivationReceipt>(join(root, 'derivation.json')); validateFiltered(state);
    for (const event of events) for (const media of event.media) { if (media.status === 'validated') mediaPath(root, media); else if (media.snapshot !== undefined) throw new Error('Unvalidated retained media has a snapshot'); }
  }
  return state;
}

/** flock keeps one real process owner; kernel release makes stale recovery safe.
 * The lock inode is NEVER unlinked/replaced (avoids ABA and pid-reuse races).
 * The child echoes a nonce only AFTER flock acquired; stdin EOF releases it
 * even when the owning Bun process is killed. Chronicle adds its own lock.
 */
export async function withWriter<T>(root: string, work: () => Promise<T>): Promise<T> {
  const nonce = randomUUID();
  const lock = join(root, 'writer.lock');
  const fd = openSync(lock, constants.O_CREAT | constants.O_RDWR | constants.O_NOFOLLOW, 0o600); closeSync(fd);
  const child: ChildProcessWithoutNullStreams = spawn('flock', ['-n', '-E', '73', lock, 'cat'], { stdio: ['pipe', 'pipe', 'pipe'] });
  try {
    await new Promise<void>((accept, reject) => {
      let echoed = '';
      const timer = setTimeout(() => reject(new Error('Candidate writer ownership acquisition timed out')), 5000);
      child.on('error', () => { clearTimeout(timer); reject(new Error('flock is required for candidate process ownership')); });
      child.on('exit', () => { clearTimeout(timer); reject(new Error('Candidate already has a writer; resume after its process exits')); });
      child.stdout.on('data', data => { echoed += String(data); if (echoed.includes(nonce)) { clearTimeout(timer); accept(); } });
      child.stdin.write(`${nonce}\n`);
    });
    durableJson(join(root, 'writer.json'), { pid: process.pid, token: nonce, state: 'owned', acquired: new Date().toISOString() });
    return await work();
  } finally {
    child.stdin.end();
    if (child.exitCode === null) await new Promise<void>(done => { const timer = setTimeout(() => { child.kill('SIGKILL'); done(); }, 2000); child.once('exit', () => { clearTimeout(timer); done(); }); });
    const writer = existsSync(join(root, 'writer.json')) ? jsonFile<{ token: string }>(join(root, 'writer.json')) : null;
    if (writer?.token === nonce) durableJson(join(root, 'writer.json'), { pid: process.pid, token: nonce, state: 'closed', released: new Date().toISOString() });
  }
}
const shellArg = (value: string): string => `'${value.replaceAll("'", "'\"'\"'")}'`;
function safeRecipePaths(value: unknown, base: string): unknown {
  if (Array.isArray(value)) return value.map(v => safeRecipePaths(v, base));
  if (!value || typeof value !== 'object') return value;
  return Object.fromEntries(Object.entries(value).map(([key, v]) => {
    // Preserve env credential references; never persist resolved credentials.
    if (typeof v === 'string' && !v.includes('${') && ['path', 'cwd', 'recipe', 'keyPath'].includes(key) && !/^https?:/.test(v)) return [key, resolve(base, v)];
    if (key === 'args' && Array.isArray(v)) return [key, v.map(arg => typeof arg === 'string' && /^\.\.?\//.test(arg) ? resolve(base, arg) : arg)];
    return [key, safeRecipePaths(v, base)];
  }));
}
function newOutput(input: string): string {
  if (typeof input !== 'string' || !input.trim()) throw new Error('Explicit new private output path is required');
  const out = resolve(input);
  if (lstatSync(out, { throwIfNoEntry: false })) throw new Error('Output already exists; preparation never overwrites a directory or symlink');
  if (realpathSync(dirname(out)) !== dirname(out)) throw new Error('Output parent must not traverse symlinks');
  return out;
}
function privateTree(root: string): void {
  const visit = (path: string): void => {
    const st = lstatSync(path);
    if (st.isSymbolicLink() || (!st.isDirectory() && !st.isFile())) throw new Error('Private candidate tree contains an unsupported entry');
    chmodSync(path, st.isDirectory() ? 0o700 : 0o600);
    if (st.isDirectory()) for (const name of readdirSync(path)) visit(join(path, name));
  };
  visit(root);
}
interface CandidateCorpus { manifest: SourceManifest; events: HistoricalEvent[]; sources: SourceSnapshot[]; dispositions: Disposition[]; omissions?: OmissionReceipt; derivation?: DerivationReceipt }
async function initializeCandidate(out: string, corpus: CandidateCorpus, recipe: Recipe, ground: string, framing: string): Promise<Record<string, unknown>> {
  durableJson(join(out, 'manifest.json'), corpus.manifest); durableJson(join(out, 'sources.json'), corpus.sources); durableJson(join(out, 'dispositions.json'), corpus.dispositions);
  writeFileSync(join(out, 'events.jsonl'), corpus.events.map(event => canonical(event)).join('\n') + (corpus.events.length ? '\n' : ''), { mode: 0o600, flag: 'wx' });
  writeFileSync(join(out, 'ground.md'), ground, { mode: 0o600, flag: 'wx' }); writeFileSync(join(out, 'framing.md'), framing, { mode: 0o600, flag: 'wx' });
  recipe.agent = { ...recipe.agent, ...AGENT_PROFILE, systemPrompt: ground, strategy: { ...PROFILE, identityReminder: framing } };
  durableJson(join(out, 'recipe.json'), recipe);
  if (corpus.omissions) durableJson(join(out, 'omissions.json'), corpus.omissions);
  if (corpus.derivation) durableJson(join(out, 'derivation.json'), corpus.derivation);
  // Both preparation paths create a new session/store with this one convention.
  const sessions = new SessionManager(join(out, 'data')); const session = sessions.createSession('Liv dated autobiographical backfill');
  if (session.id === corpus.derivation?.origin.sessionId) throw new Error('Fresh candidate native session identity collision');
  sessions.setActiveSession(session.id); // Only the independently created candidate.
  durableJson(join(out, 'data/sessions', `${session.id}.import-source.json`), { agentName: 'liv', originalMessageCount: corpus.events.length, importedMessageCount: 0, importedAt: new Date().toISOString() });
  const hashes = {
    manifestHash: bytesHash(join(out, 'manifest.json')), eventsHash: bytesHash(join(out, 'events.jsonl')), dispositionsHash: bytesHash(join(out, 'dispositions.json')), sourcesHash: bytesHash(join(out, 'sources.json')),
    recipeHash: bytesHash(join(out, 'recipe.json')), groundHash: sha256(ground), framingHash: sha256(framing), profileHash: sha256(canonical({ strategy: PROFILE, agent: AGENT_PROFILE })),
    ...(corpus.omissions ? { omissionsHash: bytesHash(join(out, 'omissions.json')) } : {}), ...(corpus.derivation ? { derivationHash: bytesHash(join(out, 'derivation.json')) } : {}),
  };
  const inventory: Inventory = { version: 1, sessionId: session.id, namespace: NAMESPACE, ...hashes, fingerprint: sha256(canonical(hashes)), eventCount: corpus.events.length, sourcePaths: Object.fromEntries(corpus.manifest.sources.map(source => [source.id, source.path])), prepared: new Date().toISOString() };
  durableJson(join(out, 'instance.json'), inventory);
  const checkpoint: Checkpoint = { version: 1, fingerprint: inventory.fingerprint, nextEvent: 0, mapping: {}, batches: [] };
  durableJson(join(out, 'checkpoint.json'), checkpoint);
  durableJson(join(out, 'audit.json'), { version: 1, requests: 0, responses: 0, failures: 0, provider: 'openai-codex', model: MODEL, groundHash: hashes.groundHash, framingHash: hashes.framingHash, requestCap: AGENT_PROFILE.contextBudgetTokens, accounting: 'conservative-UTF8-byte-upper-bound-plus-envelope-and-output-reserve; not physical-capacity proof' });
  const store = JsStore.create({ path: sessions.getStorePath(session.id) }); let cm: ContextManager | undefined;
  let baseline: Omit<Inspection, 'nativeFiles' | 'checkpointHash'>;
  try {
    const strategy = makeStrategy(framing); cm = await ContextManager.open({ store, strategy, namespace: NAMESPACE });
    baseline = inspectOpen(out, cm, strategy, inventory, checkpoint, framing); cm.sync();
    if (baseline.nativeMessages || baseline.leaves || baseline.merges || baseline.l1Queue || baseline.mergeQueue) throw new Error('Fresh candidate native baseline is not empty');
  } finally { try { cm?.close(); } finally { store.close(); privateTree(out); } }
  writeInspection(out, inventory, baseline!);
  const result = await status(out);
  return { ...result, prepared: true, ...(corpus.derivation ? { derived: true } : {}), errors: corpus.sources.filter(source => source.error).length, invocation: `MODEL=${MODEL} DATA_DIR=${shellArg(join(out, 'data'))} bun src/index.ts ${shellArg(join(out, 'recipe.json'))}` };
}
export async function prepare(options: PrepareOptions): Promise<Record<string, unknown>> {
  if (![options.manifest, options.recipe, options.framing].every(path => typeof path === 'string' && path.trim())) throw new Error('Explicit manifest, recipe and parent framing paths are required');
  const out = newOutput(options.out);
  const manifest = loadManifest(resolve(options.manifest));
  regular(resolve(options.recipe)); regular(resolve(options.framing));
  // Avoid the loader's env/auth interpolation path: the supplied full recipe
  // must ALREADY contain its assembled local system prompt.
  const rawRecipe = JSON.parse(readFileSync(options.recipe, 'utf8')) as Recipe;
  if (typeof rawRecipe.agent?.systemPrompt !== 'string' || !rawRecipe.agent.systemPrompt.trim() || rawRecipe.agent.systemPrompt.includes('${') || /^https?:\/\/\S+$/.test(rawRecipe.agent.systemPrompt)) throw new Error('Provide the actual full recipe with a resolved local carrier system prompt');
  if (rawRecipe.agent.provider !== 'openai-codex' || rawRecipe.agent.model !== MODEL) throw new Error('Full recipe must explicitly name openai-codex/gpt-6.1-sol');
  const ground = rawRecipe.agent.systemPrompt; const framing = readFileSync(options.framing, 'utf8');
  if (!framing.trim()) throw new Error('Parent-authored backfill framing is required');
  // Literal credentials in a recipe are not a supported preparation input.
  const forbidden = /(?:^token$|api.?key|access.?token|refresh.?token|password|secret|authorization|account.?id|(?:^|_)(?:key|token|credentials?|auth|bearer)(?:_|$))/i;
  const privateReference = /^\$\{[A-Za-z_][A-Za-z0-9_]*(?::-)?\}$/;
  const check = (v: unknown): void => {
    if (v && typeof v === 'object') for (const [key, val] of Object.entries(v)) {
      if (forbidden.test(key) && typeof val === 'string' && val && !privateReference.test(val)) throw new Error('Recipe literal credential fields are forbidden; use private runtime auth');
      check(val);
    }
  };
  check(rawRecipe);
  mkdirSync(out, { mode: 0o700 });
  return withWriter(out, async () => {
    const normalized = normalizeSources(manifest, out);
    const recipe = safeRecipePaths(rawRecipe, dirname(resolve(options.recipe))) as Recipe;
    return initializeCandidate(out, { manifest, ...normalized }, recipe, ground, framing);
  });
}
export async function deriveFiltered(options: DeriveOptions): Promise<Record<string, unknown>> {
  if (![options.parent, options.excludeEvents, options.framing].every(path => typeof path === 'string' && path.trim())) throw new Error('Explicit parent, exclusion decision and parent framing paths are required');
  const parent = rootPath(options.parent); const out = newOutput(options.out);
  if (out.startsWith(`${parent}/`)) throw new Error('Derived output must be outside the immutable parent candidate');
  const state = load(parent);
  if (state.derivation) throw new Error('Already filtered candidate cannot be derived; derive the complete reviewed union from the immutable original parent');
  const sessions = new SessionManager(join(parent, 'data')); regular(join(parent, 'data/sessions.json'));
  const index = sessions.load(); const nativePath = sessions.getStorePath(state.inventory.sessionId);
  if (index.version !== 1 || index.sessions[state.inventory.sessionId]?.id !== state.inventory.sessionId || !lstatSync(nativePath).isDirectory() || realpathSync(nativePath) !== nativePath) throw new Error('Invalid origin native session identity');
  const decision = validateDecision(jsonFile<unknown>(resolve(options.excludeEvents)));
  if (decision.originFingerprint !== state.inventory.fingerprint || decision.originSessionId !== state.inventory.sessionId) throw new Error('Exclusion decision origin does not match the immutable parent candidate');
  regular(resolve(options.framing)); const framing = readFileSync(options.framing, 'utf8');
  if (!framing.trim()) throw new Error('Parent-authored backfill framing is required');
  const excluded = new Set(decision.eventKeys); const eventByKey = new Map(state.events.map(event => [event.key, event]));
  if (decision.eventKeys.some(key => !eventByKey.has(key))) throw new Error('Exclusion decision contains an unknown exact event key');
  const omitted = state.events.filter(event => excluded.has(event.key)); const retained = state.events.filter(event => !excluded.has(event.key));
  // Supplied raw-count metadata can be checked when the parent sidecar has
  // complete shard mappings. It never selects events or reads native bodies.
  const completeMapping = omitted.every(event => state.checkpoint.mapping?.[event.key]?.ranges?.at(-1)?.end === event.text.length);
  if (decision.rawSourceCount !== undefined && completeMapping && decision.rawSourceCount !== omitted.reduce((sum, event) => sum + state.checkpoint.mapping[event.key].nativeIds.length, 0)) throw new Error('Exclusion decision raw-source count does not match complete parent shard mappings');
  if (state.dispositions.some(row => row.exclusion) || state.sources.some(source => source.lineage)) throw new Error('Parent already carries an exclusion policy; derive from the immutable original parent');
  const decisionHash = sha256(canonical(decision)); const coverage = omissionCoverage(omitted, state.dispositions);
  const rowKeys = new Map<number, string[]>();
  for (const event of coverage) for (const index of event.dispositionIndexes) rowKeys.set(index, [...(rowKeys.get(index) ?? []), event.key]);
  const records: OmissionReceipt['records'] = [];
  const dispositions = state.dispositions.map((row, index): Disposition => {
    const eventKeys = rowKeys.get(index);
    if (!eventKeys) return row;
    if (retained.some(event => event.source === row.source && event.recordIds.includes(row.recordId))) throw new Error('Excluded and retained event disposition lineage overlaps');
    records.push({ index, source: row.source, recordId: row.recordId, line: row.line, hash: row.hash, previousState: row.state });
    return { ...row, state: 'exclude', reason: decision.reason, exclusion: { receipt: 'omissions.json', decisionHash, eventKeys, previousState: row.state, previousReason: row.reason } };
  });
  const omissions: OmissionReceipt = { version: 1, originFingerprint: decision.originFingerprint, originSessionId: decision.originSessionId, decisionHash, decision, events: coverage, records, excludedLogicalEvents: omitted.length, excludedDispositionRecords: records.length, contributingSources: new Set(omitted.map(event => event.source)).size };
  const sourceCounts = state.sources.map(source => ({ id: source.id, originEventCount: state.events.filter(event => event.source === source.id).length, retainedEventCount: retained.filter(event => event.source === source.id).length, excludedEventCount: omitted.filter(event => event.source === source.id).length }));
  if (new Set(state.sources.map(source => source.id)).size !== state.sources.length || state.sources.length !== state.manifest.sources.length || state.sources.some((source, index) => source.id !== state.manifest.sources[index].id || source.path !== state.manifest.sources[index].path || source.eventCount !== sourceCounts[index].originEventCount) || state.events.some(event => !state.sources.some(source => source.id === event.source))) throw new Error('Parent normalized source lineage is inconsistent');
  const derivation: DerivationReceipt = { version: 1, origin: { fingerprint: state.inventory.fingerprint, sessionId: state.inventory.sessionId, eventCount: state.events.length, hashes: inventoryHashes(state.inventory) }, events: state.events.map(eventLineage), sources: sourceCounts, originDispositionsCanonicalHash: sha256(canonical(state.dispositions)), retainedEligibleEvents: retained.length, excludedLogicalEvents: omitted.length, excludedDispositionRecords: records.length, originals: 'parent-only', nativeState: 'fresh-empty', summaryReuse: false };
  const sources = state.sources.map((source, index): SourceSnapshot => {
    const { snapshot: _snapshot, partialTailSnapshot: _tail, inode: _inode, ...attribution } = source;
    return { ...attribution, active: false, eventCount: sourceCounts[index].retainedEventCount, lineage: { originFingerprint: decision.originFingerprint, originSessionId: decision.originSessionId, originEventCount: sourceCounts[index].originEventCount, excludedEventCount: sourceCounts[index].excludedEventCount, originals: 'parent-only' } };
  });
  const manifest: SourceManifest = { ...state.manifest, sources: state.manifest.sources.map(source => { const { mediaRoot: _mediaRoot, ...attribution } = source; return { ...attribution, active: false }; }), derivation: { version: 1, originFingerprint: decision.originFingerprint, originSessionId: decision.originSessionId, decisionHash, omissions: 'omissions.json', lineage: 'derivation.json' } };
  const media = new Map<string, MediaReference>();
  for (const event of retained) for (const reference of event.media) {
    if (reference.status === 'validated') { mediaPath(parent, reference); media.set(reference.snapshot!, reference); }
    else if (reference.snapshot !== undefined) throw new Error('Unvalidated retained media has a snapshot');
  }
  mkdirSync(out, { mode: 0o700 });
  return withWriter(out, async () => {
    mkdirSync(join(out, 'sources/media'), { recursive: true, mode: 0o700 });
    for (const reference of media.values()) {
      const bytes = readFileSync(mediaPath(parent, reference));
      if (sha256(bytes) !== reference.hash) throw new Error('Retained media changed during derivation');
      writeFileSync(join(out, reference.snapshot!), bytes, { mode: 0o600, flag: 'wx' });
    }
    return initializeCandidate(out, { manifest, events: retained, sources, dispositions, omissions, derivation }, state.recipe, state.ground, framing);
  });
}
class BackfillStrategy extends AutobiographicalStrategy {
  protected override rebuildChunks(store: MessageStoreView, dryRun = false, archivalThroughId?: MessageId): void {
    // During append/reopen, restore only sealed native records. Ordinary live
    // chunking's four-message minimum must not coalesce long archival shards
    // before the explicit target-sized batch seal. No profile value changes.
    super.rebuildChunks(store, dryRun, archivalThroughId, archivalThroughId === undefined);
  }
  effectiveProfile(): Record<string, unknown> {
    return Object.fromEntries(Object.keys(PROFILE).map(key => [key, key === 'type' ? this.name : this.config[key as keyof typeof this.config]]));
  }
}
function makeStrategy(framing: string): BackfillStrategy {
  // Supplemental routing is not part of the immutable prepared profile.
  // Sealed native chunks always bypass mechanical thin-turn stubs themselves.
  return new BackfillStrategy({ ...PROFILE, archivalCyberPolicyFallbackModel: ARCHIVAL_CYBER_POLICY_FALLBACK_MODEL,
    identityReminder: framing, autoTickOnNewMessage: false, logEffectiveConfig: false });
}
function nativeEnvelope(event: HistoricalEvent, start: number, end: number): string {
  return `[Historical source fragment ${xml(canonical(safeData({ key: event.key, role: event.role, voice: event.voice, precision: event.precision, indexingAnchor: event.indexingAnchor, sourceCharStart: start, sourceCharEnd: end, sourceChars: event.text.length, partialExtract: start > 0 || end < event.text.length, ancestryGap: event.ancestryGap, branchHeads: event.branchHeads })))}]\n`;
}
function mappingFromNative(events: HistoricalEvent[], messages: StoredMessage[], inventory: Inventory): Record<string, Mapping> {
  const byKey = new Map<string, StoredMessage[]>(); const eventByKey = new Map(events.map(e => [e.key, e]));
  let nativeEvent = 0; let nativeOffset = 0;
  for (const message of messages) {
    const key = message.metadata?.backfillKey;
    if (typeof key !== 'string' || message.metadata?.backfillFingerprint !== inventory.fingerprint) throw new Error('Candidate contains a foreign/unmapped native message');
    const event = eventByKey.get(key);
    if (!event || message.metadata?.backfillPayloadHash !== event.payloadHash || message.timestamp.getTime() !== event.timestampMs) throw new Error('Already included source-key payload conflicts with prepared corpus');
    if (inventory.derivationHash) {
      if (message.metadata?.sourceId !== key || canonical(message.metadata?.sourceRecordIds) !== canonical(event.recordIds)) throw new Error('Derived native source-record lineage conflicts with prepared corpus');
      const start = message.metadata?.sourceCharStart; const end = message.metadata?.sourceCharEnd;
      if (events[nativeEvent]?.key !== key || typeof start !== 'number' || typeof end !== 'number' || !Number.isInteger(start) || !Number.isInteger(end) || start !== nativeOffset || end <= start || end > event.text.length) throw new Error('Derived native chronological shard order conflicts with prepared corpus');
      const envelope = nativeEnvelope(event, start, end);
      if (message.metadata?.sourceEnvelopeChars !== envelope.length || message.content.length !== 1 || message.content[0].type !== 'text' || message.content[0].text !== envelope + event.text.slice(start, end)) throw new Error('Derived native source envelope/content conflicts with prepared corpus');
      nativeOffset = end;
      if (end === event.text.length) { nativeEvent++; nativeOffset = 0; }
    }
    const group = byKey.get(key) ?? []; group.push(message); byKey.set(key, group);
  }
  const mapping: Record<string, Mapping> = {};
  let foundGap = false;
  for (const event of events) {
    const group = byKey.get(event.key);
    if (!group) { foundGap = true; continue; }
    if (foundGap) throw new Error('Out-of-order committed source history; rebuild/new-candidate required');
    const text = group.map(m => m.content.filter(b => b.type === 'text').map(b => b.text).join('').slice(Number(m.metadata?.sourceEnvelopeChars) || 0)).join('');
    // Ingest stores one explicit shard per append; no half-logical-event ambiguity.
    const ranges = group.map(m => ({ id: m.id, start: Number(m.metadata?.sourceCharStart), end: Number(m.metadata?.sourceCharEnd) }));
    let end = 0;
    for (const range of ranges) { if (range.start !== end || !Number.isInteger(range.end) || range.end <= range.start) throw new Error('Native source shard ranges are inconsistent'); end = range.end; }
    if (text !== event.text.slice(0, end)) throw new Error('Already included source-key content conflicts');
    mapping[event.key] = { payloadHash: event.payloadHash, nativeIds: group.map(m => m.id), textHash: sha256(text), timestampMs: event.timestampMs, ranges };
  }
  return mapping;
}
/** Explicit UTF-safe shards, so a killed multi-shard append can resume at its
 * persisted source range rather than append the same logical event twice. */
function nextShard(text: string, offset: number): { text: string; end: number } {
  const maxBytes = PROFILE.targetChunkTokens * 4;
  let end = offset; let bytes = 0;
  while (end < text.length) {
    const point = text.codePointAt(end)!;
    const width = point <= 0x7F ? 1 : point <= 0x7FF ? 2 : point <= 0xFFFF ? 3 : 4;
    if (bytes + width > maxBytes) break;
    bytes += width; end += point > 0xFFFF ? 2 : 1;
  }
  if (end < text.length) {
    const seam = text.lastIndexOf('\n', end - 1);
    if (seam > offset + Math.floor((end - offset) * 0.75)) end = seam + 1;
  }
  return { text: text.slice(offset, end), end };
}
function filesHash(root: string): Record<string, string> {
  if (!lstatSync(root).isDirectory() || realpathSync(root) !== root) throw new Error('Native candidate store must be a real nonsymlink directory');
  const hashes: Record<string, string> = {};
  const visit = (directory: string, prefix: string): void => {
    for (const name of readdirSync(directory).sort()) {
      const path = join(directory, name); const st = lstatSync(path);
      if (st.isSymbolicLink()) throw new Error('Native candidate store contains unexpected symlink');
      const key = prefix ? `${prefix}/${name}` : name;
      if (st.isDirectory()) visit(path, key);
      else if (st.isFile() && !/lock/i.test(name)) hashes[key] = bytesHash(path);
    }
  };
  visit(root, ''); return hashes;
}
function inspectOpen(root: string, cm: ContextManager, strategy: BackfillStrategy, inventory: Inventory, checkpoint: Checkpoint, framing: string): Omit<Inspection, 'nativeFiles' | 'checkpointHash'> {
  const messages = cm.getAllMessages(); const rawIds = new Set(messages.map(m => m.id));
  const summaries = (cm.getStore().getStateJson(`${NAMESPACE}/autobio:summaries`) ?? []) as SummaryEntry[];
  const summaryById = new Map(summaries.map(summary => [summary.id, summary]));
  const chunks = (cm.getStore().getStateJson(`${NAMESPACE}/autobio:chunks`) ?? []) as Array<{ id: string; sourceIds: string[]; compressed: boolean; summaryId?: string }>;
  const owned = new Set(chunks.flatMap(c => c.sourceIds)); const progress = strategy.getProgressSnapshot();
  const chunkBySummary = new Map(chunks.filter(chunk => chunk.compressed && chunk.summaryId).map(chunk => [chunk.summaryId!, chunk]));
  const validAuthor = (summary: SummaryEntry): boolean => {
    const provenance = summary.provenance;
    // Legacy unfiltered Sol labels remain valid without old preimages. A fresh
    // derived store has no legacy lineage to salvage: every mint must be new.
    if (provenance?.model === MODEL) {
      if (provenance.archivalCyberPolicyFallback) return false;
      if (!inventory.derivationHash) return true;
    } else {
      if (provenance?.model !== ARCHIVAL_CYBER_POLICY_FALLBACK_MODEL || provenance.stopReason !== 'end_turn') return false;
      const route = provenance.archivalCyberPolicyFallback;
      if (!route || Object.keys(route).sort().join(',') !== 'primaryModel,reason' || route.primaryModel !== MODEL || route.reason !== 'cyber_policy') return false;
    }
    try {
      const accepted = getMintRequestByHash(cm.getStore(), provenance.requestHash);
      if (!accepted || sha256(JSON.stringify(accepted)) !== provenance.requestHash ||
          accepted.config.model !== provenance.model || accepted.tools?.length ||
          Object.keys(accepted.providerParams ?? {}).length || sha256(accepted.system ?? '') !== inventory.groundHash ||
          !accepted.messages.at(-1)?.content.some(block => block.type === 'text' && block.text.includes(framing))) return false;
      const directive = readArchivalMemoryProvenance(accepted.messages, accepted.config.model, accepted.config.maxTokens);
      return !!directive && directive.operation === (summary.level === 1 ? 'l1' : 'merge');
    } catch { return false; }
  };
  let valid = new Set(chunks.map(chunk => chunk.id)).size === chunks.length && summaryById.size === summaries.length && owned.size === chunks.reduce((total, chunk) => total + chunk.sourceIds.length, 0) && summaries.every(summary =>
    summary.sourceIds.length > 0 && summary.sourceIds.every((id, index) => summary.sourceIds.indexOf(id) === index && (summary.level === 1 ? rawIds.has(id) : summaryById.get(id)?.level === summary.level - 1 && summaryById.get(id)?.mergedInto === summary.id)) &&
    rawIds.has(summary.sourceRange.first) && rawIds.has(summary.sourceRange.last) && (!summary.mergedInto || summaryById.get(summary.mergedInto)?.sourceIds.includes(summary.id)) && validAuthor(summary)) &&
    chunks.every(chunk => chunk.sourceIds.length > 0 && chunk.sourceIds.every(id => rawIds.has(id)) && (!chunk.compressed || (() => {
      const leaf = chunk.summaryId ? summaryById.get(chunk.summaryId) : undefined;
      return leaf?.level === 1 && leaf.sourceIds.length === chunk.sourceIds.length && leaf.sourceIds.every((id, index) => id === chunk.sourceIds[index]);
    })())) && summaries.filter(summary => summary.level === 1).length === chunkBySummary.size && chunkBySummary.size === chunks.filter(chunk => chunk.compressed).length &&
    summaries.every(summary => summary.level !== 1 || chunkBySummary.has(summary.id));
  if (valid) {
    const covered = new Set<string>(); let count = 0;
    const visit = (summary: SummaryEntry): void => {
      if (summary.level === 1) { for (const id of summary.sourceIds) { count++; covered.add(id); } }
      else for (const id of summary.sourceIds) visit(summaryById.get(id)!);
    };
    for (const summary of summaries) {
      const first = summary.level === 1 ? summary.sourceIds[0] : summaryById.get(summary.sourceIds[0])!.sourceRange.first;
      const last = summary.level === 1 ? summary.sourceIds.at(-1) : summaryById.get(summary.sourceIds.at(-1)!)!.sourceRange.last;
      if (first !== summary.sourceRange.first || last !== summary.sourceRange.last) valid = false;
      if (!summary.mergedInto) visit(summary);
    }
    const expected = chunks.filter(chunk => chunk.compressed).flatMap(chunk => chunk.sourceIds);
    valid = valid && count === covered.size && count === expected.length && expected.every(id => covered.has(id));
  }
  let dateMin: number | undefined; let dateMax: number | undefined;
  for (const message of messages) { const time = message.timestamp.getTime(); dateMin = dateMin === undefined ? time : Math.min(dateMin, time); dateMax = dateMax === undefined ? time : Math.max(dateMax, time); }
  const mergeQuarantine = strategy.getMergeQuarantineStatus().count;
  return { fingerprint: inventory.fingerprint, nativeMessages: messages.length, imported: Object.keys(checkpoint.mapping).length, nextEvent: checkpoint.nextEvent,
    leaves: summaries.filter(s => s.level === 1).length, merges: summaries.filter(s => s.level > 1).length,
    l1Queue: progress.l1QueueLength, mergeQueue: progress.mergeQueueLength, unsealed: messages.filter(m => !owned.has(m.id)).length,
    quarantine: strategy.getCompressionQuarantineStatus().count + mergeQuarantine, mergeQuarantine, sourceLinksValid: valid,
    dateMin, dateMax, effectiveProfile: strategy.effectiveProfile(),
    summaries: summaries.map(s => ({ id: s.id, level: s.level, sourceIds: s.sourceIds, sourceRange: s.sourceRange, mergedInto: s.mergedInto, model: s.provenance?.model,
      ...(s.provenance?.archivalCyberPolicyFallback ? { archivalCyberPolicyFallback: s.provenance.archivalCyberPolicyFallback } : {}) })), updated: new Date().toISOString() };
}
function writeInspection(root: string, inventory: Inventory, inspection: Omit<Inspection, 'nativeFiles' | 'checkpointHash'>): void {
  const nativeFiles = filesHash(new SessionManager(join(root, 'data')).getStorePath(inventory.sessionId));
  durableJson(join(root, 'inspection.json'), { ...inspection, nativeFiles, checkpointHash: bytesHash(join(root, 'checkpoint.json')) });
}
export async function ingest(input: string, options: ApplyOptions): Promise<Record<string, unknown>> {
  const root = rootPath(input); if (!options.apply) return { ...(await status(root)), dryRun: true };
  if (options.maxEvents !== undefined && (!Number.isInteger(options.maxEvents) || options.maxEvents <= 0)) throw new Error('max-events must be a positive integer');
  return withWriter(root, async () => {
    const state = load(root); const { inventory, events, checkpoint } = state;
    const store = JsStore.open({ path: new SessionManager(join(root, 'data')).getStorePath(inventory.sessionId) });
    let cm: ContextManager | undefined; let inspection: Omit<Inspection, 'nativeFiles' | 'checkpointHash'> | undefined;
    try {
      const strategy = makeStrategy(state.framing); cm = await ContextManager.open({ store, strategy, namespace: NAMESPACE }); cm.setSystemPrompt(state.ground);
      const mapping = mappingFromNative(events, cm.getAllMessages(), inventory);
      if (state.derivation && !inspectOpen(root, cm, strategy, inventory, checkpoint, state.framing).sourceLinksValid) throw new Error('Derived native memory lineage is invalid; inference/import refused');
      // Recovery uses native source keys/ranges, never just the sidecar frontier.
      checkpoint.mapping = mapping;
      let next = 0;
      while (next < events.length && mapping[events[next].key]?.ranges.at(-1)?.end === events[next].text.length) next++;
      const start = next; const end = Math.min(events.length, start + (options.maxEvents ?? 100));
      for (; next < end; next++) {
        const event = events[next]; let offset = mapping[event.key]?.ranges.at(-1)?.end ?? 0;
        if (offset) for (const range of mapping[event.key].ranges) cm.finalizeArchivalBatch(range.id);
        while (offset < event.text.length) {
          const shard = nextShard(event.text, offset);
          const envelope = nativeEnvelope(event, offset, shard.end);
          const id = cm.addMessage(event.participant, [{ type: 'text', text: envelope + shard.text }], { sourceId: event.key, historicalSource: true, backfillKey: event.key, backfillPayloadHash: event.payloadHash, backfillFingerprint: inventory.fingerprint, sourceCharStart: offset, sourceCharEnd: shard.end, sourceEnvelopeChars: envelope.length, sourceRecordIds: event.recordIds, sourceTimestamp: event.originalTimestamp, timestampPrecision: event.precision, indexingAnchor: event.indexingAnchor, authoredDate: event.authoredDate, coveredDateRange: event.coveredDateRange, mediaReferences: event.media }, undefined, { timestampMs: event.timestampMs });
          cm.sync(); options.interrupt?.('append-before-checkpoint', cm);
          if (offset > 0 || shard.end < event.text.length) cm.finalizeArchivalBatch(id);
          const entry = checkpoint.mapping[event.key] ?? { payloadHash: event.payloadHash, nativeIds: [], textHash: '', timestampMs: event.timestampMs, ranges: [] };
          entry.nativeIds.push(id); entry.ranges.push({ id, start: offset, end: shard.end }); entry.textHash = sha256(event.text.slice(0, shard.end)); checkpoint.mapping[event.key] = entry;
          offset = shard.end;
        }
        checkpoint.nextEvent = next + 1;
      }
      // A recovered append can already complete the frontier even when no new
      // events append this invocation. It still needs a native archival seal.
      checkpoint.mapping = mappingFromNative(events, cm.getAllMessages(), inventory); checkpoint.nextEvent = next;
      const all = cm.getAllMessages(); const last = all.at(-1)?.id;
      if (last) {
        cm.finalizeArchivalBatch(last); cm.sync();
        if (checkpoint.batches.at(-1)?.lastId !== last) checkpoint.batches.push({ start, end: next, lastId: last });
      }
      durableJson(join(root, 'checkpoint.json'), checkpoint);
      options.interrupt?.('checkpoint-before-inspection', cm);
      inspection = inspectOpen(root, cm, strategy, inventory, checkpoint, state.framing); cm.sync();
    } finally { try { cm?.close(); } finally { store.close(); if (state.derivation) privateTree(root); } }
    if (inspection) writeInspection(root, inventory, inspection);
    return status(root);
  });
}
interface RequestAudit {
  version: number; requests: number; responses: number; failures: number;
  lastDrain?: { primaryRequests: number; fallbackRequests: number; primaryResponses: number; fallbackResponses: number };
  [key: string]: unknown;
}
interface TerminalClassification {
  kind: 'membrane' | 'untyped' | 'local-fallback-halt';
  localCode?: 'archival_cyber_policy_fallback_halt';
  outcome?: ArchivalCyberPolicyFallbackHalt['outcome'];
  requestHash?: string;
  evidenceHash?: string;
  primaryModel?: typeof MODEL;
  fallbackModel?: typeof ARCHIVAL_CYBER_POLICY_FALLBACK_MODEL;
  type: MembraneError['type'];
  retryable?: boolean;
  httpStatus?: number;
  providerErrorCode?: typeof ARCHIVAL_MEMORY_LOCAL_CAP_CODE | 'cyber_policy';
}
function classifyTerminalError(error: unknown): TerminalClassification {
  const classification: TerminalClassification = { kind: 'untyped', type: 'unknown' };
  try {
    if (error instanceof ArchivalCyberPolicyFallbackHalt && !types.isProxy(error)) return {
      kind: 'local-fallback-halt', type: error.errorType, retryable: false,
      localCode: error.code, outcome: error.outcome, requestHash: error.requestHash,
      evidenceHash: error.evidenceHash, primaryModel: error.primaryModel, fallbackModel: error.fallbackModel,
      ...(error.httpStatus !== undefined ? { httpStatus: error.httpStatus } : {}),
      ...(error.providerErrorCode ? { providerErrorCode: error.providerErrorCode } : {}),
    };
    if (!(error instanceof MembraneError)) return classification;
    classification.kind = 'membrane';
    if (types.isProxy(error)) return { kind: 'membrane', type: 'unknown' };
    // Only inspect own data fields: accessors must not run inside the safe catch.
    const type: unknown = Object.getOwnPropertyDescriptor(error, 'type')?.value;
    const retryable: unknown = Object.getOwnPropertyDescriptor(error, 'retryable')?.value;
    const httpStatus: unknown = Object.getOwnPropertyDescriptor(error, 'httpStatus')?.value;
    const providerErrorCode: unknown = Object.getOwnPropertyDescriptor(error, 'providerErrorCode')?.value;
    // Runtime allowlist: normalized fields can still carry invalid provider values.
    switch (type) {
      case 'rate_limit': case 'context_length': case 'invalid_request': case 'auth':
      case 'server': case 'network': case 'timeout': case 'abort': case 'safety':
      case 'unsupported': case 'unknown': classification.type = type;
    }
    if (typeof retryable === 'boolean') classification.retryable = retryable;
    if (typeof httpStatus === 'number' && Number.isInteger(httpStatus) && httpStatus >= 100 && httpStatus <= 599) classification.httpStatus = httpStatus;
    if (type === 'context_length' && retryable === false && providerErrorCode === ARCHIVAL_MEMORY_LOCAL_CAP_CODE) classification.providerErrorCode = ARCHIVAL_MEMORY_LOCAL_CAP_CODE;
    if (type === 'safety' && retryable === false && providerErrorCode === 'cyber_policy') classification.providerErrorCode = 'cyber_policy';
  } catch {
    // Even a hostile reflection trap cannot replace the caught drain failure.
    return { kind: classification.kind, type: 'unknown' };
  }
  return classification;
}
const MEMORY_FORMATTER = new OpenAIResponsesFormatter();
export class BackfillMembrane extends Membrane {
  constructor(adapter: ProviderAdapter, config: MembraneConfig, private readonly observeMemoryRequest?: (normalized: NormalizedRequest, body: unknown) => void) {
    super(adapter, config);
  }
  override async complete(request: NormalizedRequest, options?: CompleteOptions): Promise<NormalizedResponse> {
    try { return await super.complete(request, { ...options, formatter: MEMORY_FORMATTER, onRequest: body => {
      this.observeMemoryRequest?.(request, body);
      options?.onRequest?.(body);
    } }); }
    catch (error) {
      // Native strategies log thrown errors. A provider/auth error can contain
      // raw response/account material: retain an evidence hash, never replay it
      // into console telemetry or a summary. The failure still throws/retries.
      const classification = classifyTerminalError(error);
      const ownMessage = error && typeof error === 'object' && !types.isProxy(error)
        ? Object.getOwnPropertyDescriptor(error, 'message')?.value : undefined;
      const privateMessage = typeof ownMessage === 'string' ? ownMessage : 'untyped memory request failure';
      const hash = sha256(privateMessage);
      const message = `Codex memory request failed [${hash.slice(0, 16)}]; work remains resumable`;
      if (classification.kind === 'membrane') {
        // Own, finite controls only. The fixed cyber_policy code survives;
        // arbitrary provider codes/prose and raw request/response never do.
        const carrierRejected = classification.type === 'invalid_request' && /thinking|reasoning/i.test(privateMessage);
        throw new MembraneError({
          type: classification.type, retryable: classification.retryable === true, httpStatus: classification.httpStatus,
          message: carrierRejected ? `${message}; invalid_request: reasoning carrier rejected` : message,
          ...(classification.providerErrorCode ? { providerErrorCode: classification.providerErrorCode } : {}),
          rawError: undefined,
        });
      }
      throw new Error(message);
    }
  }
}
export function validateMemoryGround(normalized: NormalizedRequest, request: ProviderRequest, ground: string, framing: string): void {
  if (!isArchivalMemoryModel(normalized.config.model) || request.model !== normalized.config.model) throw new Error('Memory model must match the bounded archival routing policy');
  const providerSystem = typeof request.system === 'string' ? request.system : Array.isArray(request.system) ? request.system.map(block => block && typeof block === 'object' && 'text' in block && typeof block.text === 'string' ? block.text : '').join('') : undefined;
  if (normalized.system !== ground || providerSystem !== ground) throw new Error('Memory request is missing exact carrier ground');
  // Framing is the existing identityReminder directive, not primary chat ground.
  // Check both sides of formatting without duplicating it in the system prompt.
  const containsFraming = (value: unknown): boolean => typeof value === 'string' ? value.includes(framing) : Array.isArray(value) ? value.some(containsFraming) : !!value && typeof value === 'object' && Object.values(value).some(containsFraming);
  if (!containsFraming(normalized.messages) || !containsFraming(request.messages)) throw new Error('Memory request is missing exact parent framing');
  if (normalized.tools?.length || request.tools?.length) throw new Error('Archival memory requests must not have active tools');
  if (Object.keys(request.extra ?? {}).some(key => key !== 'normalizedMessages')) throw new Error('Unexpected provider overrides in archival memory request');

}
export async function drain(input: string, options: ApplyOptions): Promise<Record<string, unknown>> {
  const root = rootPath(input); if (!options.apply) return { ...(await status(root)), dryRun: true };
  if (options.maxSteps !== undefined && (!Number.isInteger(options.maxSteps) || options.maxSteps <= 0)) throw new Error('max-steps must be a positive integer');
  return withWriter(root, async () => {
    const state = load(root); const { inventory, checkpoint } = state;
    let adapter: (ProviderAdapter & { dispose(): void }) | undefined; let cm: ContextManager | undefined; let store: JsStore | undefined;
    let inspection: Omit<Inspection, 'nativeFiles' | 'checkpointHash'> | undefined;
    const audit = jsonFile<RequestAudit>(join(root, 'audit.json'));
    audit.routingPolicy = ARCHIVAL_ROUTING_POLICY;
    audit.lastDrain = { primaryRequests: 0, fallbackRequests: 0, primaryResponses: 0, fallbackResponses: 0 };
    try {
      if (!options.adapterFactory && (!process.env.CODEX_HOME || !process.env.CODEX_BINARY)) throw new Error('Provision private CODEX_HOME and explicit supported CODEX_BINARY before real drain (no API-key fallback)');
      adapter = options.adapterFactory?.() ?? new CodexSubscriptionAdapter({ codexHome: process.env.CODEX_HOME, codexBinary: process.env.CODEX_BINARY, onLoginRequired: () => { throw new Error('Provision subscription login privately before backfill; interactive credential bootstrap is not performed'); } });
      const membrane = new BackfillMembrane(adapter, { formatter: MEMORY_FORMATTER, assistantParticipant: 'liv', retry: { maxRetries: 0, overloaded: { maxRetries: 0 } }, hooks: {
        beforeRequest: (normalized, request) => { validateMemoryGround(normalized, request as ProviderRequest, state.ground, state.framing); },
        afterResponse: (response: NormalizedResponse) => {
          const model = response.details.model;
          validateArchivalServedModel(response.raw.response, model.requested, model.actual, model.perRound);
          audit.lastDrain![model.requested === ARCHIVAL_CYBER_POLICY_FALLBACK_MODEL ? 'fallbackResponses' : 'primaryResponses']++;
          audit.responses++; audit.lastResponse = { primaryModel: MODEL, requestedModel: model.requested, model: model.actual,
            route: model.requested === ARCHIVAL_CYBER_POLICY_FALLBACK_MODEL ? 'cyber-policy-fallback' : 'primary',
            provider: model.provider, stopReason: response.stopReason, usageSource: 'provider-normalized-token-usage', usage: response.details.usage };
          durableJson(join(root, 'audit.json'), audit); return response;
        },
        onError: () => 'abort',
      }, logger: { debug() {}, info() {}, warn(message) { console.error(`Membrane warning [${sha256(message).slice(0, 12)}]`); }, error(message) { console.error(`Membrane provider error [${sha256(message).slice(0, 12)}]`); } } }, (normalized, body) => {
        const provenance = readArchivalMemoryProvenance(normalized.messages, normalized.config.model, normalized.config.maxTokens);
        if (!provenance) throw new ArchivalMemoryBudgetError('invalid-provenance');
        let accounting: ArchivalMemoryAccounting;
        try { accounting = enforceArchivalMemoryBudget(body, provenance); }
        catch (error) {
          if (error instanceof ArchivalMemoryBudgetError && error.accounting) {
            audit.lastRefused = error.accounting;
            durableJson(join(root, 'audit.json'), audit);
          }
          throw error;
        }
        audit.requests++;
        audit.lastDrain![normalized.config.model === ARCHIVAL_CYBER_POLICY_FALLBACK_MODEL ? 'fallbackRequests' : 'primaryRequests']++;
        audit.lastRequest = { provider: adapter!.name, primaryModel: MODEL, model: normalized.config.model,
          route: normalized.config.model === ARCHIVAL_CYBER_POLICY_FALLBACK_MODEL ? 'cyber-policy-fallback' : 'primary', groundHash: inventory.groundHash, framingHash: inventory.framingHash, requestHash: sha256(canonical(normalized)), transmittedBodyHash: sha256(JSON.stringify(body)), requestPhase: 'admitted-final-body-before-auth-fetch', ...accounting };
        durableJson(join(root, 'audit.json'), audit);
      });
      store = JsStore.open({ path: new SessionManager(join(root, 'data')).getStorePath(inventory.sessionId) });
      const strategy = makeStrategy(state.framing); cm = await ContextManager.open({ store, strategy, membrane, namespace: NAMESPACE }); cm.setSystemPrompt(state.ground);
      checkpoint.mapping = mappingFromNative(state.events, cm.getAllMessages(), inventory);
      if (state.derivation && !inspectOpen(root, cm, strategy, inventory, checkpoint, state.framing).sourceLinksValid) throw new Error('Derived native memory lineage is invalid; inference refused');
      let next = 0; while (next < state.events.length && checkpoint.mapping[state.events[next].key]?.ranges.at(-1)?.end === state.events[next].text.length) next++;
      checkpoint.nextEvent = next;
      if (cm.getAllMessages().length && next === 0) throw new Error('Partial logical event must resume ingest before memory formation');
      const messages = cm.getAllMessages();
      if (messages.length && checkpoint.mapping[state.events[next - 1].key]?.nativeIds.at(-1) !== messages.at(-1)?.id) throw new Error('Partial event at frontier; resume ingest before drain');
      if (messages.length) cm.finalizeArchivalBatch(messages.at(-1)!.id);
      durableJson(join(root, 'checkpoint.json'), checkpoint); cm.sync();
      for (let step = 0; step < (options.maxSteps ?? Number.MAX_SAFE_INTEGER); step++) {
        const before = strategy.getProgressSnapshot();
        if (!before.l1QueueLength && !before.mergeQueueLength) break;
        await cm.tick(); cm.sync(); options.interrupt?.('tick-before-inspection', cm);
      }
      inspection = inspectOpen(root, cm, strategy, inventory, checkpoint, state.framing);
      if (inspection.quarantine || !inspection.sourceLinksValid || (inspection.l1Queue === 0 && inspection.unsealed > 0)) throw new Error('Native archival debt/quarantine remains; not successful convergence');
      cm.sync();
    } catch (error) {
      audit.failures++; audit.lastFailure = { kind: 'failed-resumable-work', messageHash: sha256(error instanceof Error ? error.message : String(error)), at: new Date().toISOString(), classification: classifyTerminalError(error) };
      durableJson(join(root, 'audit.json'), audit);
      throw new Error('Backfill drain failed; candidate remains resumable (see safe audit/verification state)');
    } finally {
      try { if (cm) cm.sync(); } finally { try { cm?.close(); } finally { try { store?.close(); if (state.derivation) privateTree(root); } finally { adapter?.dispose(); } } }
      if (inspection) writeInspection(root, inventory, inspection);
    }
    delete audit.lastFailure;
    durableJson(join(root, 'audit.json'), audit);
    return status(root);
  });
}
export async function status(input: string): Promise<Record<string, unknown>> {
  const root = rootPath(input); const state = load(root);
  const { sources, dispositions } = state;
  const inspection = existsSync(join(root, 'inspection.json')) ? jsonFile<Inspection>(join(root, 'inspection.json')) : undefined;
  const audit = jsonFile<RequestAudit>(join(root, 'audit.json'));
  // Deliberately no native store open: Chronicle open can repair a torn tail,
  // register states or sweep queues. Status reads the last closed-writer receipt.
  const receiptCurrent = !!inspection && inspection.fingerprint === state.inventory.fingerprint &&
    inspection.checkpointHash === bytesHash(join(root, 'checkpoint.json')) &&
    canonical(inspection.nativeFiles) === canonical(filesHash(new SessionManager(join(root, 'data')).getStorePath(state.inventory.sessionId)));
  return { version: 1, readOnly: true, sessionId: state.inventory.sessionId, namespace: NAMESPACE, fingerprint: state.inventory.fingerprint,
    imported: state.checkpoint.nextEvent, pending: state.events.length - state.checkpoint.nextEvent, nativeMessages: inspection?.nativeMessages ?? 0,
    retainedEligibleEvents: state.events.length, excludedLogicalEvents: state.omissions?.excludedLogicalEvents ?? 0, excludedDispositionRecords: state.omissions?.excludedDispositionRecords ?? 0,
    originalParentLineage: state.derivation ? { fingerprint: state.derivation.origin.fingerprint, sessionId: state.derivation.origin.sessionId, eventCount: state.derivation.origin.eventCount, eventsHash: state.derivation.origin.hashes.eventsHash, sourcesHash: state.derivation.origin.hashes.sourcesHash, dispositionsHash: state.derivation.origin.hashes.dispositionsHash } : null,
    ...(state.derivation ? { exclusion: { decisionHash: state.omissions!.decisionHash, omissionsHash: state.inventory.omissionsHash, derivationHash: state.inventory.derivationHash, originals: 'parent-only', summaryReuse: false, semanticSafetyProven: false } } : {}),
    excluded: dispositions.filter(d => d.state === 'exclude').length, candidates: dispositions.filter(d => d.state === 'candidate').length,
    quarantined: dispositions.filter(d => d.state === 'quarantine').length + sources.filter(s => s.error).length + (inspection?.quarantine ?? 0),
    projections: dispositions.filter(d => d.state === 'projection').length, records: sources.reduce((sum, s) => sum + s.recordCount, 0),
    mediaIssues: state.events.flatMap(e => e.media).filter(m => m.status !== 'validated').length,
    leaves: inspection?.leaves ?? 0, merges: inspection?.merges ?? 0, l1Queue: inspection?.l1Queue ?? 0, mergeQueue: inspection?.mergeQueue ?? 0,
    unsealed: inspection?.unsealed ?? 0, mergeQuarantined: inspection?.mergeQuarantine ?? 0, sourceLinksValid: receiptCurrent && inspection!.sourceLinksValid,
    noWork: receiptCurrent && inspection!.sourceLinksValid && !audit.lastFailure && state.checkpoint.nextEvent === state.events.length && (inspection?.l1Queue ?? 0) === 0 && (inspection?.mergeQueue ?? 0) === 0 && (inspection?.unsealed ?? 0) === 0 && (inspection?.quarantine ?? 0) === 0 && !sources.some(s => s.error) && !dispositions.some(d => d.state === 'quarantine'),
    inspection: receiptCurrent ? 'last-closed-writer-native-receipt' : 'stale-requires-writer-recovery', recoveryRequired: !receiptCurrent,
    prompt: { groundHash: state.inventory.groundHash, framingHash: state.inventory.framingHash, chars: state.ground.length, profileHash: state.inventory.profileHash },
    model: MODEL, archivalRoutingPolicy: ARCHIVAL_ROUTING_POLICY, provider: 'openai-codex', profile: { strategy: PROFILE, agent: AGENT_PROFILE }, effectiveProfile: inspection?.effectiveProfile, audit,
    dataDir: join(root, 'data'), recipe: join(root, 'recipe.json') };
}
export async function verify(input: string): Promise<Record<string, unknown>> {
  const root = rootPath(input); const state = load(root); const report = await status(root);
  const sources = state.sources;
  const issues: string[] = [];
  const activePrefixes: Array<{ source: string; state: string }> = [];
  for (const source of sources) {
    if (source.snapshot && bytesHash(join(root, source.snapshot)) !== source.hash) issues.push(`source-snapshot:${source.id}`);
    if (source.partialTailSnapshot && bytesHash(join(root, source.partialTailSnapshot)) !== source.partialTailHash) issues.push(`source-partial-tail:${source.id}`);
    if (source.active && source.snapshot) {
      const state = verifySourcePrefix(source); activePrefixes.push({ source: source.id, state });
      if (state === 'rewritten-or-replaced' || state === 'unreadable') issues.push(`active-source-prefix:${source.id}:${state}`);
    }
  }
  for (const event of state.events) for (const media of event.media) if (media.snapshot && bytesHash(join(root, media.snapshot)) !== media.hash) issues.push(`media-snapshot:${event.source}`);
  if (existsSync(join(root, 'inspection.json'))) {
    const inspection = jsonFile<Inspection>(join(root, 'inspection.json'));
    if (inspection.fingerprint !== state.inventory.fingerprint) issues.push('native-inspection-fingerprint');
    if (inspection.checkpointHash !== bytesHash(join(root, 'checkpoint.json'))) issues.push('checkpoint-needs-native-recovery');
    const actual = filesHash(new SessionManager(join(root, 'data')).getStorePath(state.inventory.sessionId));
    if (canonical(actual) !== canonical(inspection.nativeFiles)) issues.push('native-store-needs-writer-recovery-or-was-modified');
    if (!inspection.sourceLinksValid) issues.push('native-summary-source-links');
  } else if (state.checkpoint.nextEvent || state.derivation) issues.push('missing-native-inspection');
  return { ...report, verified: issues.length === 0, issues, activePrefixes, sourceVerification: state.derivation ? 'immutable-normalized-parent-lineage; raw-originals-parent-only' : 'local-original-snapshots-and-active-prefixes', recoveryRequired: issues.length > 0, readOnly: true };
}
