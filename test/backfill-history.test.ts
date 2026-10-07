import { describe, test, expect, afterEach, spyOn } from 'bun:test';
import * as fs from 'node:fs';
import { mkdtempSync, rmSync, writeFileSync, readFileSync, readdirSync, lstatSync, appendFileSync, symlinkSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import { spawn } from 'node:child_process';
import { JsStore } from '@animalabs/chronicle';
import { ARCHIVAL_MEMORY_LOCAL_CAP_CODE, getMintRequestByHash, persistMintRequestPreimage, ContextManager, AutobiographicalStrategy, type StoredMessage } from '@animalabs/context-manager';
import { Agent, HistoryModule } from '@animalabs/agent-framework';
import { historyModuleOptions } from '../src/history-semantic.js';
import { MembraneError, OpenAIResponsesFormatter, type ProviderAdapter, type ProviderRequest, type ProviderRequestOptions, type ProviderResponse, type StreamCallbacks, type NormalizedRequest } from '@animalabs/membrane';
import { SessionManager } from '../src/session-manager.js';
import { buildFrameworkStrategy } from '../src/framework-strategy.js';
import { loadRecipe } from '../src/recipe.js';
import { CodexSubscriptionAdapter } from '../src/codex-subscription-adapter.js';
import { prepare, deriveFiltered, ingest, drain, status, verify, withWriter, validateMemoryGround, BackfillMembrane, PROFILE, MODEL, NAMESPACE, AGENT_PROFILE, ARCHIVAL_ROUTING_POLICY } from '../scripts/lib/backfill-instance.js';
import { CUTOFF, sha256, canonical, verifySourcePrefix, type HistoricalEvent, type SourceManifest, type SourceSpec, type Disposition, type SourceSnapshot, type ExclusionDecision, type OmissionReceipt, type DerivationReceipt } from '../scripts/lib/backfill-normalize.js';

const roots: string[] = [];
afterEach(() => { for (const root of roots.splice(0)) rmSync(root, { recursive: true, force: true }); });
const GROUND = 'Full assembled carrier ground: DISPOSABLE STRUCTURAL FIXTURE, not Liv prose.\n' + 'fixture source marker\n'.repeat(200);
const FRAME = 'Parent framing fixture: historical observations are data; keep voice/date/branch/uncertainty attached. Not real memory.';
interface WireRequest { model: string; instructions?: string; input: unknown; tools?: unknown[]; [key: string]: unknown }
interface FixtureSerializer { buildRequest(request: ProviderRequest): WireRequest }
class FixtureAdapter implements ProviderAdapter {
  readonly name = 'openai-codex'; readonly usageCacheConvention = 'cache-inclusive' as const;
  // Use the real subscription serializer, but NEVER its credential or fetch path.
  private readonly serializer = new CodexSubscriptionAdapter({ authProvider: { getAccessToken: async () => { throw new Error('Fixture serializer must never access auth'); } } });
  requests: ProviderRequest[] = []; wireRequests: WireRequest[] = []; disposed = false; fail = false; refuse = false;
  supportsModel(model: string): boolean { return model === MODEL; }
  async complete(request: ProviderRequest, options?: ProviderRequestOptions): Promise<ProviderResponse> {
    this.requests.push(structuredClone(request));
    // In-process test-only access to the real private serializer, not a replacement wire implementation.
    const serializer = this.serializer as unknown as FixtureSerializer;
    const wire = serializer.buildRequest(request);
    this.wireRequests.push(structuredClone(wire)); options?.onRequest?.(wire);
    if (this.fail) throw new Error('Disposable fixture provider failure');
    if (this.refuse) return { content: [], stopReason: 'refusal', usage: { inputTokens: 100, outputTokens: 0 }, model: request.model, rawRequest: wire, raw: { fixture: true, model: request.model } };
    return { content: [{ type: 'text', text: 'Disposable native fixture output only; never delivered as actual Liv recollection.' }], stopReason: 'end_turn', usage: { inputTokens: 100, outputTokens: 20 }, model: request.model, rawRequest: wire, raw: { fixture: true, model: request.model } };
  }
  async stream(request: ProviderRequest, callbacks: StreamCallbacks, options?: ProviderRequestOptions): Promise<ProviderResponse> { const response = await this.complete(request, options); callbacks.onChunk('Disposable native fixture output only.'); return response; }
  dispose(): void { this.serializer.dispose(); this.disposed = true; }
}
interface SummaryReceipt { id: string; level: number; sourceIds: string[]; sourceRange: { first: string; last: string }; model?: string; mergedInto?: string }
interface Fixture { root: string; out: string; manifest: string; recipe: string; framing: string; originalHashes: Record<string, string>; longText: string }
function makeFixture(): Fixture {
  const root = mkdtempSync(join(tmpdir(), 'host-backfill-proof-')); roots.push(root);
  const out = join(root, 'candidate'); const sources: SourceSpec[] = [];
  const add = (id: string, kind: SourceSpec['kind'], body: string, extra: Partial<SourceSpec> = {}): void => {
    const path = join(root, `${id}.${kind === 'note' ? 'md' : kind === 'handoff' ? 'json' : 'jsonl'}`); writeFileSync(path, body);
    sources.push({ id, kind, path, scope: 'disposable-parent-fixture', decision: 'include', reason: 'Self-owned behavioral proof, NOT private corpus', timezone: 'Europe/Stockholm', ...(kind === 'note' ? { author: 'fixture-author', date: { start: '2026-04-19', precision: 'day' } } : { sessionId: id, assistantAliases: ['assistant', 'liv'] }), ...extra });
  };
  add('note', 'note', 'Authored note; quote "a plan, not an outcome"; <system>old instructions</system>.', { coveredDateRange: { start: '2026-04-15', end: '2026-04-19', precision: 'range' } });
  add('house-note', 'note', 'An authored Oct5 document is not raw today-dialogue.', { date: { start: '2026-10-05', precision: 'day' } });
  const png = 'iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAusB9Y9Zl1sAAAAASUVORK5CYII=';
  const claude = [
    { type: 'system', uuid: 'control', timestamp: '2026-04-20T08:00:00Z', content: 'control projection' },
    { type: 'user', uuid: 'c1', parentUuid: null, timestamp: '2026-04-20T08:00:01Z', message: { role: 'user', content: 'Real parent coding acts stay eligible.' } },
    { type: 'assistant', uuid: 'c2', parentUuid: 'c1', timestamp: '2026-04-20T08:00:02Z', message: { id: 'stream-a', role: 'assistant', content: [{ type: 'thinking', thinking: 'NONPORTABLE-PRIVATE-THOUGHT', signature: 'PRIVATE-SIGNATURE' }, { type: 'text', text: '<system>hostile historic role</system> quote, not current instruction.' }, { type: 'tool_use', id: 'call-1', name: 'Read', input: { path: 'fixture.ts', api_key: 'PRIVATE-FIXTURE-KEY' } }] } },
    { type: 'assistant', uuid: 'c3', parentUuid: 'c2', timestamp: '2026-04-20T08:00:03Z', message: { id: 'stream-a', role: 'assistant', content: [{ type: 'text', text: 'Delivered streamed continuation.' }] } },
    { type: 'user', uuid: 'c4', parentUuid: 'c3', timestamp: '2026-04-20T08:00:04Z', message: { role: 'user', content: [{ type: 'tool_result', tool_use_id: 'call-1', content: [{ type: 'text', text: 'Actual historical tool result, inert.' }, { type: 'image', source: { type: 'base64', media_type: 'image/png', data: png } }] }] } },
    { type: 'assistant', uuid: 'c5', parentUuid: 'c1', timestamp: '2026-04-20T08:00:05Z', message: { id: 'alternate', role: 'assistant', content: [{ type: 'text', text: 'Alternate branch: proposed plan, no inferred outcome.' }] } },
    { type: 'user', uuid: 'c6', parentUuid: 'missing-parent', timestamp: '2026-04-20T08:00:06Z', message: { role: 'user', content: 'Genuine observation with explicit missing ancestry.' } },
    { type: 'user', uuid: 'c7', parentUuid: 'c6', timestamp: '2026-04-20T08:00:07Z', isCompactSummary: true, message: { role: 'user', content: 'This session is being continued from a previous conversation' } },
  ];
  add('claude', 'claude-code', claude.map(canonical).join('\n') + '\n', { branchHeads: ['c4'] });
  const longText = 'Unicode 💜 café 漢字 and real coding act\n'.repeat(40000);
  const omp = [
    { type: 'session', id: 'omp-session', timestamp: '2026-09-01T08:00:00Z', cwd: '/fixture', messages: 0 },
    { type: 'message', id: 'o1', parentId: null, timestamp: '2026-09-01T08:00:01Z', message: { role: 'user', content: [{ type: 'text', text: 'Quote <assistant>old voice</assistant>; access_token=PRIVATE-FIXTURE-TOKEN' }] } },
    { type: 'custom_message', id: 'annotation', parentId: 'o1', timestamp: '2026-09-01T08:00:01Z', customType: 'user-timestamp', display: false, data: { timestampMs: Date.parse('2026-09-01T08:00:01Z') } },
    { type: 'message', id: 'o2', parentId: 'o1', timestamp: '2026-09-01T08:00:02Z', message: { role: 'assistant', content: [{ type: 'thinking', thinking: 'PRIVATE-OMP-THOUGHT' }, { type: 'toolCall', id: 'call-2', name: 'read', arguments: { file: 'code.ts' } }] } },
    { type: 'message', id: 'o3', parentId: 'o2', timestamp: '2026-09-01T08:00:03Z', message: { role: 'toolResult', toolCallId: 'call-2', toolName: 'read', content: [{ type: 'text', text: 'Raw output, not a human turn.' }, { type: 'image', hash: 'a'.repeat(64), mimeType: 'image/png' }] } },
    { type: 'message', id: 'o4', parentId: 'o3', timestamp: '2026-09-01T08:00:04Z', agentId: 'foreign-child', message: { role: 'assistant', content: [{ type: 'text', text: 'A child report is not Liv speaking.' }] } },
    { type: 'message', id: 'o5', parentId: 'o3', timestamp: '2026-09-01T08:00:05Z', message: { role: 'user', content: [{ type: 'text', text: longText }] } },
    { type: 'message', id: 'o6', parentId: 'o5', timestamp: '2026-10-05T08:00:00Z', message: { role: 'assistant', content: [{ type: 'text', text: 'TODAYS-RAW-SETUP-EXCLUDED' }] } },
    { type: 'custom', id: 'tool-end', parentId: 'o5', timestamp: '2026-09-01T08:00:06Z', customType: 'tool_execution_end' },
  ];
  add('omp', 'omp', omp.map(canonical).join('\n') + '\n{"incomplete":', { active: true });
  add('handoff', 'handoff', JSON.stringify({ version: 1, sourceSession: 'handoff', sourceBranch: 'main', records: ['user', 'assistant', 'user', 'assistant'].map((role, i) => ({ sourceRecordId: String(38 + i), participant: role === 'assistant' ? 'liv' : role, timestamp: Date.parse('2026-10-05T18:00:00Z') + i, content: [{ type: 'text', text: `DISPOSABLE house bridge fixture ${i}, not a real utterance.` }], metadata: { originalParticipant: role } })) }));
  add('candidate', 'note', 'Candidate private document remains explicitly pending, not included.', { decision: 'candidate' });
  const manifest = join(root, 'manifest.json'); writeFileSync(manifest, JSON.stringify({ version: 1, cutoff: CUTOFF, sources } satisfies SourceManifest));
  const recipe = join(root, 'recipe.json'); writeFileSync(recipe, JSON.stringify({ name: 'Fixture carrier', agent: { name: 'liv', provider: 'openai-codex', model: MODEL, systemPrompt: GROUND, timezone: 'Europe/Stockholm', strategy: { type: 'autobiographical' } }, modules: { history: true, webui: { host: '127.0.0.1', port: 0 } }, mcpServers: {} }));
  const framing = join(root, 'parent-framing.md'); writeFileSync(framing, FRAME);
  const originalHashes = Object.fromEntries([...sources.map(s => s.path), manifest, recipe, framing].map(path => [path, sha256(readFileSync(path))]));
  return { root, out, manifest, recipe, framing, originalHashes, longText };
}
function systemText(request: ProviderRequest): string {
  if (typeof request.system === 'string') return request.system;
  return (request.system ?? []).map(block => block && typeof block === 'object' && 'text' in block && typeof block.text === 'string' ? block.text : '').join('');
}
const readEvents = (out: string): HistoricalEvent[] => readFileSync(join(out, 'events.jsonl'), 'utf8').trim().split('\n').filter(Boolean).map(line => JSON.parse(line));
function treeHashes(root: string): Record<string, string> {
  const hashes: Record<string, string> = {};
  const walk = (dir: string): void => { for (const name of readdirSync(dir)) { const path = join(dir, name); if (lstatSync(path).isDirectory()) walk(path); else hashes[path.slice(root.length)] = sha256(readFileSync(path)); } };
  walk(root); return hashes;
}

function assertNativeMembership(cm: ContextManager): void {
  const summaries = (cm.getStore().getStateJson(`${NAMESPACE}/autobio:summaries`) ?? []) as SummaryReceipt[];
  const chunks = (cm.getStore().getStateJson(`${NAMESPACE}/autobio:chunks`) ?? []) as Array<{ id: string; sourceIds: string[]; summaryId?: string; compressed: boolean }>;
  const byId = new Map(summaries.map(summary => [summary.id, summary]));
  expect(byId.size).toBe(summaries.length); expect(new Set(chunks.map(chunk => chunk.id)).size).toBe(chunks.length);
  const chunkSources = chunks.flatMap(chunk => chunk.sourceIds);
  expect(new Set(chunkSources).size).toBe(chunkSources.length);
  expect([...chunkSources].sort()).toEqual(cm.getAllMessages().map(message => message.id).sort());
  for (const summary of summaries) {
    expect(new Set(summary.sourceIds).size).toBe(summary.sourceIds.length);
    if (summary.level > 1) {
      expect([...summary.sourceIds].sort()).toEqual(summaries.filter(child => child.mergedInto === summary.id).map(child => child.id).sort());
      expect(summary.sourceIds.every(id => byId.get(id)?.level === summary.level - 1)).toBe(true);
    } else {
      const chunk = chunks.find(record => record.summaryId === summary.id);
      expect(chunk?.compressed).toBe(true); expect(summary.sourceIds).toEqual(chunk!.sourceIds);
    }
  }
  const leaves = (summary: SummaryReceipt): string[] => summary.level === 1 ? summary.sourceIds : summary.sourceIds.flatMap(id => leaves(byId.get(id)!));
  for (const summary of summaries) {
    const raw = leaves(summary); expect(summary.sourceRange).toEqual({ first: raw[0], last: raw.at(-1)! });
  }
  const roots = summaries.filter(summary => !summary.mergedInto).flatMap(leaves);
  expect(roots.length).toBe(new Set(roots).size); expect(roots.sort()).toEqual([...chunkSources].sort());
}

class CyberFixtureAdapter extends FixtureAdapter {
  cyber = true;
  fallbackFailure?: 'refusal' | 'auth' | 'missing-model' | 'wrong-model';
  override supportsModel(model: string): boolean { return model === MODEL || model === ARCHIVAL_ROUTING_POLICY.fallbackModel; }
  override async complete(request: ProviderRequest, options?: ProviderRequestOptions): Promise<ProviderResponse> {
    const response = await super.complete(request, options);
    if (this.cyber && request.model === MODEL) throw new MembraneError({ type: 'safety', retryable: false, providerErrorCode: 'cyber_policy', message: 'DISPOSABLE-PRIVATE-CYBER provider body.', rawError: { private: true }, rawRequest: { private: true } });
    if (request.model === ARCHIVAL_ROUTING_POLICY.fallbackModel) {
      if (this.fallbackFailure === 'auth') throw new MembraneError({ type: 'auth', retryable: false, message: 'DISPOSABLE-PRIVATE-AUTH', rawError: undefined });
      if (this.fallbackFailure === 'refusal') return { ...response, stopReason: 'refusal', content: [] };
      if (this.fallbackFailure === 'missing-model') return { ...response, raw: {} };
      if (this.fallbackFailure === 'wrong-model') return { ...response, model: 'unapproved-backend', raw: { model: 'unapproved-backend' } };
    }
    return response;
  }
}

test('cyber-routing-host mixed authors: original profile/fingerprint, native Blue proof, normal Sol parent and current-only audit', async () => {
  const f = makeFixture(); const manifest: SourceManifest = JSON.parse(readFileSync(f.manifest, 'utf8')); manifest.sources = manifest.sources.filter(source => source.kind === 'handoff'); writeFileSync(f.manifest, JSON.stringify(manifest)); await prepare(f);
  const immutableNames = ['instance.json', 'events.jsonl', 'ground.md', 'framing.md', 'recipe.json'];
  const immutable = Object.fromEntries(immutableNames.map(name => [name, sha256(readFileSync(join(f.out, name)))])); const originalProfile = canonical({ strategy: PROFILE, agent: AGENT_PROFILE });
  const accepted: Array<{ id: string; content: string; provenance: { model?: string; requestHash: string }; mergedInto?: string }> = []; const preimages: unknown[] = [];
  for (let index = 0; index < 4; index++) {
    await ingest(f.out, { apply: true, maxEvents: 1 }); const adapter = new CyberFixtureAdapter(); adapter.cyber = index === 0;
    const report = await drain(f.out, { apply: true, maxSteps: 1, adapterFactory: () => adapter }); expect(adapter.disposed).toBe(true); expect(adapter.requests.map(request => request.model)).toEqual(index === 0 ? [MODEL, ARCHIVAL_ROUTING_POLICY.fallbackModel] : [MODEL]);
    expect(report.model).toBe(MODEL); expect(report.archivalRoutingPolicy).toEqual(ARCHIVAL_ROUTING_POLICY); expect(report.effectiveProfile).toEqual(PROFILE);
    const audit = report.audit as { lastDrain: unknown; lastRequest: { route: string }; lastResponse: { model: string; primaryModel: string; requestedModel: string; route: string } };
    expect(audit.lastDrain).toEqual({ primaryRequests: 1, fallbackRequests: index === 0 ? 1 : 0, primaryResponses: index === 0 ? 0 : 1, fallbackResponses: index === 0 ? 1 : 0 });
    expect(audit.lastResponse.primaryModel).toBe(MODEL); expect(audit.lastResponse.model).toBe(index === 0 ? ARCHIVAL_ROUTING_POLICY.fallbackModel : MODEL); expect(audit.lastRequest.route).toBe(index === 0 ? 'cyber-policy-fallback' : 'primary');
  }
  const inventory = JSON.parse(readFileSync(join(f.out, 'instance.json'), 'utf8')) as { sessionId: string }; const nativePath = new SessionManager(join(f.out, 'data')).getStorePath(inventory.sessionId);
  let cm = await ContextManager.open({ path: nativePath, namespace: NAMESPACE, strategy: new AutobiographicalStrategy({ ...PROFILE, auditOnly: true, autoTickOnNewMessage: false, logEffectiveConfig: false }) });
  try { for (const overview of cm.getSummariesInRange({ level: 1 })) {
    const summary = cm.getSummary(overview.id)!;
    accepted.push(structuredClone(summary) as typeof accepted[number]); preimages.push(getMintRequestByHash(cm.getStore(), summary.provenance!.requestHash));
  } } finally { cm.close(); }
  const parentAdapter = new CyberFixtureAdapter(); parentAdapter.cyber = false; const final = await drain(f.out, { apply: true, maxSteps: 1, adapterFactory: () => parentAdapter }); expect(final.noWork).toBe(true); expect(final.sourceLinksValid).toBe(true); expect(parentAdapter.requests.map(request => request.model)).toEqual([MODEL]); expect((await verify(f.out)).verified).toBe(true);
  cm = await ContextManager.open({ path: nativePath, namespace: NAMESPACE, strategy: new AutobiographicalStrategy({ ...PROFILE, auditOnly: true, autoTickOnNewMessage: false, logEffectiveConfig: false }) });
  let originals: unknown;
  try {
    const parent = cm.getSummary(cm.getSummariesInRange({ level: 2 })[0].id)!; expect(parent.provenance!.model).toBe(MODEL); expect(parent.provenance!.archivalCyberPolicyFallback).toBeUndefined();
    for (const [index, prior] of accepted.entries()) { const current = cm.getSummary(prior.id)!; const { mergedInto, ...authored } = current; expect(authored).toEqual(prior); expect(mergedInto).toBe(parent.id); expect(getMintRequestByHash(cm.getStore(), prior.provenance.requestHash)).toEqual(preimages[index]); }
    originals = structuredClone(cm.getStore().getStateJson(`${NAMESPACE}/autobio:summaries`));
  } finally { cm.close(); }
  expect(Object.fromEntries(immutableNames.map(name => [name, sha256(readFileSync(join(f.out, name)))]))).toEqual(immutable); expect(canonical({ strategy: PROFILE, agent: AGENT_PROFILE })).toBe(originalProfile);
  // A direct test reopen is a writer lifecycle, not status/verify; publish its
  // closed native receipt through the existing explicit recovery operation.
  expect((await ingest(f.out, { apply: true })).sourceLinksValid).toBe(true);
  expect((await verify(f.out)).verified).toBe(true);
  // Synthetic legacy absent-preimage label: do not manufacture the missing request.
  cm = await ContextManager.open({ path: nativePath, namespace: NAMESPACE, strategy: new AutobiographicalStrategy({ ...PROFILE, auditOnly: true, autoTickOnNewMessage: false, logEffectiveConfig: false }) });
  try { const legacy = structuredClone(originals) as Array<{ provenance: { model: string; requestHash: string } }>;
    legacy.find(entry => entry.provenance.model === MODEL)!.provenance.requestHash = 'f'.repeat(64); cm.getStore().setStateJson(`${NAMESPACE}/autobio:summaries`, legacy); cm.sync();
    expect(getMintRequestByHash(cm.getStore(), 'f'.repeat(64))).toBeNull();
  } finally { cm.close(); }
  expect((await ingest(f.out, { apply: true })).sourceLinksValid).toBe(true);
  // Blue is not admitted by label alone: request identity AND routing evidence are required.
  for (const missing of ['preimage', 'route', 'author', 'overrides'] as const) {
    cm = await ContextManager.open({ path: nativePath, namespace: NAMESPACE, strategy: new AutobiographicalStrategy({ ...PROFILE, auditOnly: true, autoTickOnNewMessage: false, logEffectiveConfig: false }) });
    try { const damaged = structuredClone(originals) as Array<{ provenance: { model: string; requestHash: string; archivalCyberPolicyFallback?: unknown } }>;
      const blue = damaged.find(entry => entry.provenance.model === ARCHIVAL_ROUTING_POLICY.fallbackModel)!;
      if (missing === 'preimage') blue.provenance.requestHash = 'e'.repeat(64);
      else if (missing === 'route') delete blue.provenance.archivalCyberPolicyFallback;
      else if (missing === 'author') blue.provenance.model = 'unapproved-backend';
      else { const request = getMintRequestByHash(cm.getStore(), blue.provenance.requestHash)!;
        request.providerParams = { model: 'unapproved-backend' }; const hash = sha256(JSON.stringify(request));
        persistMintRequestPreimage(cm.getStore(), request, hash); blue.provenance.requestHash = hash;
      }
      cm.getStore().setStateJson(`${NAMESPACE}/autobio:summaries`, damaged); cm.sync();
    } finally { cm.close(); }
    const inspected = await ingest(f.out, { apply: true }); expect(inspected.sourceLinksValid).toBe(false); expect(inspected.noWork).toBe(false); expect((await verify(f.out)).verified).toBe(false);
  }
});

for (const failure of ['refusal', 'auth', 'missing-model', 'wrong-model'] as const) for (const operation of ['l1', 'merge'] as const) test(`cyber-routing-host terminal ${operation}/${failure}: safe local halt audit, no automatic quarantine/retry`, async () => {
  const f = makeFixture(); const manifest: SourceManifest = JSON.parse(readFileSync(f.manifest, 'utf8')); manifest.sources = manifest.sources.filter(source => source.kind === 'handoff'); writeFileSync(f.manifest, JSON.stringify(manifest)); await prepare(f);
  if (operation === 'merge') for (let index = 0; index < 4; index++) { await ingest(f.out, { apply: true, maxEvents: 1 }); await drain(f.out, { apply: true, maxSteps: 1, adapterFactory: () => new FixtureAdapter() }); }
  else await ingest(f.out, { apply: true, maxEvents: 1 });
  const prior = JSON.parse(readFileSync(join(f.out, 'inspection.json'), 'utf8')); const adapter = new CyberFixtureAdapter(); adapter.fallbackFailure = failure;
  await expect(drain(f.out, { apply: true, maxSteps: 99, adapterFactory: () => adapter })).rejects.toThrow('resumable'); expect(adapter.disposed).toBe(true); expect(adapter.requests.map(request => request.model)).toEqual([MODEL, ARCHIVAL_ROUTING_POLICY.fallbackModel]);
  const audit = JSON.parse(readFileSync(join(f.out, 'audit.json'), 'utf8')); expect(audit.lastFailure.classification).toMatchObject({ kind: 'local-fallback-halt', localCode: 'archival_cyber_policy_fallback_halt', retryable: false, primaryModel: MODEL, fallbackModel: ARCHIVAL_ROUTING_POLICY.fallbackModel });
  expect(audit.lastFailure.classification.requestHash).toMatch(/^[a-f0-9]{64}$/); expect(audit.lastFailure.classification.evidenceHash).toMatch(/^[a-f0-9]{64}$/);
  if (failure === 'auth') { expect(audit.lastFailure.classification.type).toBe('auth'); expect(audit.lastFailure.classification.providerErrorCode).toBeUndefined(); }
  expect(audit.lastDrain.primaryRequests).toBe(1); expect(audit.lastDrain.fallbackRequests).toBe(1); expect(JSON.stringify(audit)).not.toContain('DISPOSABLE-PRIVATE'); expect((await status(f.out)).noWork).toBe(false);
  const inventory = JSON.parse(readFileSync(join(f.out, 'instance.json'), 'utf8')) as { sessionId: string }; const cm = await ContextManager.open({ path: new SessionManager(join(f.out, 'data')).getStorePath(inventory.sessionId), namespace: NAMESPACE, strategy: new AutobiographicalStrategy({ ...PROFILE, auditOnly: true, autoTickOnNewMessage: false, logEffectiveConfig: false }) });
  try { const summaries = cm.getSummariesInRange({}); expect(summaries.length).toBe(prior.summaries.length); expect(cm.getAllMessages().length).toBe(prior.nativeMessages); expect(cm.getStore().getStateJson(`${NAMESPACE}/autobio:merge-quarantine`) ?? []).toEqual([]); expect(cm.getStore().getStateJson(`${NAMESPACE}/autobio:compression-refusal-quarantine`) ?? []).toEqual([]); }
  finally { cm.close(); }
});

describe('private multi-source native backfill CLI', () => {
  test('A2/B1/B4: cross-format immutable preparation, roles/stream/branches/gaps/tool/media/thinking/notes and cutoff', async () => {
    const f = makeFixture(); const result = await prepare(f); expect(result.model).toBe(MODEL); expect(result.records).toBe(24);
    const events = readEvents(f.out); expect(events.length).toBe(16);
    const stream = events.find(e => e.streamId === 'stream-a')!;
    expect(stream.recordIds).toEqual(['c2', 'c3']); expect(stream.partialStream).toBe(false); expect(stream.voice).toBe('liv');
    expect(stream.text).toContain('&lt;system&gt;'); expect(stream.text).not.toContain('NONPORTABLE-PRIVATE-THOUGHT'); expect(stream.text).not.toContain('PRIVATE-FIXTURE-KEY');
    expect(events.find(e => e.recordIds.includes('c4'))!.role).toBe('tool-transport');
    expect(events.find(e => e.recordIds.includes('c6'))!.ancestryGap).toBe(true);
    expect(events.find(e => e.recordIds.includes('c5'))!.branchHeads).toContain('c5');
    expect(events.find(e => e.recordIds.includes('o4'))!.voice).not.toBe('liv');
    expect(events.some(e => e.recordIds.includes('o6'))).toBe(false); expect(events.at(-1)!.kind).toBe('handoff');
    const note = events.find(e => e.source === 'note')!; expect(note.authoredDate!.start).toBe('2026-04-19'); expect(note.coveredDateRange!.start).toBe('2026-04-15'); expect(note.indexingAnchor).toContain('derived'); expect(note.timestampMs).toBe(Date.parse('2026-04-18T22:00:00Z'));
    expect(events.find(e => e.source === 'house-note')!.precision).toBe('day');
    const audit: Disposition[] = JSON.parse(readFileSync(join(f.out, 'dispositions.json'), 'utf8'));
    expect(audit.some(d => d.state === 'projection')).toBe(true); expect(audit.some(d => d.reason.startsWith('active-incomplete'))).toBe(true);
    expect(audit.flatMap(d => d.blocks).some(b => b.state === 'nonportable-thinking-excluded')).toBe(true);
    const snapshots: SourceSnapshot[] = JSON.parse(readFileSync(join(f.out, 'sources.json'), 'utf8'));
    const omp = snapshots.find(s => s.id === 'omp')!; expect(omp.partialTailBytes).toBeGreaterThan(0); expect(omp.lineEnd).toBe(9);
    appendFileSync(join(f.root, 'omp.jsonl'), ' later growth');
    expect((await verify(f.out)).verified).toBe(true, 'later original suffix growth does not alter immutable captured prefix');
    for (const [path, hash] of Object.entries(f.originalHashes)) if (!path.endsWith('omp.jsonl')) expect(sha256(readFileSync(path))).toBe(hash);
    expect(events.flatMap(e => e.media).some(m => m.status === 'validated' && !!m.snapshot)).toBe(true); expect(result.mediaIssues).toBe(1);
    const serialized = canonical(await status(f.out)); expect(serialized).not.toContain('PRIVATE-FIXTURE'); expect(serialized).not.toContain('PRIVATE-OMP-THOUGHT'); expect(serialized).not.toContain('iVBOR');
    const changed = readFileSync(join(f.root, 'omp.jsonl')); changed[0] = 32; writeFileSync(join(f.root, 'omp.jsonl'), changed);
    const rewritten = await verify(f.out); expect(rewritten.verified).toBe(false); expect(rewritten.issues).toContain('active-source-prefix:omp:rewritten-or-replaced');
  });
  for (const kind of ['omp', 'handoff'] as const) for (const conflicting of [false, true]) test(`B1/B3/B5/I3: ${kind} ${conflicting ? 'conflicting' : 'identical'} stable duplicate IDs are explicit, never streams`, async () => {
    const f = makeFixture(); const manifest: SourceManifest = JSON.parse(readFileSync(f.manifest, 'utf8')); const source = manifest.sources.find(item => item.kind === kind)!;
    manifest.sources = [{ ...source, active: false }]; writeFileSync(f.manifest, JSON.stringify(manifest));
    const record = (id: string, parentId: string | null, text: string) => kind === 'omp'
      ? { type: 'message', id, parentId, timestamp: '2026-09-01T08:00:01Z', message: { role: 'user', content: [{ type: 'text', text }] } }
      : { sourceRecordId: id, parentId, timestamp: Date.parse('2026-10-05T18:00:00Z'), participant: 'user', content: [{ type: 'text', text }] };
    const records = [record('stable', null, 'First delivered act.'), record('stable', null, conflicting ? 'Conflicting delivered act.' : 'First delivered act.'), record('child', 'stable', 'Real descendant remains attributable.'), record('grandchild', 'child', 'Later descendant preserves the ancestry gap.')];
    const raw = kind === 'omp' ? records.map((item, index) => index === 1 ? JSON.stringify(item, null, 0) : canonical(item)).join('\n') + '\n' : JSON.stringify({ version: 1, sourceSession: source.sessionId, records });
    writeFileSync(source.path, raw); await prepare(f);
    const events = readEvents(f.out); const audit: Disposition[] = JSON.parse(readFileSync(join(f.out, 'dispositions.json'), 'utf8')); const duplicate = audit.filter(item => item.recordId === 'stable');
    expect(duplicate.length).toBe(2); expect(duplicate.every(item => item.blocks.length === 1 && item.hash.length === 64)).toBe(true);
    expect(audit.some(item => item.state === 'coalesced-stream-blocks')).toBe(false); expect(events.length).toBe(conflicting ? 2 : 3); expect(new Set(events.map(event => event.key)).size).toBe(events.length);
    if (conflicting) {
      expect(duplicate.every(item => item.state === 'quarantine' && item.reason === 'conflicting-duplicate-source-id')).toBe(true);
      expect(events.some(event => event.recordIds.includes('stable'))).toBe(false);
      expect(events.every(event => event.ancestryGap && event.gapReason === 'quarantined-ancestor:conflicting-duplicate-source-id')).toBe(true);
      expect(events.find(event => event.recordIds.includes('child'))!.parentInCorpus).toBe(false); expect((await status(f.out)).noWork).toBe(false);
    } else {
      expect(duplicate.map(item => item.state)).toEqual(['include', 'duplicate-identical-record']);
      const event = events.find(item => item.recordIds.includes('stable'))!; expect(event.recordIds).toEqual(['stable']); expect(event.sourceLines).toEqual([1]);
      expect(event.text.split('First delivered act.').length - 1).toBe(1); expect(events.every(item => !item.ancestryGap)).toBe(true);
      await ingest(f.out, { apply: true }); const checkpoint = JSON.parse(readFileSync(join(f.out, 'checkpoint.json'), 'utf8')); expect(Object.keys(checkpoint.mapping).length).toBe(3);
    }
    const snapshots: SourceSnapshot[] = JSON.parse(readFileSync(join(f.out, 'sources.json'), 'utf8')); expect(snapshots[0].recordCount).toBe(4); expect(snapshots[0].hash).toBe(sha256(raw)); expect(readFileSync(join(f.out, snapshots[0].snapshot!), 'utf8')).toBe(raw);
  });
  for (const kind of ['omp', 'handoff', 'claude-code'] as const) test(`B3/I3: ${kind} malformed recognized parents quarantine acts and retain descendant gaps`, async () => {
    const f = makeFixture(); const manifest: SourceManifest = JSON.parse(readFileSync(f.manifest, 'utf8')); const source = manifest.sources.find(item => item.kind === kind)!;
    manifest.sources = [{ ...source, active: false }]; writeFileSync(f.manifest, JSON.stringify(manifest));
    const record = (id: string, parent: unknown, text: string) => kind === 'handoff'
      ? { sourceRecordId: id, parentId: parent, timestamp: Date.parse('2026-10-05T18:00:00Z'), participant: 'user', content: [{ type: 'text', text }] }
      : { type: kind === 'omp' ? 'message' : 'user', ...(kind === 'omp' ? { id, parentId: parent } : { uuid: id, parentUuid: parent }), timestamp: '2026-09-01T08:00:01Z', message: { role: 'user', content: [{ type: 'text', text }] } };
    const records = [record('root', null, 'Explicit legitimate root.'), ...[42, {}, [], false, ''].map((parent, index) => record(`bad-${index}`, parent, 'Malformed ancestry, not a new root.')), record('child', 'bad-0', 'Delivered child with explicit ancestry gap.'), record('grandchild', 'child', 'Further ancestry is still uncertain.')];
    const raw = kind === 'handoff' ? JSON.stringify({ version: 1, sourceSession: source.sessionId, records }) : records.map(canonical).join('\n') + '\n'; writeFileSync(source.path, raw); await prepare(f);
    const audit: Disposition[] = JSON.parse(readFileSync(join(f.out, 'dispositions.json'), 'utf8')); const invalid = audit.filter(item => item.recordId.startsWith('bad-'));
    expect(invalid.length).toBe(5); expect(invalid.every(item => item.state === 'quarantine' && item.reason === 'malformed-source-parent-id' && item.blocks.length === 1)).toBe(true);
    const events = readEvents(f.out); expect(events.length).toBe(3); expect(events.some(item => item.recordIds.some(id => id.startsWith('bad-')))).toBe(false);
    expect(events.find(item => item.recordIds.includes('root'))!.parentId).toBe(null);
    for (const event of events.filter(item => !item.recordIds.includes('root'))) { expect(event.ancestryGap).toBe(true); expect(event.gapReason).toBe('quarantined-ancestor:malformed-source-parent-id'); }
    expect(events.find(item => item.recordIds.includes('child'))!.parentInCorpus).toBe(false); expect((await status(f.out)).noWork).toBe(false);
    const snapshots: SourceSnapshot[] = JSON.parse(readFileSync(join(f.out, 'sources.json'), 'utf8')); expect(snapshots[0].recordCount).toBe(8); expect(readFileSync(join(f.out, snapshots[0].snapshot!), 'utf8')).toBe(raw);
  });
  test('B1/B3: empty OMP aborted/error attempts are recoverable controls, not delivered assistant turns', async () => {
    const f = makeFixture(); const manifest: SourceManifest = JSON.parse(readFileSync(f.manifest, 'utf8')); const source = manifest.sources.find(item => item.kind === 'omp')!;
    manifest.sources = [{ ...source, active: false }]; writeFileSync(f.manifest, JSON.stringify(manifest));
    const timestamp = '2026-09-01T08:00:01.000Z'; const epoch = Date.parse(timestamp);
    const attempt = (id: string, parentId: string, stopReason: 'aborted' | 'error', content: unknown[] = []) => ({
      type: 'message', id, parentId, timestamp, message: { role: 'assistant', content, api: 'fixture-api', provider: 'fixture-provider', model: 'fixture-model', usage: { input: 0, output: 0, totalTokens: 0 }, stopReason, errorMessage: 'DISPOSABLE no-output failure fixture, not real memory.', errorId: `fixture-${id}`, timestamp: epoch },
    });
    const records = [
      { type: 'message', id: 'root', parentId: null, timestamp, message: { role: 'user', content: [{ type: 'text', text: 'Disposable request fixture.' }] } },
      attempt('aborted-1', 'root', 'aborted'), attempt('aborted-2', 'aborted-1', 'aborted'), attempt('aborted-3', 'aborted-2', 'aborted'),
      attempt('error-empty', 'aborted-3', 'error'),
      attempt('aborted-partial', 'error-empty', 'aborted', [{ type: 'thinking', thinking: 'DISPOSABLE-NONPORTABLE-THOUGHT' }, { type: 'text', text: 'Disposable delivered aborted partial.' }]),
      attempt('error-partial', 'aborted-partial', 'error', [{ type: 'text', text: 'Disposable delivered error partial.' }]),
    ];
    const lines = records.map(canonical); const raw = lines.join('\n') + '\n'; writeFileSync(source.path, raw);
    const result = await prepare(f); expect(result.records).toBe(7); expect(result.quarantined).toBe(0);
    const events = readEvents(f.out); expect(events.map(event => event.recordIds)).toEqual([['root'], ['aborted-partial'], ['error-partial']]);
    const audit: Disposition[] = JSON.parse(readFileSync(join(f.out, 'dispositions.json'), 'utf8'));
    for (const [index, record] of records.entries()) {
      expect(audit[index]).toMatchObject({ source: source.id, recordId: record.id, line: index + 1, hash: sha256(lines[index]) });
      if (index > 0 && index < 5) expect(audit[index]).toMatchObject({ state: 'projection', reason: `empty-assistant-${index === 4 ? 'error' : 'aborted'}-control-projection`, blocks: [] });
    }
    const partial = events.find(event => event.recordIds.includes('aborted-partial'))!;
    expect(partial).toMatchObject({ source: source.id, sourceLines: [6], role: 'assistant', voice: 'liv', parentId: 'error-empty', originalTimestamp: timestamp, timestampMs: epoch, ancestryGap: false, parentInCorpus: false, gapReason: 'parent-is-outside-selection-or-a-control-projection' });
    expect(partial.branchHeads).toEqual(['error-partial']); expect(partial.text).toContain('Disposable delivered aborted partial.');
    expect(events.find(event => event.recordIds.includes('error-partial'))!).toMatchObject({ parentId: 'aborted-partial', parentInCorpus: true, voice: 'liv' });
    expect(audit.slice(5).every(item => item.state === 'include')).toBe(true);
    expect(canonical(events)).not.toContain('DISPOSABLE-NONPORTABLE-THOUGHT'); expect(canonical(events)).not.toContain('DISPOSABLE no-output failure');
    const snapshots: SourceSnapshot[] = JSON.parse(readFileSync(join(f.out, 'sources.json'), 'utf8')); const snapshot = snapshots[0];
    expect(snapshot).toMatchObject({ recordCount: 7, eventCount: 3, projectionCount: 4, hash: sha256(raw) });
    const recoveredRaw = readFileSync(join(f.out, snapshot.snapshot!), 'utf8'); expect(recoveredRaw).toBe(raw); expect(readFileSync(source.path, 'utf8')).toBe(raw);
    const recovered = recoveredRaw.trim().split('\n').map(line => JSON.parse(line));
    for (let index = 1; index < 5; index++) { expect(recovered[index]).toEqual(records[index]); expect(sha256(recoveredRaw.split('\n')[audit[index].line - 1])).toBe(audit[index].hash); }
  });
  test('B1/B3: explicit empty OMP stop completions are distinct recoverable controls, not cancellations', async () => {
    const f = makeFixture(); const manifest: SourceManifest = JSON.parse(readFileSync(f.manifest, 'utf8')); const source = manifest.sources.find(item => item.kind === 'omp')!;
    manifest.sources = [{ ...source, active: false }]; writeFileSync(f.manifest, JSON.stringify(manifest));
    const timestamp = '2026-09-01T08:00:01.000Z'; const epoch = Date.parse(timestamp);
    const completion = (id: string, api: string, content: unknown[] = [], parentId = 'root') => ({
      type: 'message', id, parentId, timestamp, message: { role: 'assistant', content, api, provider: 'fixture-provider', model: 'fixture-model', usage: { input: 0, output: 0, totalTokens: 0 }, stopReason: 'stop', timestamp: epoch },
    });
    const empty = [...Array.from({ length: 10 }, (_, index) => completion(`stop-${index}`, 'anthropic-messages')), completion('stop-other-api', 'fixture-other-api')];
    const records = [
      { type: 'message', id: 'root', parentId: null, timestamp, message: { role: 'user', content: [{ type: 'text', text: 'Disposable coding request fixture, not real memory.' }] } },
      ...empty, completion('delivered-stop', 'anthropic-messages', [{ type: 'text', text: 'Disposable delivered successful completion.' }], 'stop-9'),
    ];
    const lines = records.map(canonical); const raw = lines.join('\n') + '\n'; writeFileSync(source.path, raw);
    const result = await prepare(f); expect(result.records).toBe(13); expect(result.quarantined).toBe(0);
    const events = readEvents(f.out); expect(events.map(event => event.recordIds)).toEqual([['root'], ['delivered-stop']]);
    const audit: Disposition[] = JSON.parse(readFileSync(join(f.out, 'dispositions.json'), 'utf8'));
    for (const [index, record] of empty.entries()) expect(audit[index + 1]).toMatchObject({ source: source.id, recordId: record.id, line: index + 2, hash: sha256(lines[index + 1]), state: 'projection', reason: 'empty-assistant-stop-control-projection', blocks: [] });
    expect(audit.at(-1)!.state).toBe('include');
    expect(events[1]).toMatchObject({ source: source.id, sourceLines: [13], role: 'assistant', voice: 'liv', parentId: 'stop-9', originalTimestamp: timestamp, timestampMs: epoch, ancestryGap: false, parentInCorpus: false, gapReason: 'parent-is-outside-selection-or-a-control-projection' });
    expect(events[1].text).toContain('Disposable delivered successful completion.'); expect(events[1].branchHeads).toEqual(['delivered-stop']);
    const snapshots: SourceSnapshot[] = JSON.parse(readFileSync(join(f.out, 'sources.json'), 'utf8')); const snapshot = snapshots[0];
    expect(snapshot).toMatchObject({ recordCount: 13, eventCount: 2, projectionCount: 11, hash: sha256(raw) });
    const recoveredRaw = readFileSync(join(f.out, snapshot.snapshot!), 'utf8'); expect(recoveredRaw).toBe(raw); expect(readFileSync(source.path, 'utf8')).toBe(raw);
    const recoveredLines = recoveredRaw.trim().split('\n');
    for (const [index, record] of empty.entries()) { expect(JSON.parse(recoveredLines[index + 1])).toEqual(record); expect(sha256(recoveredLines[audit[index + 1].line - 1])).toBe(audit[index + 1].hash); }
  });
  test('B1/B3: missing/malformed/unknown-outcome empty OMP content never becomes outcome controls', async () => {
    const f = makeFixture(); const manifest: SourceManifest = JSON.parse(readFileSync(f.manifest, 'utf8')); const source = manifest.sources.find(item => item.kind === 'omp')!;
    manifest.sources = [{ ...source, active: false }]; writeFileSync(f.manifest, JSON.stringify(manifest));
    const timestamp = '2026-09-01T08:00:01.000Z';
    const messages = [
      ...['aborted', 'error', 'stop'].flatMap(stopReason => [
        { role: 'assistant', stopReason }, ...[null, {}, 42, false].map(content => ({ role: 'assistant', content, stopReason })),
      ]),
      ...['end_turn', 'toolUse', 'unsupported', undefined].map(stopReason => ({ role: 'assistant', content: [], stopReason, errorMessage: 'An error string alone is not an explicit supported outcome.' })),
      { role: 'user', content: [], stopReason: 'aborted' }, { role: 'toolResult', content: [], stopReason: 'error' },
      { role: 'user', content: [], stopReason: 'stop' }, { role: 'toolResult', content: [], stopReason: 'stop' },
    ];
    const records = messages.map((message, index) => ({ type: 'message', id: `invalid-${index}`, parentId: null, timestamp, message }));
    const guarded = [
      { type: 'message', id: 'bad-parent', parentId: 42, timestamp, message: { role: 'assistant', content: [], stopReason: 'stop' } },
      { type: 'message', id: 'bad-time', parentId: null, timestamp: 'not-a-clock', message: { role: 'assistant', content: [], stopReason: 'stop' } },
      { type: 'message', id: 'bad-session', parentId: null, timestamp, sessionId: 'other-fixture-session', message: { role: 'assistant', content: [], stopReason: 'stop' } },
      { type: 'message', id: 'unsupported-role', parentId: null, timestamp, message: { role: 'unsupported', content: [], stopReason: 'aborted' } },
    ];
    const raw = [...records, ...guarded].map(canonical).join('\n') + '\n'; writeFileSync(source.path, raw); await prepare(f);
    const audit: Disposition[] = JSON.parse(readFileSync(join(f.out, 'dispositions.json'), 'utf8'));
    expect(audit.slice(0, records.length).every(item => item.state === 'quarantine' && item.reason === 'missing-or-unsupported-message-content')).toBe(true);
    expect(audit.slice(records.length).map(item => item.reason)).toEqual(['malformed-source-parent-id', 'missing-or-invalid-source-time', 'manifest-session-mismatch', 'control-or-copied-history-projection']);
    expect(audit.slice(records.length, -1).every(item => item.state === 'quarantine')).toBe(true);
    expect(audit.at(-1)!.state).toBe('projection'); expect(audit.some(item => item.reason.startsWith('empty-assistant-'))).toBe(false); expect(readEvents(f.out)).toEqual([]);
    const snapshots: SourceSnapshot[] = JSON.parse(readFileSync(join(f.out, 'sources.json'), 'utf8')); expect(snapshots[0].recordCount).toBe(records.length + guarded.length); expect(readFileSync(join(f.out, snapshots[0].snapshot!), 'utf8')).toBe(raw);
  });
  test('B7/I3/A5: raw framing bytes and handoff branch survive preparation and actual request serialization', async () => {
    const f = makeFixture(); const frame = FRAME + '\n'; writeFileSync(f.framing, frame);
    const manifest: SourceManifest = JSON.parse(readFileSync(f.manifest, 'utf8')); manifest.sources = manifest.sources.filter(source => source.kind === 'handoff'); writeFileSync(f.manifest, JSON.stringify(manifest));
    await prepare(f);
    expect(readFileSync(join(f.out, 'framing.md'), 'utf8')).toBe(frame);
    const recipe = JSON.parse(readFileSync(join(f.out, 'recipe.json'), 'utf8')); expect(recipe.agent.systemPrompt).toBe(GROUND); expect(recipe.agent.strategy.identityReminder).toBe(frame);
    const inventory = JSON.parse(readFileSync(join(f.out, 'instance.json'), 'utf8')); expect(inventory.framingHash).toBe(sha256(frame));
    expect(readEvents(f.out).every(event => event.sourceBranch === 'main' && event.text.includes('sourceBranch') && event.text.includes('main'))).toBe(true);
    const snapshots: SourceSnapshot[] = JSON.parse(readFileSync(join(f.out, 'sources.json'), 'utf8')); expect(snapshots[0].sourceBranch).toBe('main');
    await ingest(f.out, { apply: true }); const adapter = new FixtureAdapter(); await drain(f.out, { apply: true, adapterFactory: () => adapter });
    expect(adapter.wireRequests.length).toBe(1); expect(adapter.disposed).toBe(true);
    const body = adapter.wireRequests[0]; const input = body.input as Array<{ content: Array<{ text: string }> }>;
    const texts = input.flatMap(item => item.content.map(block => block.text)).join('\n'); expect(texts).toContain(frame); expect(texts.split(frame).length - 1).toBe(1); expect(body.instructions).toBe(GROUND);
    const audit = JSON.parse(readFileSync(join(f.out, 'audit.json'), 'utf8')); expect(audit.lastRequest.serializedBodyUtf8Bytes).toBe(Buffer.byteLength(JSON.stringify(body), 'utf8')); expect(audit.lastRequest.outputReserve).toBe(8192);
    expect(audit.lastRequest.nativeEstimatedPromptTokens).toBeGreaterThan(0); expect(audit.lastResponse.usageSource).toBe('provider-normalized-token-usage'); expect((await verify(f.out)).verified).toBe(true);
  });
  test('I2/A5: declared MCP bearer credential is rejected before any candidate artifact', async () => {
    const f = makeFixture(); const recipe = JSON.parse(readFileSync(f.recipe, 'utf8')); const secret = 'sk-DISPOSABLE-LITERAL-MCP-CREDENTIAL';
    recipe.mcpServers = { fixture: { url: 'wss://fixture.invalid', token: secret } }; writeFileSync(f.recipe, JSON.stringify(recipe));
    await expect(prepare(f)).rejects.toThrow('literal credential'); expect(readdirSync(f.root)).not.toContain('candidate');
    expect(readFileSync(f.recipe, 'utf8')).toContain(secret); // Source recipe remains unchanged; no copy of its literal secret is made.
  });
  test('I2/B11/A5: literal declared MCP env keys refuse before output; unresolved private references survive', async () => {
    const f = makeFixture(); const recipe = JSON.parse(readFileSync(f.recipe, 'utf8'));
    for (const key of ['ZULIP_KEY', 'SERVICE_CREDENTIAL', 'API_TOKEN', 'SERVICE_AUTH']) {
      for (const value of ['disposable-private-value', 'disposable-private-value${UNRESOLVED}', '${UNRESOLVED:-disposable-private-value}']) {
        recipe.mcpServers = { fixture: { command: 'unused-fixture', env: { [key]: value } } }; writeFileSync(f.recipe, JSON.stringify(recipe));
        await expect(prepare(f)).rejects.toThrow('literal credential'); expect(readdirSync(f.root)).not.toContain('candidate');
        expect(JSON.parse(readFileSync(f.recipe, 'utf8')).mcpServers.fixture.env[key]).toBe(value);
      }
    }
    recipe.mcpServers = { fixture: { command: 'unused-fixture', env: { ZULIP_KEY: '${PRIVATE_RUNTIME_ZULIP_KEY}', SERVICE_CREDENTIAL: '${PRIVATE_RUNTIME_CREDENTIAL:-}', LANG: 'C.UTF-8' } } };
    writeFileSync(f.recipe, JSON.stringify(recipe)); await prepare(f);
    expect(JSON.parse(readFileSync(join(f.out, 'recipe.json'), 'utf8')).mcpServers).toEqual(recipe.mcpServers);
    expect((await verify(f.out)).verified).toBe(true);
  });
  test('B1/B8/A5: generated invocation quotes paths and pins model despite an inherited override', async () => {
    const f = makeFixture(); f.out = join(f.root, "candidate ' $fixture; untouched"); const result = await prepare(f);
    // A shell-local bun function captures arguments only: NEVER launch the host.
    const captured = Bun.spawnSync(['bash', '-c', `bun(){ printf '%s\\n' "$MODEL" "$DATA_DIR" "$@"; }; ${result.invocation}`], { env: { ...process.env, MODEL: 'incorrect-inherited-model' } });
    expect(captured.exitCode).toBe(0); expect(captured.stdout.toString().trim().split('\n')).toEqual([MODEL, join(f.out, 'data'), 'src/index.ts', join(f.out, 'recipe.json')]);
    expect((await verify(f.out)).verified).toBe(true);
  });
  test('B4: equal-length divergent active tails retain exact independent hashes and private recovery bytes', async () => {
    const a = makeFixture(); const b = makeFixture();
    const tailA = Buffer.from('{"partial":"A💜'); const tailB = Buffer.from('{"partial":"B💜');
    expect(tailA.length).toBe(tailB.length);
    const originals: Buffer[] = [];
    for (const [f, tail] of [[a, tailA], [b, tailB]] as const) {
      const file = join(f.root, 'omp.jsonl'); const raw = readFileSync(file); const prefix = raw.subarray(0, raw.lastIndexOf(10) + 1);
      const original = Buffer.concat([prefix, tail]); originals.push(original); writeFileSync(file, original);
      await prepare(f); expect(lstatSync(f.out).mode & 0o077).toBe(0);
    }
    const snapshot = (f: Fixture): SourceSnapshot => (JSON.parse(readFileSync(join(f.out, 'sources.json'), 'utf8')) as SourceSnapshot[]).find(source => source.id === 'omp')!;
    const first = snapshot(a); const second = snapshot(b);
    expect(first.hash).toBe(second.hash); expect(first.partialTailHash).not.toBe(second.partialTailHash);
    expect(first.partialTailHash).toBe(sha256(tailA)); expect(second.partialTailHash).toBe(sha256(tailB));
    for (const [index, f] of [a, b].entries()) {
      const source = snapshot(f); const prefix = readFileSync(join(f.out, source.snapshot!)); const tail = readFileSync(join(f.out, source.partialTailSnapshot!));
      expect(Buffer.concat([prefix, tail]).equals(originals[index])).toBe(true);
      const disposition = (JSON.parse(readFileSync(join(f.out, 'dispositions.json'), 'utf8')) as Disposition[]).find(item => item.source === 'omp' && item.recordId === 'partial-tail')!;
      expect(disposition.hash).toBe(sha256(tail)); expect(source.byteEnd).toBe(prefix.length); expect(source.lineEnd).toBe(9); expect(source.lastId).toBe('tool-end');
      expect((await verify(f.out)).verified).toBe(true);
      appendFileSync(join(f.root, 'omp.jsonl'), 'allowed suffix growth'); expect((await verify(f.out)).verified).toBe(true);
    }
    writeFileSync(join(a.out, first.partialTailSnapshot!), tailB);
    expect((await verify(a.out)).issues).toContain('source-partial-tail:omp');
  });
  test('B4/I5: capture and verify reject post-hash rewrites, retry unstable growth, and permit stable suffix growth', async () => {
    const f = makeFixture(); const manifest: SourceManifest = JSON.parse(readFileSync(f.manifest, 'utf8')); const source = manifest.sources.find(item => item.id === 'omp')!;
    manifest.sources = [source]; writeFileSync(f.manifest, JSON.stringify(manifest));
    const original = Buffer.from(canonical({ type: 'message', id: 'race', parentId: null, timestamp: '2026-09-01T08:00:01Z', message: { role: 'user', content: 'Before capture.' } }) + '\n' + '{"tail":"before"');
    const rewritten = Buffer.from(original.toString().replace('Before capture.', 'After! capture.').replace('"before"', '"after!"'));
    expect(original.length).toBe(rewritten.length); writeFileSync(source.path, original);
    const inode = lstatSync(source.path, { bigint: true }).ino; const actualFstat = fs.fstatSync;
    let calls = 0; let fired = 0; let action: (() => void) | undefined = () => { writeFileSync(source.path, rewritten); };
    const hook = spyOn(fs, 'fstatSync').mockImplementation((fd, options) => {
      const stats = actualFstat(fd, options);
      if (options?.bigint && stats.ino === inode && ++calls === 2 && action) { fired++; action(); return actualFstat(fd, options); }
      return stats;
    });
    try {
      await prepare(f); expect(fired).toBe(1); expect(calls).toBeGreaterThanOrEqual(4);
      const snapshot = (JSON.parse(readFileSync(join(f.out, 'sources.json'), 'utf8')) as SourceSnapshot[])[0];
      const prefix = readFileSync(join(f.out, snapshot.snapshot!)); const tail = readFileSync(join(f.out, snapshot.partialTailSnapshot!));
      expect(Buffer.concat([prefix, tail]).equals(rewritten)).toBe(true); expect(snapshot.hash).toBe(sha256(prefix)); expect(readEvents(f.out)[0].text).toContain('After! capture.');
      calls = 0; action = () => { writeFileSync(source.path, original); };
      expect(verifySourcePrefix(snapshot)).toBe('rewritten-or-replaced'); expect(calls).toBeGreaterThanOrEqual(4);
      writeFileSync(source.path, rewritten); calls = 0; action = () => { appendFileSync(source.path, 'legal suffix growth'); };
      expect(verifySourcePrefix(snapshot)).toBe('grown'); expect(calls).toBeGreaterThanOrEqual(4);
      action = undefined; expect(verifySourcePrefix(snapshot)).toBe('grown'); expect((await verify(f.out)).verified).toBe(true);
    } finally { hook.mockRestore(); }
  });
  test('I2/A4: secret-shaped linkage and provenance stay out of normalized text and actual memory requests', async () => {
    const f = makeFixture(); const toolId = 'sk-ABCDEFGHIJKLMNOPQRSTUV'; const sourceId = 'sk-SOURCEABCDEFGHIJKLMNOP';
    const firstId = 'sk-FIRSTABCDEFGHIJKLMNOP'; const streamId = 'sk-STREAMABCDEFGHIJKLMNOP'; const lastId = 'sk-LASTABCDEFGHIJKLMNOPQ';
    const account = 'fixture-private-account-value'; const privateThought = 'FIXTURE-NONPORTABLE-SECRET-THOUGHT';
    const records = [
      { type: 'user', uuid: firstId, parentUuid: null, timestamp: '2026-04-20T08:00:01Z', message: { role: 'user', content: f.longText.slice(0, 180000) } },
      { type: 'assistant', uuid: streamId, parentUuid: firstId, timestamp: '2026-04-20T08:00:02Z', message: { id: streamId, role: 'assistant', content: [{ type: 'thinking', thinking: privateThought }, { type: 'tool_use', id: toolId, name: sourceId, input: { account_id: account, access_token: toolId } }] } },
      { type: 'user', uuid: lastId, parentUuid: streamId, timestamp: '2026-04-20T08:00:03Z', message: { role: 'user', content: [{ type: 'tool_result', tool_use_id: toolId, content: 'Inert exact linked result.' }] } },
    ];
    const manifest: SourceManifest = JSON.parse(readFileSync(f.manifest, 'utf8')); const source = manifest.sources.find(item => item.id === 'claude')!;
    manifest.sources = [{ ...source, id: sourceId, branchHeads: [lastId] }]; writeFileSync(source.path, records.map(canonical).join('\n') + '\n'); writeFileSync(f.manifest, JSON.stringify(manifest));
    await prepare(f); const events = readEvents(f.out); const link = `redacted-id:${sha256(toolId)}`;
    expect(events.flatMap(event => event.tools).map(tool => tool.id)).toEqual([link, link]);
    for (const event of events) for (const secret of [toolId, sourceId, firstId, streamId, lastId, account, privateThought]) expect(event.text).not.toContain(secret);
    const imported = await ingest(f.out, { apply: true, maxEvents: 100 }); expect(imported.nativeMessages).toBeGreaterThan(events.length);
    const adapter = new FixtureAdapter(); await drain(f.out, { apply: true, maxSteps: 100, adapterFactory: () => adapter });
    expect(adapter.requests.length).toBeGreaterThan(0); expect(adapter.disposed).toBe(true);
    const captured = canonical(adapter.wireRequests); const publicReport = canonical(await status(f.out));
    for (const secret of [toolId, sourceId, firstId, streamId, lastId, account, privateThought]) { expect(captured).not.toContain(secret); expect(publicReport).not.toContain(secret); }
    expect(captured).toContain(link); for (const request of adapter.requests) { expect(request.model).toBe(MODEL); expect(systemText(request)).toBe(GROUND); expect(request.tools?.length ?? 0).toBe(0); }
    const snapshot = (JSON.parse(readFileSync(join(f.out, 'sources.json'), 'utf8')) as SourceSnapshot[])[0]; expect(readFileSync(join(f.out, snapshot.snapshot!), 'utf8')).toContain(toolId);
  });
  test('fit-audit: standalone local misses are not responses/failures and accepted native preimage correlates with final body', async () => {
    const f = makeFixture(); const manifest: SourceManifest = JSON.parse(readFileSync(f.manifest, 'utf8')); manifest.sources = manifest.sources.filter(source => source.kind === 'handoff'); writeFileSync(f.manifest, JSON.stringify(manifest)); await prepare(f);
    class LargeRecallAdapter extends FixtureAdapter {
      override async complete(request: ProviderRequest, options?: ProviderRequestOptions): Promise<ProviderResponse> {
        const response = await super.complete(request, options);
        return { ...response, content: [{ type: 'text', text: 'Exact disposable prior native recollection. ' + 'x'.repeat(310000) }] };
      }
    }
    let last: LargeRecallAdapter | undefined;
    for (let index = 0; index < 4; index++) { await ingest(f.out, { apply: true, maxEvents: 1 }); last = new LargeRecallAdapter(); await drain(f.out, { apply: true, maxSteps: 1, adapterFactory: () => last! }); expect(last.disposed).toBe(true); }
    expect(last!.requests.length).toBe(3); // Two auth-free local cap misses, then one admitted response.
    const audit = JSON.parse(readFileSync(join(f.out, 'audit.json'), 'utf8')) as { requests: number; responses: number; failures: number; lastFailure?: unknown; lastRefused: { totalTokenUpperBound: number }; lastRequest: { requestHash: string; transmittedBodyHash: string; serializedBodyUtf8Bytes: number; outputReserve: number; totalTokenUpperBound: number } };
    expect(audit.requests).toBe(4); expect(audit.responses).toBe(4); expect(audit.failures).toBe(0); expect(audit.lastFailure).toBeUndefined(); expect(audit.lastRefused.totalTokenUpperBound).toBeGreaterThan(604608);
    const body = last!.wireRequests.at(-1)!; expect(audit.lastRequest.transmittedBodyHash).toBe(sha256(JSON.stringify(body))); expect(audit.lastRequest.serializedBodyUtf8Bytes).toBe(Buffer.byteLength(JSON.stringify(body), 'utf8')); expect(audit.lastRequest.outputReserve).toBe(8192); expect(audit.lastRequest.totalTokenUpperBound).toBeLessThanOrEqual(604608);
    const inventory = JSON.parse(readFileSync(join(f.out, 'instance.json'), 'utf8')) as { sessionId: string }; const cm = await ContextManager.open({ path: new SessionManager(join(f.out, 'data')).getStorePath(inventory.sessionId), namespace: NAMESPACE, strategy: new AutobiographicalStrategy({ ...PROFILE, identityReminder: FRAME, autoTickOnNewMessage: false, logEffectiveConfig: false }) });
    try {
      const summaries = cm.getStore().getStateJson(`${NAMESPACE}/autobio:summaries`) as Array<SummaryReceipt & { provenance: { requestHash: string } }>; const minted = summaries.at(-1)!; const preimage = getMintRequestByHash(cm.getStore(), minted.provenance.requestHash)!;
      expect(sha256(JSON.stringify(preimage))).toBe(minted.provenance.requestHash); expect(sha256(canonical(preimage))).toBe(audit.lastRequest.requestHash);
      const kept = preimage.messages.flatMap(message => message.content.flatMap(block => block.type === 'text' ? [...block.text.matchAll(/^\[CM\] Recall memory (.+)\.$/g)].map(match => match[1]) : [])); expect(kept).toEqual([summaries[2].id]); assertNativeMembership(cm);
    } finally { cm.close(); }
    expect(JSON.stringify(audit)).not.toContain('Exact disposable prior native recollection');
  });
  test('fit-audit-floor: mandatory ground cap is failed/resumable debt with zero admitted requests or responses and unchanged fixed profile', async () => {
    const f = makeFixture(); const manifest: SourceManifest = JSON.parse(readFileSync(f.manifest, 'utf8')); manifest.sources = manifest.sources.filter(source => source.kind === 'handoff'); writeFileSync(f.manifest, JSON.stringify(manifest));
    const recipe = JSON.parse(readFileSync(f.recipe, 'utf8')); recipe.agent.systemPrompt = 'Disposable mandatory floor ground. ' + 'g'.repeat(605000); writeFileSync(f.recipe, JSON.stringify(recipe)); await prepare(f); await ingest(f.out, { apply: true, maxEvents: 1 });
    const immutable = Object.fromEntries(['instance.json', 'events.jsonl', 'ground.md', 'framing.md', 'recipe.json', 'checkpoint.json'].map(name => [name, sha256(readFileSync(join(f.out, name)))]));
    const adapter = new FixtureAdapter(); await expect(drain(f.out, { apply: true, adapterFactory: () => adapter })).rejects.toThrow('resumable'); expect(adapter.disposed).toBe(true); expect(adapter.requests.length).toBe(1);
    const audit = JSON.parse(readFileSync(join(f.out, 'audit.json'), 'utf8')) as { requests: number; responses: number; failures: number; lastFailure?: unknown; lastRefused: { totalTokenUpperBound: number; serializedBodyUtf8Bytes: number } };
    expect(audit.requests).toBe(0); expect(audit.responses).toBe(0); expect(audit.failures).toBe(1); expect(audit.lastFailure).toMatchObject({ classification: { kind: 'untyped', type: 'unknown' } }); expect(audit.lastRefused.totalTokenUpperBound).toBeGreaterThan(604608); expect(audit.lastRefused.serializedBodyUtf8Bytes).toBe(Buffer.byteLength(JSON.stringify(adapter.wireRequests[0]), 'utf8'));
    const failed = await status(f.out); expect(failed.noWork).toBe(false); expect(failed.quarantined).toBe(1); expect(failed.leaves).toBe(0); expect((await verify(f.out)).noWork).toBe(false);
    expect(Object.fromEntries(Object.keys(immutable).map(name => [name, sha256(readFileSync(join(f.out, name)))]))).toEqual(immutable);
    const idle = new FixtureAdapter(); await expect(drain(f.out, { apply: true, adapterFactory: () => idle })).rejects.toThrow('resumable'); expect(idle.requests.length).toBe(0); expect(idle.disposed).toBe(true);
    expect(JSON.stringify(audit)).not.toContain('Disposable mandatory floor ground');
  });
  const terminalSecret = 'DISPOSABLE-TERMINAL-PRIVATE https://private.example/account/path auth=fixture-token provider body source prose';
  const typedTerminalFailure = (fields: Record<string, unknown>): MembraneError => Object.assign(new MembraneError({
    type: 'network', retryable: true, message: terminalSecret,
    rawError: { message: terminalSecret, cause: terminalSecret }, rawRequest: { body: terminalSecret },
  }), { name: terminalSecret, stack: terminalSecret, cause: terminalSecret, providerErrorCode: terminalSecret }, fields);
  const terminalCases: Array<{ name: string; error: unknown; classification: Record<string, unknown> }> = [
    ...(['rate_limit', 'context_length', 'invalid_request', 'auth', 'server', 'network', 'timeout', 'abort', 'safety', 'unsupported', 'unknown'] as const).map(type => ({
      name: `allowlisted ${type}`, error: typedTerminalFailure({ type, retryable: false, httpStatus: 401 }), classification: { kind: 'membrane', type, retryable: false, httpStatus: 401 },
    })),
    { name: 'private type and invalid controls', error: typedTerminalFailure({ type: terminalSecret, retryable: terminalSecret, httpStatus: terminalSecret }), classification: { kind: 'membrane', type: 'unknown' } },
    { name: 'object-valued controls', error: typedTerminalFailure({ type: { toJSON: () => terminalSecret }, retryable: { toJSON: () => terminalSecret }, httpStatus: { toJSON: () => terminalSecret } }), classification: { kind: 'membrane', type: 'unknown' } },
    ...(['type', 'retryable', 'httpStatus', 'providerErrorCode'] as const).map(field => ({
      name: `F1 throwing ${field} accessor`,
      error: Object.defineProperty(typedTerminalFailure({ httpStatus: 503 }), field, { get() { throw new Error(terminalSecret); } }),
      classification: { kind: 'membrane', type: field === 'type' ? 'unknown' : 'network', ...(field === 'retryable' ? {} : { retryable: true }), ...(field === 'httpStatus' ? {} : { httpStatus: 503 }) },
    })),
    { name: 'F1 valid-looking type accessor is not evaluated', error: Object.defineProperty(typedTerminalFailure({}), 'type', { get() { return 'auth'; } }), classification: { kind: 'membrane', type: 'unknown', retryable: true } },
    { name: 'F1 throwing classification reflection trap', error: new Proxy(typedTerminalFailure({}), { getOwnPropertyDescriptor() { throw new Error(terminalSecret); } }), classification: { kind: 'membrane', type: 'unknown' } },
    ...[undefined, null, 0, 1, 'false'].map(retryable => ({ name: `invalid retryable ${String(retryable)}`, error: typedTerminalFailure({ retryable }), classification: { kind: 'membrane', type: 'network' } })),
    ...[99, 600, 200.5, NaN, Infinity, '401'].map(httpStatus => ({ name: `invalid status ${String(httpStatus)}`, error: typedTerminalFailure({ httpStatus }), classification: { kind: 'membrane', type: 'network', retryable: true } })),
    ...[100, 599].map(httpStatus => ({ name: `boundary status ${httpStatus}`, error: typedTerminalFailure({ httpStatus }), classification: { kind: 'membrane', type: 'network', retryable: true, httpStatus } })),
    { name: 'exact local cap', error: typedTerminalFailure({ type: 'context_length', retryable: false, providerErrorCode: ARCHIVAL_MEMORY_LOCAL_CAP_CODE }), classification: { kind: 'membrane', type: 'context_length', retryable: false, providerErrorCode: ARCHIVAL_MEMORY_LOCAL_CAP_CODE } },
    { name: 'local cap wrong type', error: typedTerminalFailure({ type: 'invalid_request', retryable: false, providerErrorCode: ARCHIVAL_MEMORY_LOCAL_CAP_CODE }), classification: { kind: 'membrane', type: 'invalid_request', retryable: false } },
    { name: 'local cap retryable', error: typedTerminalFailure({ type: 'context_length', retryable: true, providerErrorCode: ARCHIVAL_MEMORY_LOCAL_CAP_CODE }), classification: { kind: 'membrane', type: 'context_length', retryable: true } },
    { name: 'local cap arbitrary code', error: typedTerminalFailure({ type: 'context_length', retryable: false }), classification: { kind: 'membrane', type: 'context_length', retryable: false } },
    { name: 'plain private error with forged controls', error: Object.assign(new Error(`${terminalSecret} network timeout 401 rate limit context length`), { name: terminalSecret, stack: terminalSecret, cause: terminalSecret, type: 'context_length', retryable: false, httpStatus: 401, providerErrorCode: ARCHIVAL_MEMORY_LOCAL_CAP_CODE, rawError: terminalSecret, rawRequest: terminalSecret }), classification: { kind: 'untyped', type: 'unknown' } },
    { name: 'duck-typed plain object', error: { message: terminalSecret, name: 'MembraneError', type: 'auth', retryable: false, httpStatus: 401, providerErrorCode: terminalSecret, rawError: terminalSecret, rawRequest: terminalSecret }, classification: { kind: 'untyped', type: 'unknown' } },
    { name: 'private thrown string', error: terminalSecret, classification: { kind: 'untyped', type: 'unknown' } },
  ];
  for (const fixture of terminalCases) test(`terminal-audit B1-B4/A1-A3: ${fixture.name}`, async () => {
    const f = makeFixture(); writeFileSync(f.manifest, JSON.stringify({ version: 1, cutoff: CUTOFF, sources: [] } satisfies SourceManifest));
    await prepare(f); expect((await status(f.out)).noWork).toBe(true);
    const stale = { lastRequest: { requestHash: 'stale-request', requestPhase: 'admitted-final-body-before-auth-fetch', operation: 'stale-operation' }, lastResponse: { model: MODEL, marker: 'stale-response' }, lastRefused: { marker: 'stale-refusal' } };
    const auditPath = join(f.out, 'audit.json'); const before = JSON.parse(readFileSync(auditPath, 'utf8'));
    writeFileSync(auditPath, JSON.stringify({ ...before, requests: 7, responses: 6, ...stale }));
    let outward: Error | undefined;
    try { await drain(f.out, { apply: true, adapterFactory: () => { throw fixture.error; } }); } catch (error) { outward = error as Error; }
    expect(outward).toBeInstanceOf(Error); expect(outward!.message).toBe('Backfill drain failed; candidate remains resumable (see safe audit/verification state)');
    const audit = JSON.parse(readFileSync(auditPath, 'utf8')); const failed = await status(f.out);
    expect(failed.noWork).toBe(false); expect(failed.audit).toEqual(audit); expect(audit).toMatchObject({ requests: 7, responses: 6, failures: 1, ...stale });
    expect(audit.lastFailure).toEqual({ kind: 'failed-resumable-work', messageHash: sha256(fixture.error instanceof Error ? fixture.error.message : String(fixture.error)), at: expect.any(String), classification: fixture.classification });
    expect(Number.isFinite(Date.parse(audit.lastFailure.at))).toBe(true);
    const receipt = JSON.stringify(audit.lastFailure);
    for (const marker of ['stale-request', 'stale-response', 'stale-refusal', 'stale-operation', 'requestPhase', 'requestHash', 'operation', 'admission', 'credential', 'commit', 'SSE']) expect(receipt).not.toContain(marker);
    const serialized = JSON.stringify({ audit, status: failed, error: { message: outward!.message, name: outward!.name, stack: outward!.stack } });
    expect(serialized).not.toContain(terminalSecret);
    for (const field of ['rawError', 'rawRequest', 'cause', 'provider prose']) expect(serialized).not.toContain(field);
    const hashes = treeHashes(f.out); expect((await verify(f.out)).noWork).toBe(false); expect(treeHashes(f.out)).toEqual(hashes);
    const adapter = new FixtureAdapter(); const recovered = await drain(f.out, { apply: true, adapterFactory: () => adapter });
    expect(adapter.disposed).toBe(true); expect(adapter.requests.length).toBe(0); expect(recovered.noWork).toBe(true); expect(recovered.nativeMessages).toBe(0);
    expect(recovered.audit).toEqual({ ...audit, lastFailure: undefined }); expect((await verify(f.out)).verified).toBe(true);
  });
  test('terminal-audit B1-B5/A1-A3: retryable provider merge failure is classified, closed and recovered without duplicate membership', async () => {
    const f = makeFixture(); const manifest: SourceManifest = JSON.parse(readFileSync(f.manifest, 'utf8')); manifest.sources = manifest.sources.filter(source => source.kind === 'handoff'); writeFileSync(f.manifest, JSON.stringify(manifest));
    await prepare(f);
    for (let index = 0; index < 4; index++) { await ingest(f.out, { apply: true, maxEvents: 1 }); await drain(f.out, { apply: true, maxSteps: 1, adapterFactory: () => new FixtureAdapter() }); }
    const checkpointBefore = readFileSync(join(f.out, 'checkpoint.json'), 'utf8');
    const summariesBefore = JSON.parse(readFileSync(join(f.out, 'inspection.json'), 'utf8')).summaries as SummaryReceipt[];
    class RetryableAdapter extends FixtureAdapter {
      override async complete(request: ProviderRequest, options?: ProviderRequestOptions): Promise<ProviderResponse> {
        await super.complete(request, options);
        throw typedTerminalFailure({ type: 'network', retryable: true, httpStatus: 503 });
      }
    }
    const failedAdapter = new RetryableAdapter(); const cmClose = spyOn(ContextManager.prototype, 'close'); const storeClose = spyOn(JsStore.prototype, 'close');
    const logged: unknown[][] = []; const consoleError = spyOn(console, 'error').mockImplementation((...args) => { logged.push(args); });
    try {
      await expect(drain(f.out, { apply: true, maxSteps: 1, adapterFactory: () => failedAdapter })).rejects.toThrow('resumable');
      expect(cmClose).toHaveBeenCalledTimes(1); expect(storeClose).toHaveBeenCalledTimes(1);
    } finally { cmClose.mockRestore(); storeClose.mockRestore(); consoleError.mockRestore(); }
    expect(failedAdapter.disposed).toBe(true); expect(failedAdapter.requests.length).toBe(1);
    const failed = await status(f.out); const audit = failed.audit as { requests: number; responses: number; failures: number; lastFailure: { messageHash: string; classification: unknown }; lastRequest: unknown; lastResponse: unknown };
    expect(failed.noWork).toBe(false); expect(failed.mergeQuarantined).toBe(0); expect(failed.quarantined).toBe(0);
    expect(audit).toMatchObject({ requests: 5, responses: 4, failures: 1, lastFailure: { kind: 'failed-resumable-work', classification: { kind: 'membrane', type: 'network', retryable: true, httpStatus: 503 } } });
    expect(audit.lastFailure.messageHash).toBe(sha256(`Codex memory request failed [${sha256(terminalSecret).slice(0, 16)}]; work remains resumable`));
    expect(Object.keys(audit.lastFailure).sort()).toEqual(['at', 'classification', 'kind', 'messageHash']);
    expect(readFileSync(join(f.out, 'checkpoint.json'), 'utf8')).toBe(checkpointBefore);
    const cli = resolve(import.meta.dir, '../scripts/backfill-history.ts'); const statusCLI = Bun.spawnSync(['bun', cli, 'status', f.out, '--json']);
    expect(statusCLI.exitCode).toBe(0); expect(JSON.parse(statusCLI.stdout.toString()).audit).toEqual(audit);
    expect(JSON.stringify({ audit, failed, logged, stdout: statusCLI.stdout.toString(), stderr: statusCLI.stderr.toString() })).not.toContain(terminalSecret);
    const recoveredAdapter = new FixtureAdapter(); const recovered = await drain(f.out, { apply: true, maxSteps: 1, adapterFactory: () => recoveredAdapter });
    expect(recoveredAdapter.disposed).toBe(true); expect(recoveredAdapter.requests.length).toBe(1); expect(recovered.noWork).toBe(true);
    expect(recovered.audit).toMatchObject({ requests: 6, responses: 5, failures: 1 }); expect(recovered.audit).not.toHaveProperty('lastFailure');
    const inventory = JSON.parse(readFileSync(join(f.out, 'instance.json'), 'utf8')) as { sessionId: string };
    const cm = await ContextManager.open({ path: new SessionManager(join(f.out, 'data')).getStorePath(inventory.sessionId), namespace: NAMESPACE, strategy: new AutobiographicalStrategy({ ...PROFILE, autoTickOnNewMessage: false, identityReminder: FRAME, logEffectiveConfig: false }) });
    try {
      assertNativeMembership(cm);
      const checkpoint = JSON.parse(checkpointBefore);
      const sourceIds = Object.values(checkpoint.mapping).flatMap(entry => {
        if (!entry || typeof entry !== 'object' || !('nativeIds' in entry) || !Array.isArray(entry.nativeIds)) throw new Error('Invalid disposable checkpoint mapping');
        return entry.nativeIds;
      });
      expect(cm.getAllMessages().map(message => message.id)).toEqual(sourceIds);
      const summaries = cm.getStore().getStateJson(`${NAMESPACE}/autobio:summaries`) as SummaryReceipt[]; expect(summaries.length).toBe(5);
      const summariesAfter = JSON.parse(readFileSync(join(f.out, 'inspection.json'), 'utf8')).summaries as SummaryReceipt[];
      for (const prior of summariesBefore) { const { mergedInto, ...current } = summariesAfter.find(summary => summary.id === prior.id)!; expect(current).toEqual(prior); expect(mergedInto).toBeDefined(); }
    } finally { cm.close(); }
  });
  test('B10: failed empty drain is not no-work; successful recovery clears only active failure', async () => {
    const f = makeFixture(); writeFileSync(f.manifest, JSON.stringify({ version: 1, cutoff: CUTOFF, sources: [] } satisfies SourceManifest));
    await prepare(f); expect((await status(f.out)).noWork).toBe(true);
    await expect(drain(f.out, { apply: true, adapterFactory: () => { throw new Error('Disposable missing-provider prerequisite'); } })).rejects.toThrow('resumable');
    const failed = await status(f.out); const failedAudit = failed.audit as { failures: number; lastFailure?: unknown };
    expect(failed.noWork).toBe(false); expect(failedAudit.failures).toBe(1); expect(failedAudit.lastFailure).toBeDefined();
    const before = treeHashes(f.out); expect((await verify(f.out)).noWork).toBe(false); expect(treeHashes(f.out)).toEqual(before);
    const adapter = new FixtureAdapter(); const recovered = await drain(f.out, { apply: true, adapterFactory: () => adapter });
    expect(adapter.requests.length).toBe(0); expect(adapter.disposed).toBe(true); expect(recovered.noWork).toBe(true);
    const audit = recovered.audit as { failures: number; lastFailure?: unknown }; expect(audit.failures).toBe(1); expect(audit.lastFailure).toBeUndefined(); expect((await verify(f.out)).verified).toBe(true);
  });
  test('B10/A3: durable merge quarantine is failed work and explicit native clear resumes without source surgery', async () => {
    const f = makeFixture(); const manifest: SourceManifest = JSON.parse(readFileSync(f.manifest, 'utf8')); manifest.sources = manifest.sources.filter(source => source.kind === 'handoff'); writeFileSync(f.manifest, JSON.stringify(manifest));
    await prepare(f);
    for (let i = 0; i < 4; i++) { await ingest(f.out, { apply: true, maxEvents: 1 }); await drain(f.out, { apply: true, maxSteps: 1, adapterFactory: () => new FixtureAdapter() }); }
    const earlier = JSON.parse(readFileSync(join(f.out, 'inspection.json'), 'utf8')) as { summaries: SummaryReceipt[] }; expect(earlier.summaries.length).toBe(4);
    const refused = new FixtureAdapter(); refused.refuse = true; await expect(drain(f.out, { apply: true, maxSteps: 5, adapterFactory: () => refused })).rejects.toThrow('resumable'); expect(refused.disposed).toBe(true); expect(refused.requests.length).toBe(5);
    const failed = await status(f.out); expect(failed.noWork).toBe(false); expect(failed.mergeQueue).toBe(0); expect(failed.mergeQuarantined).toBe(1); expect(failed.quarantined).toBe(1); expect((failed.audit as { lastFailure?: unknown }).lastFailure).toBeDefined();
    const hashes = treeHashes(f.out); expect((await verify(f.out)).noWork).toBe(false); expect(treeHashes(f.out)).toEqual(hashes);
    const idle = new FixtureAdapter(); await expect(drain(f.out, { apply: true, maxSteps: 1, adapterFactory: () => idle })).rejects.toThrow('resumable');
    expect(idle.requests.length).toBe(0); expect(idle.disposed).toBe(true);
    const stillFailed = await status(f.out); expect(stillFailed.noWork).toBe(false); expect(stillFailed.mergeQuarantined).toBe(1);
    expect(stillFailed.quarantined).toBe(1); expect(stillFailed.audit).toMatchObject({ failures: 2, lastFailure: expect.any(Object) });
    const inventory = JSON.parse(readFileSync(join(f.out, 'instance.json'), 'utf8')) as { sessionId: string }; const strategy = new AutobiographicalStrategy({ ...PROFILE, autoTickOnNewMessage: false, identityReminder: FRAME, logEffectiveConfig: false });
    const cm = await ContextManager.open({ path: new SessionManager(join(f.out, 'data')).getStorePath(inventory.sessionId), namespace: NAMESPACE, strategy }); let sources: string[];
    try {
      sources = cm.getAllMessages().map(message => message.id); expect(cm.isReady()).toBe(false); expect(strategy.getMergeQuarantineStatus().count).toBe(1); assertNativeMembership(cm);
      strategy.clearMergeQuarantine(strategy.getMergeQuarantineStatus().records[0].key); cm.sync(); expect(strategy.getProgressSnapshot().mergeQueueLength).toBe(1);
    } finally { cm.close(); }
    const recovered = new FixtureAdapter(); const result = await drain(f.out, { apply: true, maxSteps: 10, adapterFactory: () => recovered }); expect(recovered.disposed).toBe(true); expect(recovered.requests.length).toBe(1); expect(result.noWork).toBe(true); expect(result.mergeQuarantined).toBe(0); expect(result.leaves).toBe(4); expect(result.merges).toBe(1);
    const audit = result.audit as { failures: number; lastFailure?: unknown }; expect(audit.failures).toBe(2); expect(audit.lastFailure).toBeUndefined(); expect((await verify(f.out)).verified).toBe(true);
    const after = JSON.parse(readFileSync(join(f.out, 'inspection.json'), 'utf8')) as { summaries: SummaryReceipt[] };
    for (const { mergedInto: _, ...prior } of earlier.summaries) { const { mergedInto: parent, ...current } = after.summaries.find(summary => summary.id === prior.id)!; expect(current).toEqual(prior); expect(parent).toBeDefined(); }
    const reopened = await ContextManager.open({ path: new SessionManager(join(f.out, 'data')).getStorePath(inventory.sessionId), namespace: NAMESPACE, strategy: new AutobiographicalStrategy({ ...PROFILE, autoTickOnNewMessage: false, identityReminder: FRAME, logEffectiveConfig: false }) });
    try { expect(reopened.getAllMessages().map(message => message.id)).toEqual(sources!); assertNativeMembership(reopened); } finally { reopened.close(); }
  });
  test('A3/A5/B10: SIGKILL after durable checkpoint cannot report no-work from a stale prepared inspection', async () => {
    const f = makeFixture(); const manifest: SourceManifest = JSON.parse(readFileSync(f.manifest, 'utf8')); manifest.sources = manifest.sources.filter(source => source.kind === 'handoff'); writeFileSync(f.manifest, JSON.stringify(manifest));
    await prepare(f); const helper = new URL('../scripts/lib/backfill-instance.ts', import.meta.url).href;
    const child = spawn('bun', ['-e', `import {ingest} from ${JSON.stringify(helper)}; await ingest(${JSON.stringify(f.out)}, {apply:true,interrupt:(point)=>{if(point==='checkpoint-before-inspection')process.kill(process.pid,'SIGKILL')}});`], { stdio: 'ignore' });
    await new Promise<void>(done => child.once('exit', () => done())); expect(child.signalCode).toBe('SIGKILL');
    const hashes = treeHashes(f.out); const crashed = await status(f.out); expect(crashed.pending).toBe(0); expect(crashed.nativeMessages).toBe(0); expect(crashed.noWork).toBe(false); expect(crashed.recoveryRequired).toBe(true); expect(crashed.inspection).toBe('stale-requires-writer-recovery'); expect((await verify(f.out)).verified).toBe(false); expect(treeHashes(f.out)).toEqual(hashes);
    const checkpoint = JSON.parse(readFileSync(join(f.out, 'checkpoint.json'), 'utf8')) as { mapping: Record<string, { nativeIds: string[] }> }; const earlierIds = Object.values(checkpoint.mapping).flatMap(entry => entry.nativeIds);
    const resumed = await ingest(f.out, { apply: true }); expect(resumed.nativeMessages).toBe(4); expect(resumed.imported).toBe(4); expect(resumed.noWork).toBe(false); expect(resumed.recoveryRequired).toBe(false);
    const adapter = new FixtureAdapter(); const final = await drain(f.out, { apply: true, maxSteps: 10, adapterFactory: () => adapter }); expect(adapter.disposed).toBe(true); expect(final.noWork).toBe(true); expect(adapter.requests.length).toBe(1); expect((await verify(f.out)).verified).toBe(true);
    const inventory = JSON.parse(readFileSync(join(f.out, 'instance.json'), 'utf8')) as { sessionId: string }; const cm = await ContextManager.open({ path: new SessionManager(join(f.out, 'data')).getStorePath(inventory.sessionId), namespace: NAMESPACE, strategy: new AutobiographicalStrategy({ ...PROFILE, autoTickOnNewMessage: false, identityReminder: FRAME, logEffectiveConfig: false }) });
    try { expect(cm.getAllMessages().map(message => message.id)).toEqual(earlierIds); assertNativeMembership(cm); } finally { cm.close(); }
  }, 60000);
  test('A2: explicit record selections include complete streamed groups without hiding missing ancestry', async () => {
    const f = makeFixture(); const manifest: SourceManifest = JSON.parse(readFileSync(f.manifest, 'utf8'));
    manifest.sources = manifest.sources.filter(s => s.id === 'claude'); manifest.sources[0].recordIds = ['c2']; writeFileSync(f.manifest, JSON.stringify(manifest));
    await prepare(f); const events = readEvents(f.out); expect(events.length).toBe(1); expect(events[0].recordIds).toEqual(['c2', 'c3']); expect(events[0].partialStream).toBe(false);
    expect(events[0].parentId).toBe('c1'); expect(events[0].parentInCorpus).toBe(false); expect(events[0].gapReason).toBe('parent-is-outside-selection-or-a-control-projection');
  });
  test('A3/A5: append kill before checkpoint, two batches, same-session reopen, grounded native leaf/merge, read-only reports', async () => {
    const f = makeFixture(); await prepare(f);
    // Real process death, after public native append+sync and before sidecar checkpoint.
    const helper = new URL('../scripts/lib/backfill-instance.ts', import.meta.url).href;
    const child = spawn('bun', ['-e', `import {ingest} from ${JSON.stringify(helper)}; await ingest(${JSON.stringify(f.out)}, {apply:true,maxEvents:2,interrupt:()=>process.kill(process.pid,'SIGKILL')});`], { stdio: 'ignore' });
    await new Promise<void>(done => child.once('exit', () => done())); expect(child.signalCode).toBe('SIGKILL');
    expect((await verify(f.out)).recoveryRequired).toBe(true);
    const first = await ingest(f.out, { apply: true, maxEvents: 10 }); const sessionId = String(first.sessionId);
    expect(first.imported).toBe(11); // One complete fsynced event recovered, ten appended.
    const adapter1 = new FixtureAdapter(); await drain(f.out, { apply: true, maxSteps: 100, adapterFactory: () => adapter1 }); expect(adapter1.disposed).toBe(true);
    const firstInspection = JSON.parse(readFileSync(join(f.out, 'inspection.json'), 'utf8')) as { summaries: SummaryReceipt[] };
    expect(firstInspection.summaries.length).toBeGreaterThan(1);
    const firstSummaries = structuredClone(firstInspection.summaries);
    const second = await ingest(f.out, { apply: true, maxEvents: 100 }); expect(second.sessionId).toBe(sessionId); expect(second.imported).toBe(readEvents(f.out).length);
    const adapter = new FixtureAdapter(); const final = await drain(f.out, { apply: true, maxSteps: 100, adapterFactory: () => adapter }); expect(adapter.disposed).toBe(true); expect(final.leaves).toBeGreaterThanOrEqual(4); expect(final.merges).toBeGreaterThan(0); expect(final.sourceLinksValid).toBe(true);
    const after = JSON.parse(readFileSync(join(f.out, 'inspection.json'), 'utf8')) as { summaries: SummaryReceipt[] };
    for (const earlier of firstSummaries) {
      const preserved = after.summaries.find(summary => summary.id === earlier.id);
      expect(preserved).toBeDefined();
      const { mergedInto: earlierParent, ...immutable } = earlier;
      const { mergedInto: parent, ...current } = preserved!;
      expect(current).toEqual(immutable);
      if (earlierParent) expect(parent).toBe(earlierParent);
    }
    const noOp = await ingest(f.out, { apply: true, maxEvents: 100 }); expect(noOp.nativeMessages).toBe(final.nativeMessages);
    const index = new SessionManager(join(f.out, 'data')); expect(index.load().activeSessionId).toBe(sessionId); expect(index.listSessions().length).toBe(1);
    const runtimeRecipe = await loadRecipe(join(f.out, 'recipe.json')); expect(runtimeRecipe.agent.maxStreamTokens).toBe(2418432); expect(runtimeRecipe.agent.strategy!.targetChunkTokens).toBe(24000);
    const strategy = buildFrameworkStrategy(runtimeRecipe, MODEL, 'Europe/Stockholm'); const store = JsStore.open({ path: index.getStorePath(sessionId) }); let cm: ContextManager | undefined;
    try {
      cm = await ContextManager.open({ store, strategy, namespace: NAMESPACE });
      assertNativeMembership(cm);
      const messages = cm.getAllMessages(); expect(messages.length).toBe(final.nativeMessages); expect(messages.every(m => !m.bodyGroupId)).toBe(true, 'native memory targets must not reassemble a huge source body');
      const event = readEvents(f.out).find(e => e.recordIds.includes('o5'))!;
      const parts = messages.filter(m => m.metadata?.backfillKey === event.key); expect(parts.length).toBeGreaterThan(1);
      const recovered = parts.map(m => m.content.filter(b => b.type === 'text').map(b => b.text).join('').slice(Number(m.metadata!.sourceEnvelopeChars))).join(''); expect(recovered).toBe(event.text);
      const partIds = new Set(parts.map(m => m.id)); expect(cm.getSummariesInRange({ level: 1 }).filter(s => s.sourceIds.some(id => partIds.has(id))).length).toBeGreaterThan(1);
      expect(parts.every(m => m.timestamp.getTime() === event.timestampMs)).toBe(true);
      expect(cm.queryMessagesByTime({ fromMs: event.timestampMs, toMs: event.timestampMs }).messages.length).toBe(parts.length);
      const allSummaries = cm.getStore().getStateJson(`${NAMESPACE}/autobio:summaries`) as SummaryReceipt[];
      const byId = new Map(allSummaries.map(summary => [summary.id, summary])); const l3 = allSummaries.find(summary => summary.level === 3)!;
      expect(l3.sourceIds.length).toBe(4); const l2s = l3.sourceIds.map(id => byId.get(id)!); expect(l2s.every(summary => summary.level === 2 && summary.sourceIds.length === 4)).toBe(true);
      const leafIds = l2s.flatMap(summary => summary.sourceIds); expect(leafIds.length).toBe(16);
      const coveredRaw = new Set(leafIds.flatMap(id => byId.get(id)!.sourceIds));
      const rawBytes = parts.filter(message => coveredRaw.has(message.id)).map(message => Buffer.byteLength(message.content.filter(block => block.type === 'text').map(block => block.text).join('').slice(Number(message.metadata!.sourceEnvelopeChars)), 'utf8'));
      expect(rawBytes.filter(bytes => bytes >= 90000).length).toBeGreaterThanOrEqual(12); expect(rawBytes.reduce((sum, bytes) => sum + bytes, 0)).toBeGreaterThan(1000000);
      const l3Requests = [...adapter1.wireRequests, ...adapter.wireRequests].filter(request => canonical(request.input).includes('single L3 memory'));
      expect(l3Requests.length).toBe(1); const l3Bytes = Buffer.byteLength(canonical(l3Requests[0]), 'utf8');
      expect(l3Bytes).toBeLessThan(50000); expect(canonical(l3Requests[0].input)).not.toContain('Unicode 💜'); expect(canonical(l3Requests[0].input)).toContain('Recall memory L1-');
      console.log(JSON.stringify({ proof: 'large-source-native-L3', l2Children: l2s.length, l1Leaves: leafIds.length, fullSizeFragments: rawBytes.filter(bytes => bytes >= 90000).length, coveredOriginalUtf8Bytes: rawBytes.reduce((sum, bytes) => sum + bytes, 0), serializedL3EnvelopeUtf8Bytes: l3Bytes, serializedMaxEnvelopeUtf8Bytes: Math.max(...[...adapter1.wireRequests, ...adapter.wireRequests].map(request => Buffer.byteLength(canonical(request), 'utf8'))) }));
      const summary = cm.getSummariesInRange({ level: 1 })[0]; expect(cm.getSummary(summary.id)).not.toBeNull();
    } finally { cm?.close(); store.close(); }
    // Reopen initialization can advance native maintenance; restore the writer
    // receipt using the supported no-op ingest, then prove readers mutate NOTHING.
    await ingest(f.out, { apply: true }); const before = treeHashes(f.out);
    const report = await status(f.out); const verified = await verify(f.out); expect(verified.verified).toBe(true); expect(treeHashes(f.out)).toEqual(before); expect(report.effectiveProfile).toEqual(PROFILE);
    expect(adapter1.wireRequests.length).toBe(adapter1.requests.length); expect(adapter.wireRequests.length).toBe(adapter.requests.length);
    for (const request of [...adapter1.wireRequests, ...adapter.wireRequests]) { expect(request.model).toBe(MODEL); expect(request.instructions).toBe(GROUND); expect(canonical(request.input)).toContain(FRAME); expect(canonical(request.input).split(FRAME).length - 1).toBe(1); expect(request.tools?.length ?? 0).toBe(0); expect(request.normalizedMessages).toBeUndefined(); }
  }, 60000);
  test('B7/B10/A5: generated real Agent primary is carrier-only; runtime native memory gets exact framing once', async () => {
    const f = makeFixture(); const manifest: SourceManifest = JSON.parse(readFileSync(f.manifest, 'utf8')); manifest.sources = manifest.sources.filter(source => source.kind === 'note' && source.decision === 'include'); writeFileSync(f.manifest, JSON.stringify(manifest));
    await prepare(f); await ingest(f.out, { apply: true }); const leafAdapter = new FixtureAdapter(); await drain(f.out, { apply: true, maxSteps: 100, adapterFactory: () => leafAdapter });
    const recipe = await loadRecipe(join(f.out, 'recipe.json')); expect(recipe.agent.systemPrompt).toBe(GROUND); expect(recipe.agent.systemPrompt).not.toContain(FRAME); expect(recipe.agent.strategy!.identityReminder).toBe(FRAME);
    const inventory = JSON.parse(readFileSync(join(f.out, 'instance.json'), 'utf8')) as { sessionId: string; groundHash: string; framingHash: string };
    expect(inventory.groundHash).toBe(sha256(GROUND)); expect(inventory.framingHash).toBe(sha256(FRAME));
    const auxiliary = new FixtureAdapter(); const primary = new FixtureAdapter(); const tools = [{ name: 'ordinary_fixture', description: 'Inert fixture, never executed.', inputSchema: { type: 'object' as const } }];
    const memoryTransport = new BackfillMembrane(auxiliary, { formatter: new OpenAIResponsesFormatter(), assistantParticipant: 'liv', retry: { maxRetries: 0 }, hooks: { beforeRequest: (normalized, providerRequest) => { validateMemoryGround(normalized, providerRequest as ProviderRequest, GROUND, FRAME); } } });
    const primaryTransport = new BackfillMembrane(primary, { formatter: new OpenAIResponsesFormatter(), assistantParticipant: 'liv', retry: { maxRetries: 0 } });
    const strategy = buildFrameworkStrategy(recipe, MODEL, 'Europe/Stockholm'); const store = JsStore.open({ path: new SessionManager(join(f.out, 'data')).getStorePath(inventory.sessionId) }); let cm: ContextManager | undefined; let agent: Agent | undefined;
    try {
      cm = await ContextManager.open({ store, strategy, membrane: memoryTransport, namespace: NAMESPACE });
      agent = new Agent({ name: 'liv', model: recipe.agent.model, systemPrompt: recipe.agent.systemPrompt, contextBudgetTokens: recipe.agent.contextBudgetTokens, maxTokens: recipe.agent.maxTokens }, cm, primaryTransport);
      const before = store.currentSequence(); const preview = await agent.buildActivationRequest(tools); expect(preview.system).toBe(GROUND); expect(canonical(preview)).not.toContain(FRAME); expect(store.currentSequence()).toBe(before); expect(primary.wireRequests.length).toBe(0);
      const started = await agent.startStreamWithInjections(tools);
      for await (const event of started.stream) if (event.type === 'error') throw event.error;
      expect(primary.wireRequests.length).toBe(1); const actual = primary.wireRequests[0]; expect(actual.instructions).toBe(GROUND); expect(canonical(actual)).not.toContain(FRAME); expect(actual.tools?.length).toBe(1);
      await cm.resetHeadWindow(); expect(auxiliary.wireRequests.length).toBe(1);
      for (const request of [...leafAdapter.wireRequests, ...auxiliary.wireRequests]) { expect(request.instructions).toBe(GROUND); expect(canonical(request.input).split(FRAME).length - 1).toBe(1); expect(request.tools?.length ?? 0).toBe(0); }
    } finally { agent?.reset(); try { cm?.close(); } finally { store.close(); auxiliary.dispose(); primary.dispose(); } }
    expect(auxiliary.disposed).toBe(true); expect(primary.disposed).toBe(true);
  });
  test('A3/B9: actual SIGKILL between large UTF-safe fragment append and seal resumes exact dated ranges', async () => {
    const f = makeFixture(); const manifest: SourceManifest = JSON.parse(readFileSync(f.manifest, 'utf8'));
    manifest.sources = manifest.sources.filter(source => source.id === 'omp'); manifest.sources[0].recordIds = ['o5']; writeFileSync(f.manifest, JSON.stringify(manifest));
    await prepare(f); const helper = new URL('../scripts/lib/backfill-instance.ts', import.meta.url).href;
    const child = spawn('bun', ['-e', `import {ingest} from ${JSON.stringify(helper)}; let count=0; await ingest(${JSON.stringify(f.out)}, {apply:true,maxEvents:1,interrupt:()=>{if(++count===2)process.kill(process.pid,'SIGKILL')}});`], { stdio: 'ignore' });
    await new Promise<void>(done => child.once('exit', () => done())); expect(child.signalCode).toBe('SIGKILL'); expect((await verify(f.out)).recoveryRequired).toBe(true);
    const inventory = JSON.parse(readFileSync(join(f.out, 'instance.json'), 'utf8')) as { sessionId: string };
    const path = new SessionManager(join(f.out, 'data')).getStorePath(inventory.sessionId);
    let cm = await ContextManager.open({ path, namespace: NAMESPACE }); let firstIds: string[];
    try { firstIds = cm.getAllMessages().map(message => message.id); expect(firstIds.length).toBe(2); } finally { cm.close(); }
    const premature = new FixtureAdapter(); await expect(drain(f.out, { apply: true, maxSteps: 1, adapterFactory: () => premature })).rejects.toThrow('resumable'); expect(premature.requests.length).toBe(0); expect(premature.disposed).toBe(true);
    const resumed = await ingest(f.out, { apply: true, maxEvents: 1 }); expect(resumed.imported).toBe(1); expect(resumed.sessionId).toBe(inventory.sessionId);
    cm = await ContextManager.open({ path, namespace: NAMESPACE });
    try {
      const parts = cm.getAllMessages(); const event = readEvents(f.out)[0]; expect(parts.slice(0, 2).map(message => message.id)).toEqual(firstIds!);
      expect(parts.map(message => message.content.filter(block => block.type === 'text').map(block => block.text).join('').slice(Number(message.metadata!.sourceEnvelopeChars))).join('')).toBe(event.text);
      expect(parts.every(message => message.timestamp.getTime() === event.timestampMs)).toBe(true); expect(cm.queryMessagesByTime({ fromMs: event.timestampMs, toMs: event.timestampMs }).messages.length).toBe(parts.length);
    } finally { cm.close(); }
  }, 60000);
  test('A3/A4: conflicting content and concurrent writers refused; failed provider disposed and resumes without empty memories', async () => {
    const f = makeFixture(); await prepare(f);
    await withWriter(f.out, async () => { await expect(ingest(f.out, { apply: true })).rejects.toThrow('writer'); });
    await ingest(f.out, { apply: true, maxEvents: 2 });
    const failed = new FixtureAdapter(); failed.fail = true; await expect(drain(f.out, { apply: true, maxSteps: 1, adapterFactory: () => failed })).rejects.toThrow('resumable'); expect(failed.disposed).toBe(true); expect((await status(f.out)).leaves).toBe(0);
    const recovered = new FixtureAdapter(); await drain(f.out, { apply: true, maxSteps: 1, adapterFactory: () => recovered }); expect(recovered.disposed).toBe(true); expect((await status(f.out)).leaves).toBe(1);
    const inventory = JSON.parse(readFileSync(join(f.out, 'instance.json'), 'utf8')) as { sessionId: string };
    const store = JsStore.open({ path: new SessionManager(join(f.out, 'data')).getStorePath(inventory.sessionId) }); let cm: ContextManager | undefined;
    try { cm = await ContextManager.open({ store }); const id = cm.getAllMessages()[0].id; cm.editMessage(id, [{ type: 'text', text: 'Conflicting payload' }]); cm.sync(); } finally { cm?.close(); store.close(); }
    await expect(ingest(f.out, { apply: true })).rejects.toThrow('conflicts');
  });
  test('A4: exact model/ground/frame/no-tools validation, actual-shaped auxiliary request and supported Codex disposal without auth access', async () => {
    const request: NormalizedRequest = { system: GROUND, config: { model: MODEL, maxTokens: 8192 }, messages: [{ participant: 'Context Manager', content: [{ type: 'text', text: FRAME }] }] };
    const raw: ProviderRequest = { system: GROUND, model: MODEL, maxTokens: 8192, messages: [{ role: 'user', content: FRAME }] };
    expect(() => validateMemoryGround(request, raw, GROUND, FRAME)).not.toThrow();
    expect(() => validateMemoryGround({ ...request, config: { model: 'wrong', maxTokens: 8192 } }, raw, GROUND, FRAME)).toThrow('model');
    expect(() => validateMemoryGround({ ...request, system: 'wrong' }, raw, GROUND, FRAME)).toThrow('ground');
    expect(() => validateMemoryGround({ ...request, messages: [{ participant: 'Context Manager', content: [{ type: 'text', text: 'Unframed directive' }] }] }, raw, GROUND, FRAME)).toThrow('framing');
    expect(() => validateMemoryGround(request, { ...raw, messages: [{ role: 'user', content: 'Unframed provider directive' }] }, GROUND, FRAME)).toThrow('framing');

    expect(() => validateMemoryGround({ ...request, tools: [{ name: 'forbidden', description: 'fixture', inputSchema: { type: 'object' } }] }, raw, GROUND, FRAME)).toThrow('tools');

    const f = makeFixture(); const auxiliary = new FixtureAdapter();
    const membrane = new BackfillMembrane(auxiliary, { formatter: new OpenAIResponsesFormatter(), assistantParticipant: 'liv', retry: { maxRetries: 0 }, hooks: { beforeRequest: (normalized, providerRequest) => { validateMemoryGround(normalized, providerRequest as ProviderRequest, GROUND, FRAME); } } });
    const cm = await ContextManager.open({ path: join(f.root, 'auxiliary-store'), namespace: NAMESPACE, strategy: new AutobiographicalStrategy({ ...PROFILE, autoTickOnNewMessage: false, identityReminder: FRAME, logEffectiveConfig: false }), membrane });
    try {
      cm.setSystemPrompt(GROUND); cm.addMessage('user', [{ type: 'text', text: 'Disposable prior conversation fixture.' }]);
      await cm.resetHeadWindow(); expect(auxiliary.requests.length).toBe(1);
      const captured = auxiliary.wireRequests[0]; expect(captured.model).toBe(MODEL); expect(captured.instructions).toBe(GROUND); expect(canonical(captured.input)).toContain(FRAME); expect(canonical(captured.input).split(FRAME).length - 1).toBe(1); expect(captured.tools?.length ?? 0).toBe(0); expect(captured.normalizedMessages).toBeUndefined();
    } finally { cm.close(); auxiliary.dispose(); }
    expect(auxiliary.disposed).toBe(true);
    let disposed = 0; const adapter = new CodexSubscriptionAdapter({ authProvider: { getAccessToken: async () => { throw new Error('Auth must not be accessed'); }, dispose: () => { disposed++; } } }); expect(adapter.name).toBe('openai-codex'); expect(adapter.supportsModel(MODEL)).toBe(true); adapter.dispose(); expect(disposed).toBe(1);
  });
  test('A5: finite CLI prepare/ingest/dry-drain/status/verify on an explicit disposable candidate', () => {
    const f = makeFixture(); const cli = resolve(import.meta.dir, '../scripts/backfill-history.ts');
    const run = (...args: string[]): Record<string, unknown> => {
      const child = Bun.spawnSync(['bun', cli, ...args]); expect(child.exitCode).toBe(0);
      return JSON.parse(child.stdout.toString());
    };
    expect(run('prepare', '--manifest', f.manifest, '--recipe', f.recipe, '--framing', f.framing, '--out', f.out).prepared).toBe(true);
    expect(run('ingest', f.out, '--apply', '--max-events', '3').imported).toBe(3);
    const hashes = treeHashes(f.out); expect(run('status', f.out, '--json').readOnly).toBe(true); expect(run('verify', f.out, '--json').verified).toBe(true);
    expect(run('drain', f.out).dryRun).toBe(true); expect(treeHashes(f.out)).toEqual(hashes);
  });
  test('A1/B1/B5/A5: unreadable/unsupported sources audited, unexpected outputs refused, immutable earlier corpus rebuild and finite CLI commands', async () => {
    const f = makeFixture(); const manifest: SourceManifest = JSON.parse(readFileSync(f.manifest, 'utf8')); manifest.sources = [{ ...manifest.sources[0], path: join(f.root, 'unreadable-source') }, { ...manifest.sources[2], id: 'unsupported', path: join(f.root, 'unsupported.jsonl') }]; writeFileSync(manifest.sources[1].path, 'not-json\n'); writeFileSync(f.manifest, JSON.stringify(manifest));
    const result = await prepare(f); expect(result.quarantined).toBe(2); expect(result.noWork).toBe(false); expect(result.records).toBe(1);
    await expect(prepare(f)).rejects.toThrow('already exists');
    const symlink = join(f.root, 'symlink'); symlinkSync(f.out, symlink); await expect(prepare({ ...f, out: symlink })).rejects.toThrow('exists');
    const events = readFileSync(join(f.out, 'events.jsonl'), 'utf8'); writeFileSync(join(f.out, 'events.jsonl'), events + '{}\n'); await expect(ingest(f.out, { apply: true })).rejects.toThrow('new-candidate');
    const cli = resolve(import.meta.dir, '../scripts/backfill-history.ts');
    const help = Bun.spawnSync(['bun', cli, '--help']); expect(help.exitCode).toBe(0); expect(help.stdout.toString()).toContain('prepare'); expect(help.stdout.toString()).toContain('drain');
    const invalid = Bun.spawnSync(['bun', cli, 'ingest', '--apply']); expect(invalid.exitCode).toBe(1); expect(JSON.parse(invalid.stderr.toString())).toEqual({ error: 'Backfill operation failed; inspect private candidate state before acceptance', messageHash: sha256('Explicit private instance path is required'), failed: true });
  });
});

describe('filtered-exclusion fresh-candidate contract', () => {
  function decisionInput(f: Fixture, eventKeys: string[]) {
    const origin = JSON.parse(readFileSync(join(f.out, 'instance.json'), 'utf8'));
    const decision: ExclusionDecision = { version: 1, originFingerprint: origin.fingerprint, originSessionId: origin.sessionId, selectedSummaryIds: ['synthetic-review-label'], eventKeys, reason: 'Synthetic exact-event directive', logicalEventCount: eventKeys.length, wholeEventExclusionHasNoAdditionalNativeShards: true, newCandidateRequired: true, existingSummariesMustNotBeReusedWithoutCompleteDependencyEvidence: true, missingAcceptedPreimageCount: 0, reviewProseMustNotEnterLiveContext: true };
    const excludeEvents = join(f.root, 'fixture-decision.json'); writeFileSync(excludeEvents, JSON.stringify(decision));
    return { parent: f.out, excludeEvents, framing: f.framing, out: join(f.root, 'filtered'), decision };
  }
  function onlyHandoff(f: Fixture): void {
    const manifest: SourceManifest = JSON.parse(readFileSync(f.manifest, 'utf8'));
    manifest.sources = manifest.sources.filter(source => source.kind === 'handoff'); writeFileSync(f.manifest, JSON.stringify(manifest));
  }
  function rebind(out: string): void {
    const inventory = JSON.parse(readFileSync(join(out, 'instance.json'), 'utf8'));
    const files = { manifestHash: 'manifest.json', eventsHash: 'events.jsonl', dispositionsHash: 'dispositions.json', sourcesHash: 'sources.json', recipeHash: 'recipe.json', groundHash: 'ground.md', framingHash: 'framing.md', omissionsHash: 'omissions.json', derivationHash: 'derivation.json' };
    for (const [key, file] of Object.entries(files)) inventory[key] = sha256(readFileSync(join(out, file)));
    inventory.fingerprint = sha256(canonical({ ...Object.fromEntries(Object.keys(files).map(key => [key, inventory[key]])), profileHash: inventory.profileHash }));
    inventory.eventCount = readEvents(out).length; writeFileSync(join(out, 'instance.json'), JSON.stringify(inventory));
    const checkpoint = JSON.parse(readFileSync(join(out, 'checkpoint.json'), 'utf8')); checkpoint.fingerprint = inventory.fingerprint; writeFileSync(join(out, 'checkpoint.json'), JSON.stringify(checkpoint));
  }
  test('B1-B4/I1-I3: exact coalesced/sharded omissions, parent-only originals, retained media and witnessed empty native store', async () => {
    const f = makeFixture(); const claudePath = join(f.root, 'claude.jsonl');
    const raw = readFileSync(claudePath, 'utf8').trim().split('\n').map(line => JSON.parse(line));
    const excludedMedia = Buffer.concat([Buffer.from([137, 80, 78, 71, 13, 10, 26, 10]), Buffer.from('synthetic-excluded-media')]);
    raw.find(record => record.uuid === 'c2').message.content.push({ type: 'image', source: { media_type: 'image/png', data: excludedMedia.toString('base64') } });
    writeFileSync(claudePath, raw.map(canonical).join('\n') + '\n');
    await prepare(f); await ingest(f.out, { apply: true, maxEvents: 100 }); await drain(f.out, { apply: true, maxSteps: 100, adapterFactory: () => new FixtureAdapter() });
    const parentEvents = readEvents(f.out); const input = decisionInput(f, ['claude:stream-a', 'omp:o5']);
    const parentCheckpoint = JSON.parse(readFileSync(join(f.out, 'checkpoint.json'), 'utf8'));
    input.decision.rawSourceCount = input.decision.eventKeys.reduce((sum, key) => sum + parentCheckpoint.mapping[key].nativeIds.length, 0);
    expect(input.decision.rawSourceCount).toBeGreaterThan(2); writeFileSync(input.excludeEvents, JSON.stringify(input.decision));
    const originalHashes = Object.fromEntries(Object.values(JSON.parse(readFileSync(join(f.out, 'instance.json'), 'utf8')).sourcePaths as Record<string, string>).map(path => [path, sha256(readFileSync(path))]));
    const oldSummaries: SummaryReceipt[] = JSON.parse(readFileSync(join(f.out, 'inspection.json'), 'utf8')).summaries; expect(oldSummaries.length).toBeGreaterThan(0);
    const oldStore = JsStore.open({ path: new SessionManager(join(f.out, 'data')).getStorePath(String((await status(f.out)).sessionId)) });
    const oldAccepted = (oldStore.getStateJson(`${NAMESPACE}/autobio:summaries`) as Array<{ provenance: { requestHash: string } }>).map(summary => summary.provenance.requestHash); oldStore.close();
    const parentHashes = treeHashes(f.out);
    const forbidden = new Set([...Object.keys(originalHashes), ...Object.keys(parentHashes).filter(path => path.endsWith('.original')).map(path => f.out + path)]);
    const reader = fs.readFileSync; const reads: string[] = []; const hook = spyOn(fs, 'readFileSync').mockImplementation(((path: unknown, ...args: unknown[]) => {
      if (typeof path === 'string' && forbidden.has(path)) { reads.push(path); throw new Error('Synthetic raw-source parse forbidden'); }
      return (reader as (...args: unknown[]) => unknown)(path, ...args);
    }) as typeof fs.readFileSync);
    let report: Record<string, unknown>;
    try { report = await deriveFiltered(input); } finally { hook.mockRestore(); }
    expect(reads).toEqual([]); expect(treeHashes(f.out)).toEqual(parentHashes);
    for (const [path, hash] of Object.entries(originalHashes)) expect(sha256(readFileSync(path))).toBe(hash);
    const retained = parentEvents.filter(event => !input.decision.eventKeys.includes(event.key)); expect(readEvents(input.out)).toEqual(retained);
    expect(report!.retainedEligibleEvents).toBe(retained.length); expect(report!.excludedLogicalEvents).toBe(2); expect(report!.excludedDispositionRecords).toBe(3); expect(report!.nativeMessages).toBe(0); expect(report!.leaves).toBe(0); expect(report!.merges).toBe(0); expect(report!.l1Queue).toBe(0); expect(report!.mergeQueue).toBe(0);
    const checkpoint = JSON.parse(readFileSync(join(input.out, 'checkpoint.json'), 'utf8')); expect(checkpoint).toMatchObject({ nextEvent: 0, mapping: {}, batches: [] });
    const inventory = JSON.parse(readFileSync(join(input.out, 'instance.json'), 'utf8')); expect(inventory.sessionId).not.toBe(input.decision.originSessionId); expect(inventory.fingerprint).not.toBe(input.decision.originFingerprint); expect(inventory.profileHash).toBe(JSON.parse(readFileSync(join(f.out, 'instance.json'), 'utf8')).profileHash);
    expect(readFileSync(join(input.out, 'ground.md'), 'utf8')).toBe(GROUND); expect(readFileSync(join(input.out, 'framing.md'), 'utf8')).toBe(readFileSync(f.framing, 'utf8'));
    const omissions: OmissionReceipt = JSON.parse(readFileSync(join(input.out, 'omissions.json'), 'utf8')); const lineage: DerivationReceipt = JSON.parse(readFileSync(join(input.out, 'derivation.json'), 'utf8'));
    expect(omissions.decision).toEqual(input.decision); expect(omissions.events.find(event => event.key === 'claude:stream-a')!.recordIds).toEqual(['c2', 'c3']); expect(omissions.records.map(row => row.recordId)).toEqual(['c2', 'c3', 'o5']); expect(lineage.events.map(event => event.key)).toEqual(parentEvents.map(event => event.key)); expect(lineage).toMatchObject({ originals: 'parent-only', nativeState: 'fresh-empty', summaryReuse: false });
    const oldRows: Disposition[] = JSON.parse(readFileSync(join(f.out, 'dispositions.json'), 'utf8')); const rows: Disposition[] = JSON.parse(readFileSync(join(input.out, 'dispositions.json'), 'utf8'));
    rows.forEach((row, index) => { if (omissions.records.some(record => record.index === index)) { expect(row.state).toBe('exclude'); expect(row.reason).toBe(input.decision.reason); expect(row.exclusion!.previousState).toBe(oldRows[index].state); expect(row.blocks).toEqual(oldRows[index].blocks); } else expect(row).toEqual(oldRows[index]); });
    const sources: SourceSnapshot[] = JSON.parse(readFileSync(join(input.out, 'sources.json'), 'utf8')); expect(sources.every(source => !source.snapshot && !source.partialTailSnapshot && !source.inode && source.active === false && source.lineage?.originals === 'parent-only')).toBe(true); expect(sources.find(source => source.id === 'claude')!.decision).toBe('include');
    const snapshots = retained.flatMap(event => event.media).filter(media => media.status === 'validated'); expect(readdirSync(join(input.out, 'sources'))).toEqual(['media']); expect(readdirSync(join(input.out, 'sources/media')).sort()).toEqual([...new Set(snapshots.map(media => media.hash!))].sort()); expect(readdirSync(join(input.out, 'sources/media'))).not.toContain(sha256(excludedMedia));
    for (const media of snapshots) expect(sha256(readFileSync(join(input.out, media.snapshot!)))).toBe(media.hash!);
    const checkModes = (path: string): void => { const st = lstatSync(path); expect(st.mode & 0o777).toBe(st.isDirectory() ? 0o700 : 0o600); if (st.isDirectory()) for (const name of readdirSync(path)) checkModes(join(path, name)); }; checkModes(input.out);
    expect((await verify(input.out)).verified).toBe(true); const before = treeHashes(input.out); await status(input.out); await verify(input.out); expect(treeHashes(input.out)).toEqual(before);
    const store = JsStore.open({ path: new SessionManager(join(input.out, 'data')).getStorePath(inventory.sessionId) }); const cm = await ContextManager.open({ store, namespace: NAMESPACE, strategy: new AutobiographicalStrategy({ ...PROFILE, auditOnly: true, autoTickOnNewMessage: false, logEffectiveConfig: false }) });
    try { expect(store.stats().blobCount).toBe(0); expect(cm.getAllMessages()).toEqual([]); expect(cm.getSummariesInRange({})).toEqual([]); for (const hash of oldAccepted) expect(getMintRequestByHash(store, hash)).toBeNull(); } finally { cm.close(); store.close(); }
  }, 60000);
  test('B1/B2/B6/I1/I5: explicit origin/keys/audit schema, output symlinks, raw bypass and repeated derive all fail closed', async () => {
    const f = makeFixture(); onlyHandoff(f); await prepare(f); await ingest(f.out, { apply: true }); const input = decisionInput(f, ['handoff:38']);
    const invalid: unknown[] = [null, { ...input.decision, version: 2 }, { ...input.decision, originFingerprint: 'f'.repeat(64) }, { ...input.decision, originSessionId: 'ffffffff' }, { ...input.decision, selectedSummaryIds: [] }, { ...input.decision, selectedSummaryIds: ['same', 'same'] }, { ...input.decision, eventKeys: [] }, { ...input.decision, eventKeys: ['handoff:38', 'handoff:38'] }, { ...input.decision, eventKeys: ['handoff:unknown'] }, { ...input.decision, eventKeys: [' handoff:38'] }, { ...input.decision, reason: ' ' }, { ...input.decision, logicalEventCount: 2 }, { ...input.decision, rawSourceCount: 2 }, { ...input.decision, missingAcceptedPreimageCount: -1 }, { ...input.decision, newCandidateRequired: false }, { ...input.decision, reviewProseMustNotEnterLiveContext: 'true' }, { ...input.decision, eventBody: 'DISPOSABLE-UNSAFE-METADATA' }];
    for (const value of invalid) { writeFileSync(input.excludeEvents, JSON.stringify(value)); await expect(deriveFiltered(input)).rejects.toThrow(); expect(fs.existsSync(input.out)).toBe(false); }
    writeFileSync(input.excludeEvents, JSON.stringify(input.decision));
    for (const field of ['parent', 'excludeEvents', 'framing', 'out'] as const) await expect(deriveFiltered({ ...input, [field]: '' })).rejects.toThrow('Explicit');
    const link = join(f.root, 'dangling-output'); symlinkSync(join(f.root, 'absent'), link); await expect(deriveFiltered({ ...input, out: link })).rejects.toThrow('exists');
    const parentLink = join(f.root, 'parent-link'); symlinkSync(f.root, parentLink); await expect(deriveFiltered({ ...input, out: join(parentLink, 'new') })).rejects.toThrow('symlinks');
    await expect(deriveFiltered({ ...input, parent: parentLink })).rejects.toThrow(); await expect(deriveFiltered({ ...input, out: join(f.out, 'nested') })).rejects.toThrow('outside');
    const indexPath = join(f.out, 'data/sessions.json'); const indexBytes = readFileSync(indexPath); const index = JSON.parse(indexBytes.toString()); index.sessions[input.decision.originSessionId].id = 'invalid'; writeFileSync(indexPath, JSON.stringify(index)); await expect(deriveFiltered(input)).rejects.toThrow('origin native session'); writeFileSync(indexPath, indexBytes);
    await deriveFiltered(input); await expect(deriveFiltered(input)).rejects.toThrow('exists'); await expect(deriveFiltered({ ...input, parent: input.out, out: join(f.root, 'second') })).rejects.toThrow('Already filtered candidate'); expect(fs.existsSync(join(f.root, 'second'))).toBe(false);
    await expect(prepare({ manifest: join(input.out, 'manifest.json'), recipe: f.recipe, framing: f.framing, out: join(f.root, 'bypass') })).rejects.toThrow('cannot be raw-prepared'); expect(fs.existsSync(join(f.root, 'bypass'))).toBe(false);
  });
  test('B1/B7/I5: explicit derive CLI validates flags and masks unsafe parser exceptions without provider calls', async () => {
    const f = makeFixture(); onlyHandoff(f); await prepare(f); const input = decisionInput(f, ['handoff:38']); const cli = resolve(import.meta.dir, '../scripts/backfill-history.ts');
    const base = ['derive', input.parent, '--exclude-events', input.excludeEvents, '--framing', input.framing, '--out', input.out];
    for (const args of [['derive'], ['derive', input.parent], ['derive', input.parent, '--exclude-events', input.excludeEvents], [...base, '--apply'], [...base, '--recipe', f.recipe], [...base, '--max-events', '1'], [...base, '--json', '--json'], [...base, 'another-parent']]) { const child = Bun.spawnSync(['bun', cli, ...args]); expect(child.exitCode).toBe(1); expect(JSON.parse(child.stderr.toString()).failed).toBe(true); expect(fs.existsSync(input.out)).toBe(false); }
    writeFileSync(input.excludeEvents, '{"DISPOSABLE-UNSAFE-JSON-PROSE":'); const broken = Bun.spawnSync(['bun', cli, ...base]); expect(broken.exitCode).toBe(1); expect(broken.stderr.toString()).not.toContain('DISPOSABLE-UNSAFE-JSON-PROSE'); expect(JSON.parse(broken.stderr.toString()).messageHash).toMatch(/^[a-f0-9]{64}$/);
    writeFileSync(input.excludeEvents, JSON.stringify(input.decision)); const child = Bun.spawnSync(['bun', cli, ...base, '--json']); expect(child.exitCode).toBe(0); const report = JSON.parse(child.stdout.toString()); expect(report.derived).toBe(true); expect(report.nativeMessages).toBe(0); expect(report.excludedLogicalEvents).toBe(1); expect(report.audit.requests).toBe(0); expect(report.audit.responses).toBe(0); expect(canonical(report)).not.toContain('DISPOSABLE house bridge');
  });
  test('B5/I4: missing/changed/foreign policy, inconsistent coverage and events/checkpoint key reinsertion reject before dispatch', async () => {
    const f = makeFixture(); onlyHandoff(f); await prepare(f); const input = decisionInput(f, ['handoff:38']); await deriveFiltered(input);
    const files = ['instance.json', 'manifest.json', 'events.jsonl', 'dispositions.json', 'sources.json', 'omissions.json', 'derivation.json', 'checkpoint.json']; const original = Object.fromEntries(files.map(file => [file, readFileSync(join(input.out, file))]));
    const mutations: Array<() => void> = [
      () => rmSync(join(input.out, 'omissions.json')),
      () => rmSync(join(input.out, 'derivation.json')),
      () => { const receipt = JSON.parse(original['omissions.json'].toString()); receipt.decision.reason = 'changed policy'; writeFileSync(join(input.out, 'omissions.json'), JSON.stringify(receipt)); },
      () => { const receipt = JSON.parse(original['omissions.json'].toString()); receipt.decision.originSessionId = 'ffffffff'; writeFileSync(join(input.out, 'omissions.json'), JSON.stringify(receipt)); rebind(input.out); },
      () => { const receipt = JSON.parse(original['omissions.json'].toString()); receipt.records = []; writeFileSync(join(input.out, 'omissions.json'), JSON.stringify(receipt)); rebind(input.out); },
      () => { const rows = JSON.parse(original['dispositions.json'].toString()); rows.find((row: Disposition) => row.exclusion).state = 'include'; writeFileSync(join(input.out, 'dispositions.json'), JSON.stringify(rows)); rebind(input.out); },
      () => { const event = readEvents(f.out).find(event => event.key === 'handoff:38')!; appendFileSync(join(input.out, 'events.jsonl'), canonical(event) + '\n'); rebind(input.out); },
      () => { const event = readEvents(f.out).find(event => event.key === 'handoff:38')!; const checkpoint = JSON.parse(original['checkpoint.json'].toString()); checkpoint.mapping[event.key] = { payloadHash: event.payloadHash, nativeIds: ['foreign'], textHash: sha256(event.text), timestampMs: event.timestampMs, ranges: [{ id: 'foreign', start: 0, end: event.text.length }] }; writeFileSync(join(input.out, 'checkpoint.json'), JSON.stringify(checkpoint)); },
      () => { const manifest = JSON.parse(original['manifest.json'].toString()); delete manifest.derivation; writeFileSync(join(input.out, 'manifest.json'), JSON.stringify(manifest)); rebind(input.out); },
    ];
    for (const mutate of mutations) {
      for (const [file, bytes] of Object.entries(original)) writeFileSync(join(input.out, file), bytes); mutate();
      let adapters = 0; const adapter = new FixtureAdapter();
      await expect(status(input.out)).rejects.toThrow(); await expect(verify(input.out)).rejects.toThrow(); await expect(ingest(input.out, { apply: true })).rejects.toThrow(); await expect(drain(input.out, { apply: true, adapterFactory: () => { adapters++; return adapter; } })).rejects.toThrow(); expect(adapters).toBe(0); expect(adapter.requests).toEqual([]); adapter.dispose();
    }
    for (const [file, bytes] of Object.entries(original)) writeFileSync(join(input.out, file), bytes); expect((await verify(input.out)).verified).toBe(true);
  });
  test('B5/I3 F-OPT-PREFIX-001: retained native shards cannot hide excluded act text in a skipped prefix', async () => {
    const f = makeFixture(); onlyHandoff(f); await prepare(f); const input = decisionInput(f, ['handoff:38']); await deriveFiltered(input);
    const inventory = JSON.parse(readFileSync(join(input.out, 'instance.json'), 'utf8')); const retained = readEvents(input.out)[0]; const omitted = readEvents(f.out).find(event => event.key === 'handoff:38')!;
    const cm = await ContextManager.open({ path: new SessionManager(join(input.out, 'data')).getStorePath(inventory.sessionId), namespace: NAMESPACE });
    try { cm.addMessage(retained.participant, [{ type: 'text', text: omitted.text + retained.text }], { sourceId: retained.key, sourceRecordIds: retained.recordIds, backfillKey: retained.key, backfillFingerprint: inventory.fingerprint, backfillPayloadHash: retained.payloadHash, sourceCharStart: 0, sourceCharEnd: retained.text.length, sourceEnvelopeChars: omitted.text.length }, undefined, { timestampMs: retained.timestampMs }); cm.sync(); } finally { cm.close(); }
    await expect(ingest(input.out, { apply: true })).rejects.toThrow('envelope/content'); const adapter = new FixtureAdapter(); await expect(drain(input.out, { apply: true, adapterFactory: () => adapter })).rejects.toThrow('resumable'); expect(adapter.requests).toEqual([]); expect(adapter.disposed).toBe(true); expect((await verify(input.out)).verified).toBe(false);
  });
  test('B5/I2/I4 F-INTERLEAVED-SHARDS-002: per-key valid shards cannot cross another retained event in native order', async () => {
    const f = makeFixture(); onlyHandoff(f); const source = join(f.root, 'handoff.json'); const bridge = JSON.parse(readFileSync(source, 'utf8')); bridge.records[0].content = [{ type: 'text', text: 'Synthetic retained large act. '.repeat(15000) }]; writeFileSync(source, JSON.stringify(bridge)); await prepare(f);
    const input = decisionInput(f, ['handoff:40']); await deriveFiltered(input); await ingest(input.out, { apply: true }); const clean = JSON.parse(readFileSync(join(input.out, 'instance.json'), 'utf8'));
    let cm = await ContextManager.open({ path: new SessionManager(join(input.out, 'data')).getStorePath(clean.sessionId), namespace: NAMESPACE }); let rows: StoredMessage[];
    try { rows = structuredClone(cm.getAllMessages()); } finally { cm.close(); }
    const a = rows!.filter(row => row.metadata?.backfillKey === 'handoff:38'); const b = rows!.filter(row => row.metadata?.backfillKey === 'handoff:39'); const rest = rows!.filter(row => row.metadata?.backfillKey !== 'handoff:38' && row.metadata?.backfillKey !== 'handoff:39'); expect(a.length).toBeGreaterThan(1); expect(b.length).toBe(1);
    const out = join(f.root, 'interleaved'); await deriveFiltered({ ...input, out }); const inventory = JSON.parse(readFileSync(join(out, 'instance.json'), 'utf8')); cm = await ContextManager.open({ path: new SessionManager(join(out, 'data')).getStorePath(inventory.sessionId), namespace: NAMESPACE });
    try { for (const row of [a[0], ...b, ...a.slice(1), ...rest]) cm.addMessage(row.participant, row.content, { ...row.metadata, backfillFingerprint: inventory.fingerprint }, undefined, { timestampMs: row.timestamp.getTime() }); cm.sync(); } finally { cm.close(); }
    await expect(ingest(out, { apply: true })).rejects.toThrow('chronological shard order'); const adapter = new FixtureAdapter(); await expect(drain(out, { apply: true, adapterFactory: () => adapter })).rejects.toThrow('resumable'); expect(adapter.requests).toEqual([]); expect(adapter.disposed).toBe(true);
  });
  test('B3/I2 F-ZERO-MEDIA-003: zero-byte validated non-image media keeps the existing normalization convention', async () => {
    const f = makeFixture(); onlyHandoff(f); const source = join(f.root, 'handoff.json'); const bridge = JSON.parse(readFileSync(source, 'utf8')); bridge.records[1].content.push({ type: 'audio', source: { media_type: 'audio/mpeg', data: '' } }, { type: 'document', source: { media_type: 'application/pdf', data: '' } }); writeFileSync(source, JSON.stringify(bridge)); await prepare(f);
    const original = readEvents(f.out); const media = original.flatMap(event => event.media); expect(media.length).toBe(2); expect(media.every(reference => reference.status === 'validated' && reference.bytes === 0)).toBe(true);
    const input = decisionInput(f, ['handoff:38']); await deriveFiltered(input); expect(readEvents(input.out)).toEqual(original.filter(event => event.key !== 'handoff:38')); expect(readdirSync(join(input.out, 'sources/media'))).toEqual([sha256(Buffer.alloc(0))]); expect(readFileSync(join(input.out, media[0].snapshot!)).length).toBe(0); expect((await verify(input.out)).verified).toBe(true);
  });
  test('B5/B7/I4: empty derived candidate cannot verify from a missing or foreign closed native receipt', async () => {
    const f = makeFixture(); onlyHandoff(f); await prepare(f); const input = decisionInput(f, ['handoff:38']); await deriveFiltered(input);
    const path = join(input.out, 'inspection.json'); const bytes = readFileSync(path); rmSync(path);
    expect((await status(input.out)).noWork).toBe(false); const missing = await verify(input.out); expect(missing.verified).toBe(false); expect(missing.recoveryRequired).toBe(true); expect(missing.issues).toContain('missing-native-inspection');
    const receipt = JSON.parse(bytes.toString()); receipt.fingerprint = 'f'.repeat(64); writeFileSync(path, JSON.stringify(receipt)); const foreign = await verify(input.out); expect(foreign.verified).toBe(false); expect(foreign.recoveryRequired).toBe(true); expect(foreign.issues).toContain('native-inspection-fingerprint');
    writeFileSync(path, bytes); expect((await verify(input.out)).verified).toBe(true);
  });
  for (const point of ['append-before-checkpoint', 'checkpoint-before-inspection'] as const) test(`B5/I2/I4: SIGKILL ${point} imports the retained chronological shards exactly once`, async () => {
    const f = makeFixture(); await prepare(f); const input = decisionInput(f, ['note:note', 'claude:stream-a']); await deriveFiltered(input);
    const events = readEvents(input.out); const helper = new URL('../scripts/lib/backfill-instance.ts', import.meta.url).href;
    const body = point === 'append-before-checkpoint' ? `let n=0; await ingest(${JSON.stringify(input.out)}, {apply:true,maxEvents:100,interrupt:(p,cm)=>{if(p==='append-before-checkpoint' && cm.getAllMessages().at(-1)?.metadata?.backfillKey==='omp:o5' && ++n===2)process.kill(process.pid,'SIGKILL')}});` : `await ingest(${JSON.stringify(input.out)}, {apply:true,maxEvents:100,interrupt:(p)=>{if(p==='checkpoint-before-inspection')process.kill(process.pid,'SIGKILL')}});`;
    const child = spawn('bun', ['-e', `import {ingest} from ${JSON.stringify(helper)}; ${body}`], { stdio: 'ignore' }); await new Promise<void>(done => child.once('exit', () => done())); expect(child.signalCode).toBe('SIGKILL');
    const crashed = await status(input.out); expect(crashed.noWork).toBe(false); expect(crashed.recoveryRequired).toBe(true); expect((await verify(input.out)).verified).toBe(false);
    const nativePath = new SessionManager(join(input.out, 'data')).getStorePath(String(crashed.sessionId)); let cm = await ContextManager.open({ path: nativePath, namespace: NAMESPACE }); let priorIds: string[];
    try { priorIds = cm.getAllMessages().map(message => message.id); } finally { cm.close(); }
    if (point === 'append-before-checkpoint') { const adapter = new FixtureAdapter(); await expect(drain(input.out, { apply: true, maxSteps: 1, adapterFactory: () => adapter })).rejects.toThrow('resumable'); expect(adapter.requests.length).toBe(0); expect(adapter.disposed).toBe(true); }
    const resumed = await ingest(input.out, { apply: true, maxEvents: 100 }); expect(resumed.imported).toBe(events.length); expect(resumed.pending).toBe(0); expect(resumed.excludedLogicalEvents).toBe(2); expect(resumed.excludedDispositionRecords).toBe(3); expect(resumed.nativeMessages).toBeGreaterThan(events.length);
    const checkpoint = JSON.parse(readFileSync(join(input.out, 'checkpoint.json'), 'utf8')); expect(Object.keys(checkpoint.mapping)).toEqual(events.map(event => event.key));
    const ids: string[] = []; for (const entry of Object.values(checkpoint.mapping)) { if (!entry || typeof entry !== 'object' || !('nativeIds' in entry) || !Array.isArray(entry.nativeIds)) throw new Error('Invalid synthetic mapping'); ids.push(...entry.nativeIds); } expect(new Set(ids).size).toBe(ids.length); expect(ids.slice(0, priorIds!.length)).toEqual(priorIds!);
    const noOp = await ingest(input.out, { apply: true }); expect(noOp.nativeMessages).toBe(resumed.nativeMessages); expect((await verify(input.out)).verified).toBe(true);
    cm = await ContextManager.open({ path: nativePath, namespace: NAMESPACE });
    try { for (const event of events) { const parts = cm.getAllMessages().filter(message => message.metadata?.backfillKey === event.key); expect(parts.every(message => message.timestamp.getTime() === event.timestampMs)).toBe(true); expect(parts.map(message => message.content.filter(block => block.type === 'text').map(block => block.text).join('').slice(Number(message.metadata?.sourceEnvelopeChars))).join('')).toBe(event.text); } expect(cm.getAllMessages().map(message => message.id)).toEqual(ids); expect(cm.getAllMessages().every(message => !input.decision.eventKeys.includes(String(message.metadata?.backfillKey)))).toBe(true); } finally { cm.close(); }
  }, 60000);
  test('B5/I3/I4: excluded native row injection is rejected before any synthetic memory request', async () => {
    const f = makeFixture(); onlyHandoff(f); await prepare(f); const input = decisionInput(f, ['handoff:38']); await deriveFiltered(input); await ingest(input.out, { apply: true });
    const inventory = JSON.parse(readFileSync(join(input.out, 'instance.json'), 'utf8')); const event = readEvents(f.out).find(event => event.key === 'handoff:38')!;
    const cm = await ContextManager.open({ path: new SessionManager(join(input.out, 'data')).getStorePath(inventory.sessionId), namespace: NAMESPACE });
    try { cm.addMessage(event.participant, [{ type: 'text', text: event.text }], { backfillKey: event.key, backfillFingerprint: inventory.fingerprint, backfillPayloadHash: event.payloadHash, sourceCharStart: 0, sourceCharEnd: event.text.length, sourceEnvelopeChars: 0, sourceRecordIds: event.recordIds }, undefined, { timestampMs: event.timestampMs }); cm.sync(); } finally { cm.close(); }
    expect((await status(input.out)).noWork).toBe(false); expect((await verify(input.out)).verified).toBe(false); await expect(ingest(input.out, { apply: true })).rejects.toThrow('conflicts'); const adapter = new FixtureAdapter(); await expect(drain(input.out, { apply: true, adapterFactory: () => adapter })).rejects.toThrow('resumable'); expect(adapter.requests).toEqual([]); expect(adapter.disposed).toBe(true);
  });
  for (const corrupted of ['sourceId', 'sourceRecordIds'] as const) test(`B5/I3: native ${corrupted} cannot relabel retained payload as excluded source lineage`, async () => {
    const f = makeFixture(); onlyHandoff(f); await prepare(f); const input = decisionInput(f, ['handoff:38']); await deriveFiltered(input);
    const inventory = JSON.parse(readFileSync(join(input.out, 'instance.json'), 'utf8')); const event = readEvents(input.out)[0];
    const cm = await ContextManager.open({ path: new SessionManager(join(input.out, 'data')).getStorePath(inventory.sessionId), namespace: NAMESPACE });
    try { const metadata = { sourceId: event.key, sourceRecordIds: event.recordIds, backfillKey: event.key, backfillFingerprint: inventory.fingerprint, backfillPayloadHash: event.payloadHash, sourceCharStart: 0, sourceCharEnd: event.text.length, sourceEnvelopeChars: 0 }; if (corrupted === 'sourceId') metadata.sourceId = 'handoff:38'; else metadata.sourceRecordIds = ['38']; cm.addMessage(event.participant, [{ type: 'text', text: event.text }], metadata, undefined, { timestampMs: event.timestampMs }); cm.sync(); } finally { cm.close(); }
    await expect(ingest(input.out, { apply: true })).rejects.toThrow('source-record lineage'); const adapter = new FixtureAdapter(); await expect(drain(input.out, { apply: true, adapterFactory: () => adapter })).rejects.toThrow('resumable'); expect(adapter.requests).toEqual([]); expect(adapter.disposed).toBe(true);
  });
  test('B3/B5/I4: retained media path/hash tampering fails closed before memory dispatch', async () => {
    const f = makeFixture(); const manifest: SourceManifest = JSON.parse(readFileSync(f.manifest, 'utf8')); manifest.sources = manifest.sources.filter(source => source.id === 'claude'); writeFileSync(f.manifest, JSON.stringify(manifest)); await prepare(f);
    const input = decisionInput(f, ['claude:stream-a']); await deriveFiltered(input); const media = readEvents(input.out).flatMap(event => event.media).find(media => media.status === 'validated')!; const path = join(input.out, media.snapshot!); const bytes = readFileSync(path);
    for (const symlink of [false, true]) { rmSync(path); if (symlink) symlinkSync(join(f.out, media.snapshot!), path); else writeFileSync(path, Buffer.alloc(bytes.length)); const adapter = new FixtureAdapter(); let created = 0; await expect(status(input.out)).rejects.toThrow(); await expect(verify(input.out)).rejects.toThrow(); await expect(ingest(input.out, { apply: true })).rejects.toThrow(); await expect(drain(input.out, { apply: true, adapterFactory: () => { created++; return adapter; } })).rejects.toThrow(); expect(created).toBe(0); expect(adapter.requests).toEqual([]); adapter.dispose(); }
    rmSync(path); writeFileSync(path, bytes, { mode: 0o600 }); expect((await verify(input.out)).verified).toBe(true);
  });
  test('B5/I3: a fresh accepted summary without its own new preimage cannot influence later authoring', async () => {
    const f = makeFixture(); onlyHandoff(f); await prepare(f); const input = decisionInput(f, ['handoff:38']); await deriveFiltered(input); await ingest(input.out, { apply: true }); await drain(input.out, { apply: true, adapterFactory: () => new FixtureAdapter() });
    const inventory = JSON.parse(readFileSync(join(input.out, 'instance.json'), 'utf8')); const cm = await ContextManager.open({ path: new SessionManager(join(input.out, 'data')).getStorePath(inventory.sessionId), namespace: NAMESPACE });
    try { const summaries = cm.getStore().getStateJson(`${NAMESPACE}/autobio:summaries`) as Array<{ provenance: { requestHash: string } }>; expect(summaries.length).toBeGreaterThan(0); summaries[0].provenance.requestHash = 'f'.repeat(64); cm.getStore().setStateJson(`${NAMESPACE}/autobio:summaries`, summaries); cm.sync(); } finally { cm.close(); }
    await expect(ingest(input.out, { apply: true })).rejects.toThrow('memory lineage'); const adapter = new FixtureAdapter(); await expect(drain(input.out, { apply: true, adapterFactory: () => adapter })).rejects.toThrow('resumable'); expect(adapter.requests).toEqual([]); expect(adapter.disposed).toBe(true);
  });
  test('B3/B5/B7/I3/I5: fresh memories/preimages and public retrieval/search/context/semantic namespaces exclude selected fixture acts', async () => {
    const f = makeFixture(); onlyHandoff(f); const marker = 'SYNTHETIC-EXCLUDED-ACT-UNIQUE'; const source = join(f.root, 'handoff.json'); const bridge = JSON.parse(readFileSync(source, 'utf8')); bridge.records[0].content = [{ type: 'text', text: marker }]; writeFileSync(source, JSON.stringify(bridge));
    const recipe = JSON.parse(readFileSync(f.recipe, 'utf8')); recipe.modules.history = { semantic: { url: 'http://127.0.0.1:1', namespace: 'fixture-shared-prefix', syncIntervalMs: 0 } }; writeFileSync(f.recipe, JSON.stringify(recipe));
    const fetches: string[] = []; const indexes = new Map<string, Map<string, { id: string; text: string; kind: string; level?: number }>>();
    const fetchHook = spyOn(globalThis, 'fetch').mockImplementation((async (input: string | URL | Request, init?: RequestInit) => {
      const url = new URL(String(input)); fetches.push(url.pathname); const match = /^\/v1\/index\/([^/]+)\/(stats|upsert|search)$/.exec(url.pathname); if (!match) throw new Error('Synthetic semantic service only'); const ns = decodeURIComponent(match[1]);
      if (match[2] === 'stats') return new Response(JSON.stringify({ error: { message: 'synthetic namespace absent' } }), { status: 404 });
      const body = JSON.parse(String(init?.body)); const index = indexes.get(ns) ?? new Map(); indexes.set(ns, index);
      if (match[2] === 'upsert') { for (const item of body.items) index.set(item.id, item); return Response.json({ inserted: body.items.length, updated: 0, unchanged: 0, count: index.size }); }
      const hits = [...index.values()].filter(item => item.text.includes(body.query)).map(item => ({ ...item, score: 1, ts: null, channel: null, meta: {}, chars: item.text.length, level: item.level ?? null })); return Response.json({ namespace: ns, hits, count_indexed: index.size, timing_ms: { embed: 0, search: 0 } });
    }) as typeof fetch);
    let history: HistoryModule | undefined; let cm: ContextManager | undefined;
    try {
      await prepare(f); const input = decisionInput(f, ['handoff:38']); await deriveFiltered(input); await ingest(input.out, { apply: true }); await status(input.out); await verify(input.out); expect(fetches).toEqual([]);
      const adapter = new FixtureAdapter(); const result = await drain(input.out, { apply: true, maxSteps: 100, adapterFactory: () => adapter }); expect(result.noWork).toBe(true); expect(adapter.requests.length).toBeGreaterThan(0); expect(canonical(adapter.wireRequests)).not.toContain(marker); expect(adapter.requests.every(request => request.tools?.length === 0 || request.tools === undefined)).toBe(true); expect(adapter.disposed).toBe(true); expect(fetches).toEqual([]);
      const inventory = JSON.parse(readFileSync(join(input.out, 'instance.json'), 'utf8')); const generated = JSON.parse(readFileSync(join(input.out, 'recipe.json'), 'utf8'));
      const options = historyModuleOptions(generated.modules.history, generated.agent.name, inventory.sessionId); const oldOptions = historyModuleOptions(recipe.modules.history, recipe.agent.name, input.decision.originSessionId); expect(options.semantic!.namespace).not.toBe(oldOptions.semantic!.namespace); expect(options.semantic!.namespace).toBe(`fixture-shared-prefix/${inventory.sessionId}`); expect(historyModuleOptions(true, 'liv', inventory.sessionId)).toEqual({});
      cm = await ContextManager.open({ path: new SessionManager(join(input.out, 'data')).getStorePath(inventory.sessionId), namespace: NAMESPACE, strategy: new AutobiographicalStrategy({ ...PROFILE, autoTickOnNewMessage: false, identityReminder: FRAME, logEffectiveConfig: false }) });
      const messages = cm.getAllMessages(); const nativeIds = new Set(messages.map(message => message.id)); expect(canonical(messages)).not.toContain(marker); expect(cm.queryMessages({ metadata: { backfillKey: 'handoff:38' } }).messages).toEqual([]); expect(cm.getMessageWindow(0, 100).messages.map(message => message.id)).toEqual(messages.map(message => message.id));
      const summaries = cm.getSummariesInRange({}); expect(summaries.length).toBeGreaterThan(0); assertNativeMembership(cm);
      for (const overview of summaries) { const summary = cm.getSummary(overview.id)!; expect(summary.sourceIds.every(id => nativeIds.has(id))).toBe(true); const accepted = getMintRequestByHash(cm.getStore(), summary.provenance!.requestHash)!; expect(accepted).not.toBeNull(); expect(sha256(JSON.stringify(accepted))).toBe(summary.provenance!.requestHash); expect(canonical(accepted)).not.toContain(marker); }
      expect(cm.searchSummaries({ text: marker })).toEqual([]); expect(canonical(await cm.compile())).not.toContain(marker);
      indexes.set(oldOptions.semantic!.namespace, new Map([[`sum:${summaries[0].id}`, { id: `sum:${summaries[0].id}`, text: marker, kind: 'summary', level: 1 }]]));
      history = new HistoryModule(options); history.bind(cm);
      for (const name of ['extract', 'search', 'overview', 'semantic_search']) { const response = await history.handleToolCall({ id: `fixture-${name}`, name, input: name.includes('search') ? { query: marker, kinds: 'summaries' } : { limit: 100 } }); expect(response.success).toBe(true); expect(canonical(response)).not.toContain(marker); if (name === 'semantic_search') { const data = response.data; if (!data || typeof data !== 'object' || !('hits' in data)) throw new Error('Invalid synthetic semantic result'); expect(data.hits).toEqual([]); } }
      expect(fetches.length).toBeGreaterThan(0); expect(fetches.every(path => !path.includes(encodeURIComponent(oldOptions.semantic!.namespace)))).toBe(true); expect(indexes.get(oldOptions.semantic!.namespace)!.get(`sum:${summaries[0].id}`)!.text).toBe(marker);
    } finally { await history?.stop(); cm?.close(); fetchHook.mockRestore(); }
  }, 60000);
});
