import { afterEach, describe, expect, test } from 'bun:test';
import { Membrane, MembraneError, NativeFormatter, OpenAIResponsesFormatter, OpenAIResponsesAPIAdapter, type ProviderRequest, type NormalizedRequest, type NormalizedResponse } from '@animalabs/membrane';
import { ArchivalMemoryBudgetError, enforceArchivalMemoryBudget, readArchivalMemoryProvenance, validateArchivalServedModel, type ArchivalMemoryProvenance } from '../src/archival-memory-budget.js';
import { CodexSubscriptionAdapter } from '../src/codex-subscription-adapter.js';
import { ARCHIVAL_MEMORY_LOCAL_CAP_CODE, ArchivalCyberPolicyFallbackHalt, getMintRequestByHash, ContextManager, AutobiographicalStrategy } from '@animalabs/context-manager';
import { createHash } from 'node:crypto';
import { BackfillMembrane, PROFILE, NAMESPACE, validateMemoryGround } from '../scripts/lib/backfill-instance.js';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

const originalFetch = globalThis.fetch;
const originalBaseURL = process.env.CODEX_BASE_URL;
afterEach(() => {
  globalThis.fetch = originalFetch;
  if (originalBaseURL === undefined) delete process.env.CODEX_BASE_URL;
  else process.env.CODEX_BASE_URL = originalBaseURL;
});
const request: ProviderRequest = { model: 'gpt-5.4', messages: [], maxTokens: 100 };
const completed = (model?: string) => new Response(`data: ${JSON.stringify({ type: 'response.completed', response: { status: 'completed', ...(model ? { model } : {}), output: [] } })}\n\n`);

describe('Codex host integration', () => {
  test('bridges token refresh and the refreshed account ID to Membrane', async () => {
    const flags: boolean[] = [];
    const headers: Headers[] = [];
    let account = 'old-account';
    globalThis.fetch = async (_url, init) => {
      headers.push(new Headers(init?.headers));
      return headers.length === 1 ? new Response('expired', { status: 401 }) : completed();
    };
    const adapter = new CodexSubscriptionAdapter({ authProvider: {
      getAccessToken: async (forceRefresh = false) => {
        flags.push(forceRefresh);
        if (forceRefresh) account = 'new-account';
        return forceRefresh ? 'fresh' : 'expired';
      },
      getAccountId: () => account,
    } });
    expect(adapter).toBeInstanceOf(OpenAIResponsesAPIAdapter);
    expect(adapter.usageCacheConvention).toBe('cache-inclusive');
    await adapter.complete(request);
    expect(flags).toEqual([false, true]);
    expect(headers[1]?.get('authorization')).toBe('Bearer fresh');
    expect(headers[1]?.get('chatgpt-account-id')).toBe('new-account');
  });

  test('preserves CODEX_BASE_URL, Fast controls and auth disposal', async () => {
    process.env.CODEX_BASE_URL = 'https://example.test/codex/';
    const calls: Array<{ url: string; body: any }> = [];
    let disposed = false;
    globalThis.fetch = async (url, init) => {
      calls.push({ url: String(url), body: JSON.parse(String(init?.body)) });
      return completed();
    };
    const adapter = new CodexSubscriptionAdapter({ fastMode: true, authProvider: {
      getAccessToken: async () => 'token', dispose: () => { disposed = true; },
    } });
    expect(adapter.isFastMode()).toBe(true);
    await adapter.complete(request);
    adapter.setFastMode(false);
    await adapter.complete(request);
    expect(calls[0]?.url).toBe('https://example.test/codex/responses');
    expect(calls[0]?.body.service_tier).toBe('priority');
    expect(calls[1]?.body.service_tier).toBeUndefined();
    adapter.dispose();
    expect(disposed).toBe(true);
  });

  test('passes explicit endpoint configuration through the host wrapper', async () => {
    process.env.CODEX_BASE_URL = 'https://unused.test';
    let endpoint: string | undefined;
    globalThis.fetch = async url => { endpoint = String(url); return completed(); };
    await new CodexSubscriptionAdapter({ baseURL: 'https://explicit.test', authProvider: { getAccessToken: async () => 'token' } }).complete(request);
    expect(endpoint).toBe('https://explicit.test/responses');
  });
});

for (const mode of ['subscription', 'api'] as const) {
  for (const lane of ['complete', 'stream'] as const) {
    test(`logging wrapper preserves disjoint Membrane usage (${mode}/${lane})`, async () => {
      const { Membrane, OpenAIResponsesFormatter } = await import('@animalabs/membrane');
      const { LoggingProviderAdapter } = await import('../src/logging-provider-wrapper.js');
      const { mkdtempSync, readFileSync, rmSync } = await import('node:fs');
      const { tmpdir } = await import('node:os');
      const dir = mkdtempSync(`${tmpdir()}/codex-usage-`);
      try {
        const data = { status: 'completed', model: 'gpt-5.4', output: [
          { type: 'message', role: 'assistant', content: [{ type: 'output_text', text: 'hello' }] },
        ], usage: { input_tokens: 100, output_tokens: 2, input_tokens_details: { cached_tokens: 80 } } };
        globalThis.fetch = async (_url, init) => JSON.parse(String(init?.body)).stream
          ? new Response(`data: ${JSON.stringify({ type: 'response.completed', response: data })}\n\n`)
          : new Response(JSON.stringify(data));
        const adapter = mode === 'subscription'
          ? new CodexSubscriptionAdapter({ authProvider: { getAccessToken: async () => 'token' } })
          : new OpenAIResponsesAPIAdapter({ apiKey: 'sk-fixture' });
        const wrapped = new LoggingProviderAdapter(adapter, `${dir}/calls.jsonl`);
        const membrane = new Membrane(wrapped, { formatter: new OpenAIResponsesFormatter() });
        const normalized = { messages: [{ participant: 'user', content: [{ type: 'text' as const, text: 'hello' }] }], config: { model: 'gpt-5.4', maxTokens: 100 } };
        const response = lane === 'complete' ? await membrane.complete(normalized) : await membrane.stream(normalized, { onChunk: () => {} });
        // 2026-07-31 incident: adding cached tokens twice ratcheted calibration until the agent wedged.
        expect(response.usage.inputTokens).toBe(20);
        expect(response.usage.cacheReadTokens).toBe(80);
        const log = JSON.parse(readFileSync(`${dir}/calls.jsonl`, 'utf8').trim());
        expect(log.response.usage.inputTokens).toBe(100);
        expect(log.response.usage.cacheConvention).toBe('cache-inclusive');
        expect(log.provider).toBe(mode === 'subscription' ? 'openai-codex' : 'openai-responses-api');
      } finally { rmSync(dir, { recursive: true, force: true }); }
    });
  }
}

test('wrapped subscription maintenance calls keep participant attribution', async () => {
  const { Membrane, OpenAIResponsesFormatter, NativeFormatter } = await import('@animalabs/membrane');
  const { LoggingProviderAdapter } = await import('../src/logging-provider-wrapper.js');
  let input: unknown;
  globalThis.fetch = async (_url, init) => { input = JSON.parse(String(init?.body)).input; return completed(); };
  const adapter = new LoggingProviderAdapter(new CodexSubscriptionAdapter({ authProvider: { getAccessToken: async () => 't' } }), '/dev/null');
  const membrane = new Membrane(adapter, { formatter: new OpenAIResponsesFormatter() });
  await membrane.complete({ messages: [
    { participant: 'Alice', content: [{ type: 'text', text: 'first' }] },
    { participant: 'Bob', content: [{ type: 'text', text: 'second' }] },
  ], config: { model: 'gpt-5.4', maxTokens: 100 } }, { formatter: new NativeFormatter({ participantMode: 'multiuser' }) });
  expect(JSON.stringify(input)).toContain('Alice: first');
  expect(JSON.stringify(input)).toContain('Bob: second');
  expect(adapter.requiresNativeResponsesInput).toBe(false);
});

const archivalGround = 'Disposable full carrier ground, not Liv prose.';
const archivalFrame = 'Disposable parent memory-only framing.\n';
function archivalRequest(operation: ArchivalMemoryProvenance['operation'], text = 'Historical observation.', patch: Record<string, unknown> = {}): NormalizedRequest {
  return {
    system: archivalGround,
    config: { model: 'gpt-6.1-sol', maxTokens: 8192 },
    messages: [
      { participant: 'Historical source', content: [{ type: 'text', text }] },
      { participant: 'Context Manager', content: [{ type: 'text', text: `Write the ${operation} memory.\n${archivalFrame}` }], metadata: { archivalMemory: {
        version: 1, operation, model: 'gpt-6.1-sol', inputBudgetTokens: 600512,
        maxOutputTokens: 8192, outputReserve: 8192, nativeEstimatedPromptTokens: 123,
        ...patch,
      } } },
    ],
  };
}
for (const lane of ['complete', 'stream'] as const) {
  for (const operation of ['l1', 'merge', 'transition'] as const) {
    test(`archival final-body admission accepts actual native formatter (${lane}/${operation})`, async () => {
      let auth = 0; const bodies: Record<string, unknown>[] = []; const observed: unknown[] = [];
      globalThis.fetch = async (_url, init) => { const body = JSON.parse(String(init?.body)); bodies.push(body); return completed(body.model); };
      const adapter = new CodexSubscriptionAdapter({ authProvider: { getAccessToken: async () => { auth++; return 'fixture-token'; } } });
      const membrane = new Membrane(adapter, { formatter: new OpenAIResponsesFormatter(), retry: { maxRetries: 0 } });
      const normalized = archivalRequest(operation);
      const options = { formatter: new NativeFormatter({ participantMode: 'multiuser' }), onRequest: (body: unknown) => { observed.push(structuredClone(body)); } };
      try {
        if (lane === 'complete') await membrane.complete(normalized, options);
        else await membrane.stream(normalized, { ...options, onChunk: () => {} });
        expect(auth).toBe(1); expect(bodies.length).toBe(1); expect(observed).toEqual(bodies);
        const body = bodies[0]; expect(body.model).toBe('gpt-6.1-sol'); expect(body.instructions).toBe(archivalGround); expect(body.stream).toBe(true);
        expect(JSON.stringify(body.input)).toContain('Historical source: Historical observation.'); expect(body.tools).toBeUndefined();
        expect(body.normalizedMessages).toBeUndefined(); expect(JSON.stringify(body)).not.toContain('archivalMemory');
        const provenance = normalized.messages.at(-1)!.metadata!.archivalMemory as ArchivalMemoryProvenance;
        const accounting = enforceArchivalMemoryBudget(body, provenance);
        expect(accounting.serializedBodyUtf8Bytes).toBe(Buffer.byteLength(JSON.stringify(body), 'utf8'));
        expect(accounting.nativeEstimatedPromptTokens).toBe(123); expect(accounting.outputReserve).toBe(8192);
        expect(accounting.promptTokenUpperBound).toBeGreaterThan(accounting.serializedBodyUtf8Bytes);
        expect(accounting.totalTokenUpperBound).toBe(accounting.promptTokenUpperBound + 8192);
        expect(accounting.totalTokenUpperBound).toBeLessThanOrEqual(604608);
        expect(body.max_output_tokens).toBeUndefined(); // Reserve is requested, not a fictional subscription wire limit.
      } finally { adapter.dispose(); }
    });
  }
  test(`archival over-cap and invalid provenance refuse before auth/network (${lane})`, async () => {
    let auth = 0; let network = 0;
    globalThis.fetch = async () => { network++; return completed('gpt-6.1-sol'); };
    const adapter = new CodexSubscriptionAdapter({ authProvider: { getAccessToken: async () => { auth++; return 'fixture-token'; } } });
    const membrane = new Membrane(adapter, { formatter: new OpenAIResponsesFormatter(), retry: { maxRetries: 0 } });
    const cases = [
      archivalRequest('l1', 'x'.repeat(605000)),
      archivalRequest('merge', 'Historical observation.', { inputBudgetTokens: Number.NaN }),
      archivalRequest('l1', 'Historical observation.', { inputBudgetTokens: 0 }),
      archivalRequest('transition', 'Historical observation.', { outputReserve: 8193 }),
      archivalRequest('merge', 'Historical observation.', { maxOutputTokens: 0 }),
      archivalRequest('transition', 'Historical observation.', { nativeEstimatedPromptTokens: Infinity }),
      archivalRequest('l1', 'Historical observation.', { operation: 'primary' }),
      archivalRequest('merge', 'Historical observation.', { model: 'other-model' }),
      archivalRequest('transition', 'Historical observation.', { version: 2 }),
    ];
    const wrongOwner = archivalRequest('l1'); wrongOwner.messages.at(-1)!.participant = 'Historical source'; cases.push(wrongOwner);
    const wrongPosition = archivalRequest('merge'); wrongPosition.messages.reverse(); cases.push(wrongPosition);
    const duplicate = archivalRequest('l1'); duplicate.messages[0].metadata = structuredClone(duplicate.messages[1].metadata); cases.push(duplicate);
    try {
      for (const normalized of cases) {
        const invoke = () => lane === 'complete'
          ? membrane.complete(normalized, { formatter: new NativeFormatter({ participantMode: 'multiuser' }) })
          : membrane.stream(normalized, { onChunk: () => {}, formatter: new NativeFormatter({ participantMode: 'multiuser' }) });
        await expect(invoke()).rejects.toThrow();
        expect(auth).toBe(0); expect(network).toBe(0);
      }
      const native = cases[0].messages.at(-1)!.metadata!.archivalMemory as ArchivalMemoryProvenance;
      try { enforceArchivalMemoryBudget({ model: 'gpt-6.1-sol', instructions: archivalGround, input: ['x'.repeat(605000)] }, native); throw new Error('Expected cap refusal'); }
      catch (error) { expect(error).toBeInstanceOf(ArchivalMemoryBudgetError); expect((error as ArchivalMemoryBudgetError).accounting!.promptTokenUpperBound).toBeGreaterThan(600512); }
    } finally { adapter.dispose(); }
  });
  test(`archival caller onRequest failures and late body growth propagate before auth/network (${lane})`, async () => {
    let auth = 0; let network = 0;
    globalThis.fetch = async () => { network++; return completed('gpt-6.1-sol'); };
    const adapter = new CodexSubscriptionAdapter({ authProvider: { getAccessToken: async () => { auth++; return 'fixture-token'; } } });
    const membrane = new Membrane(adapter, { formatter: new OpenAIResponsesFormatter(), retry: { maxRetries: 0 } });
    try {
      for (const onRequest of [
        () => { throw new Error('Disposable caller refused dispatch'); },
        (body: unknown) => { if (!body || typeof body !== 'object') throw new Error('Expected provider body'); Object.assign(body, { input: ['x'.repeat(605000)] }); },
        (body: unknown) => { if (!body || typeof body !== 'object') throw new Error('Expected provider body'); Object.assign(body, { metadata: { archivalMemory: { nativeEstimatedPromptTokens: 1 } } }); },
        (body: unknown) => { if (!body || typeof body !== 'object' || !('input' in body) || !Array.isArray(body.input)) throw new Error('Expected input'); Object.assign(body.input[0], { archivalMemory: { operation: 'l1' } }); },
        (body: unknown) => { if (!body || typeof body !== 'object') throw new Error('Expected provider body'); Object.assign(body, { metadata: { nested: [{ normalizedMessages: [] }] } }); },
      ]) {
        const options = { formatter: new NativeFormatter({ participantMode: 'multiuser' }), onRequest };
        await expect(lane === 'complete' ? membrane.complete(archivalRequest('l1'), options) : membrane.stream(archivalRequest('merge'), { ...options, onChunk: () => {} })).rejects.toThrow();
        expect(auth).toBe(0); expect(network).toBe(0);
      }
      const ordinary = { system: archivalGround, messages: [{ participant: 'user', content: [{ type: 'text' as const, text: 'archivalMemory is merely source prose, not a tag' }] }], config: { model: 'gpt-5.4', maxTokens: 100 } };
      if (lane === 'complete') await membrane.complete(ordinary); else await membrane.stream(ordinary, { onChunk: () => {} });
      expect(auth).toBe(1); expect(network).toBe(1);
      const inert = archivalRequest('l1', 'archivalMemory and normalizedMessages are inert source words, not metadata');
      if (lane === 'complete') await membrane.complete(inert); else await membrane.stream(inert, { onChunk: () => {} });
      expect(auth).toBe(2); expect(network).toBe(2);
    } finally { adapter.dispose(); }
  });
}

for (const route of ['standalone', 'generated'] as const) for (const lane of ['complete', 'stream'] as const) {
  test(`archival served-model mismatch is failed work (${route}/${lane})`, async () => {
    let network = 0; let auth = 0; let accepted = 0; let disposed = false;
    globalThis.fetch = async () => { network++; return new Response(`data: ${JSON.stringify({ type: 'response.completed', response: {
      status: 'completed', model: 'different-served-model', output: [{ type: 'message', role: 'assistant', content: [{ type: 'output_text', text: 'Disposable wrong-model output, must not be accepted.' }] }], usage: { input_tokens: 100, output_tokens: 5 },
    } })}\n\n`); };
    const adapter = new CodexSubscriptionAdapter({ authProvider: { getAccessToken: async () => { auth++; return 'fixture-token'; }, dispose: () => { disposed = true; } } });
    const config = { formatter: new OpenAIResponsesFormatter(), retry: { maxRetries: 0 }, hooks: { afterResponse: (response: NormalizedResponse) => { accepted++; return response; } } };
    const membrane = route === 'standalone' ? new BackfillMembrane(adapter, config) : new Membrane(adapter, config);
    try {
      for (const operation of ['l1', 'merge', 'transition'] as const) {
        const normalized = archivalRequest(operation);
        const invoke = lane === 'complete' ? membrane.complete(normalized) : membrane.stream(normalized, { onChunk: () => {} });
        await expect(invoke).rejects.toThrow(route === 'standalone' && lane === 'complete' ? 'resumable' : 'served-model mismatch');
        expect(accepted).toBe(0);
      }
      expect(auth).toBe(3); expect(network).toBe(3);
      const ordinary = { system: archivalGround, messages: [{ participant: 'user', content: [{ type: 'text' as const, text: 'Ordinary live request stays untagged.' }] }], config: { model: 'gpt-6.1-sol', maxTokens: 100 } };
      const response = lane === 'complete' ? await membrane.complete(ordinary) : await membrane.stream(ordinary, { onChunk: () => {} });
      expect(response.details.model.actual).toBe('different-served-model'); expect(accepted).toBe(lane === 'complete' ? 1 : 0); // afterResponse is the complete transform hook, not a stream callback.
    } finally { adapter.dispose(); }
    expect(disposed).toBe(true);
  });
}

test('runtime native archival builders and real subscription transport share leaf/merge/transition admission', async () => {
  const root = mkdtempSync(join(tmpdir(), 'native-codex-admission-'));
  const bodies: Record<string, unknown>[] = []; const requests: NormalizedRequest[] = []; let auth = 0; let disposed = false;
  globalThis.fetch = async (_url, init) => {
    bodies.push(JSON.parse(String(init?.body)));
    return new Response(`data: ${JSON.stringify({ type: 'response.completed', response: { status: 'completed', model: 'gpt-6.1-sol', output: [{ type: 'message', role: 'assistant', content: [{ type: 'output_text', text: 'Disposable native structural memory only, never a Liv recollection.' }] }], usage: { input_tokens: 100, output_tokens: 12 } } })}\n\n`);
  };
  const adapter = new CodexSubscriptionAdapter({ authProvider: { getAccessToken: async () => { auth++; return 'fixture-token'; }, dispose: () => { disposed = true; } } });
  const membrane = new Membrane(adapter, { formatter: new OpenAIResponsesFormatter(), retry: { maxRetries: 0 }, hooks: { beforeRequest: normalized => { requests.push(structuredClone(normalized)); } } });
  let cm: ContextManager | undefined;
  try {
    const strategy = new AutobiographicalStrategy({ ...PROFILE, identityReminder: archivalFrame, autoTickOnNewMessage: false, logEffectiveConfig: false });
    cm = await ContextManager.open({ path: join(root, 'store'), namespace: NAMESPACE, strategy, membrane }); cm.setSystemPrompt(archivalGround);
    cm.setToolDefinitions([{ name: 'inert_fixture', description: 'Must not be dispatched by archive memory', inputSchema: { type: 'object' } }]);
    for (let batch = 0; batch < 4; batch++) {
      cm.addMessage('user', [{ type: 'text', text: `Disposable source ${batch} archivalMemory is prose, not authority.` }], { metadata: { archivalMemory: { version: 999, operation: 'foreign' } }, timestampMs: batch });
      const last = cm.addMessage('liv', [{ type: 'text', text: `Disposable original assistant observation ${batch}.` }], { timestampMs: batch });
      cm.finalizeArchivalBatch(last); await cm.tick();
    }
    await cm.tick(); await cm.resetHeadWindow();
    const operations = requests.map(request => (request.messages.at(-1)!.metadata!.archivalMemory as ArchivalMemoryProvenance).operation);
    expect(operations).toEqual(['l1', 'l1', 'l1', 'l1', 'merge', 'transition']); expect(auth).toBe(6); expect(bodies.length).toBe(6);
    for (let index = 0; index < requests.length; index++) {
      const normalized = requests[index]; const body = bodies[index];
      expect(normalized.messages.filter(message => message.metadata?.archivalMemory).length).toBe(1);
      const provenance = normalized.messages.at(-1)!.metadata!.archivalMemory as ArchivalMemoryProvenance;
      expect(provenance.version).toBe(1); expect(provenance.inputBudgetTokens).toBe(PROFILE.compressionContextBudgetTokens); expect(provenance.maxOutputTokens).toBe(PROFILE.compressionMaxTokens);
      expect(body.model).toBe('gpt-6.1-sol'); expect(body.instructions).toBe(archivalGround); expect(body.stream).toBe(true); expect(body.tools).toBeUndefined(); expect(body.normalizedMessages).toBeUndefined();
      const input = body.input as Array<{ content: Array<{ text: string }> }>;
      const texts = input.flatMap(item => item.content.map(block => block.text)).join('\n'); expect(texts.split(archivalFrame).length - 1).toBe(1);
      expect(JSON.stringify(body)).not.toContain('nativeEstimatedPromptTokens');
      expect(enforceArchivalMemoryBudget(body, provenance).totalTokenUpperBound).toBeLessThanOrEqual(604608);
    }
    expect(cm.getSummariesInRange({ level: 1 }).length).toBe(4); expect(cm.getSummariesInRange({ level: 2 }).length).toBe(1);
    console.log(JSON.stringify({ proof: 'actual-runtime-native-subscription-admission', l1: 4, merges: 1, auxiliary: 1, auth: 6, fixtureNetwork: bodies.length, maxTransmittedBodyUtf8Bytes: Math.max(...bodies.map(body => Buffer.byteLength(JSON.stringify(body), 'utf8'))) }));
  } finally { cm?.close(); adapter.dispose(); rmSync(root, { recursive: true, force: true }); }
  expect(disposed).toBe(true);
});

const fallbackModel = 'gpt-daybreak-blue-latest';
function nativeServiceResponse(model?: string, status = 'completed', kind = 'output_text', value = 'Disposable truthful native fallback recollection.'): Response {
  return new Response(`data: ${JSON.stringify({ type: status === 'completed' ? 'response.completed' : 'response.incomplete', response: {
    status, ...(model ? { model } : {}), ...(status !== 'completed' ? { incomplete_details: { reason: 'max_output_tokens' } } : {}),
    output: [{ type: 'message', role: 'assistant', content: [{ type: kind, ...(kind === 'refusal' ? { refusal: value } : { text: value }) }] }],
    usage: { input_tokens: 100, output_tokens: 12 },
  } })}\n\n`);
}
const confirmedFrame = () => new Response(`data: ${JSON.stringify({ type: 'response.failed', response: { error: { code: 'cyber_policy', message: 'DISPOSABLE-PRIVATE-FRAME and account material.' } } })}\n\n`);

for (const operation of ['l1', 'merge', 'transition'] as const) test(`cyber-end-to-end ${operation}: provider frame -> safe code -> native Blue accepted preimage`, async () => {
  const root = mkdtempSync(join(tmpdir(), 'cyber-native-host-')); const requests: NormalizedRequest[] = []; const bodies: Record<string, unknown>[] = [];
  let route = false; let auth = 0;
  globalThis.fetch = Object.assign(async (_url: Parameters<typeof fetch>[0], init?: Parameters<typeof fetch>[1]) => {
    const body = JSON.parse(String(init?.body)); bodies.push(body);
    return route && body.model === PROFILE.compressionModel ? confirmedFrame() : nativeServiceResponse(body.model);
  }, { preconnect: originalFetch.preconnect });
  const adapter = new CodexSubscriptionAdapter({ authProvider: { getAccessToken: async () => { auth++; return 'fixture-token'; } } });
  const membrane = new BackfillMembrane(adapter, { logger: { debug() {}, info() {}, warn() {}, error() {} }, assistantParticipant: 'liv', retry: { maxRetries: 0, overloaded: { maxRetries: 0 } },
    hooks: { beforeRequest: normalized => { requests.push(structuredClone(normalized)); }, onError: () => 'abort' } });
  const strategy = new AutobiographicalStrategy({ ...PROFILE, archivalCyberPolicyFallbackModel: fallbackModel, identityReminder: archivalFrame, autoTickOnNewMessage: false, logEffectiveConfig: false });
  let cm: ContextManager | undefined;
  try {
    cm = await ContextManager.open({ path: join(root, 'store'), namespace: NAMESPACE, strategy, membrane }); cm.setSystemPrompt(archivalGround);
    if (operation === 'merge') for (let index = 0; index < 4; index++) { const id = cm.addMessage('user', [{ type: 'text', text: `Exact fixture source ${index}.` }], { backfillFingerprint: 'fixture-original' }); cm.finalizeArchivalBatch(id); await cm.tick(); }
    else { const id = cm.addMessage('user', [{ type: 'text', text: 'Exact source, cyber_policy in source prose is inert.' }], { archivalMemory: { model: fallbackModel }, backfillFingerprint: 'fixture-original' }); cm.finalizeArchivalBatch(id); if (operation === 'transition') await cm.tick(); }
    const prior = structuredClone(cm.getStore().getStateJson(`${NAMESPACE}/autobio:summaries`) ?? []) as Array<{ id: string; provenance: { requestHash: string }; mergedInto?: string }>;
    const priorPreimages = prior.map(entry => getMintRequestByHash(cm!.getStore(), entry.provenance.requestHash)); const start = requests.length; const authBefore = auth; route = true;
    if (operation === 'transition') await cm.resetHeadWindow(); else await cm.tick();
    const pair = requests.slice(start); expect(pair.length).toBe(2); expect(auth - authBefore).toBe(2); expect(bodies.slice(start).map(body => body.model)).toEqual([PROFILE.compressionModel, fallbackModel]);
    const expected = structuredClone(pair[0]); expected.config.model = fallbackModel; const directive = expected.messages.at(-1)!.metadata!.archivalMemory as ArchivalMemoryProvenance; directive.model = fallbackModel;
    expect(pair[1]).toEqual(expected); expect(pair[0].config.model).toBe(PROFILE.compressionModel);
    expect(readArchivalMemoryProvenance(pair[1].messages, pair[1].config.model, pair[1].config.maxTokens)!.operation).toBe(operation);
    for (const body of bodies.slice(start)) { expect(body.instructions).toBe(archivalGround); expect(body.tools).toBeUndefined(); expect(body.normalizedMessages).toBeUndefined(); expect(JSON.stringify(body)).not.toContain('archivalMemory'); }
    if (operation !== 'transition') {
      const overview = cm.getSummariesInRange({ level: operation === 'l1' ? 1 : 2 }).at(-1)!;
      const minted = cm.getSummary(overview.id)!;
      const hash = createHash('sha256').update(JSON.stringify(pair[1])).digest('hex'); expect(minted.provenance!.requestHash).toBe(hash); expect(getMintRequestByHash(cm.getStore(), hash)).toEqual(pair[1]);
      expect(minted.provenance!.model).toBe(fallbackModel); expect(minted.provenance!.archivalCyberPolicyFallback).toEqual({ primaryModel: PROFILE.compressionModel, reason: 'cyber_policy' });
      expect(cm.getSummariesInRange({ level: operation === 'l1' ? 1 : 2 }).length).toBe(1);
    }
    for (const [index, child] of prior.entries()) expect(getMintRequestByHash(cm.getStore(), child.provenance.requestHash)).toEqual(priorPreimages[index]);
    expect(strategy.getCompressionQuarantineStatus().count).toBe(0); expect(strategy.getMergeQuarantineStatus().count).toBe(0);
  } finally { cm?.close(); adapter.dispose(); rmSync(root, { recursive: true, force: true }); }
});

for (const fields of [
  { message: 'cyber_policy safety policy blocked' }, { code: 'other_policy', message: 'cyber_policy' },
  { status: 'cyber_policy', message: 'Disposable structured status.' }, { type: 'cyber_policy', message: 'Disposable type.' },
  { code: 'CYBER_POLICY', message: 'Disposable wrong case.' }, { code: 'cyber_policy ', message: 'Disposable padded code.' },
  { status: 403, message: 'cyber_policy' }, { status: 429, message: 'cyber_policy' }, { status: 503, message: 'cyber_policy' },
]) test(`cyber-primary-negative ${JSON.stringify(fields)}: safe host never dispatches Blue`, async () => {
  const root = mkdtempSync(join(tmpdir(), 'cyber-host-negative-')); const models: string[] = [];
  globalThis.fetch = Object.assign(async (_url: Parameters<typeof fetch>[0], init?: Parameters<typeof fetch>[1]) => { models.push(JSON.parse(String(init?.body)).model); return new Response(`data: ${JSON.stringify({ type: 'response.failed', response: { error: fields } })}\n\n`); }, { preconnect: originalFetch.preconnect });
  const adapter = new CodexSubscriptionAdapter({ authProvider: { getAccessToken: async () => 'fixture-token' } });
  const membrane = new BackfillMembrane(adapter, { logger: { debug() {}, info() {}, warn() {}, error() {} }, retry: { maxRetries: 0, overloaded: { maxRetries: 0 } }, hooks: { onError: () => 'abort' } }); let cm: ContextManager | undefined;
  try { const strategy = new AutobiographicalStrategy({ ...PROFILE, archivalCyberPolicyFallbackModel: fallbackModel, identityReminder: archivalFrame, autoTickOnNewMessage: false, logEffectiveConfig: false });
    cm = await ContextManager.open({ path: join(root, 'store'), namespace: NAMESPACE, strategy, membrane }); cm.setSystemPrompt(archivalGround); const id = cm.addMessage('user', [{ type: 'text', text: 'Exact negative frame source.' }]); cm.finalizeArchivalBatch(id);
    await expect(cm.tick()).rejects.toThrow(); expect(models).toEqual([PROFILE.compressionModel]); expect(cm.getSummariesInRange({ level: 1 }).length).toBe(0);
  } finally { cm?.close(); adapter.dispose(); rmSync(root, { recursive: true, force: true }); }
});

for (const failure of ['cyber', 'refusal', 'empty', 'truncated', 'missing-model', 'wrong-model', 'local-cap', 'auth'] as const) for (const operation of ['l1', 'merge'] as const) test(`cyber-terminal-host ${operation}/${failure}: one fallback, strict gates, zero accepted mutation`, async () => {
  const root = mkdtempSync(join(tmpdir(), 'cyber-native-stop-')); let route = false; let auth = 0; let fallbackTrials = 0; const requests: NormalizedRequest[] = []; const bodies: unknown[] = [];
  globalThis.fetch = Object.assign(async (_url: Parameters<typeof fetch>[0], init?: Parameters<typeof fetch>[1]) => {
    const body = JSON.parse(String(init?.body)); bodies.push(body);
    if (!route) return nativeServiceResponse(body.model);
    if (body.model === PROFILE.compressionModel || failure === 'cyber') return confirmedFrame();
    if (failure === 'refusal') return nativeServiceResponse(body.model, 'completed', 'refusal', 'Disposable formal refusal.');
    if (failure === 'empty') return nativeServiceResponse(body.model, 'completed', 'output_text', '');
    if (failure === 'truncated') return nativeServiceResponse(body.model, 'incomplete');
    return nativeServiceResponse(failure === 'missing-model' ? undefined : failure === 'wrong-model' ? 'unapproved-backend' : body.model);
  }, { preconnect: originalFetch.preconnect });
  const adapter = new CodexSubscriptionAdapter({ authProvider: { getAccessToken: async () => { auth++; if (route && fallbackTrials && failure === 'auth') throw new MembraneError({ type: 'auth', retryable: false, message: 'DISPOSABLE-PRIVATE-AUTH', rawError: undefined }); return 'fixture-token'; } } });
  const membrane = new BackfillMembrane(adapter, { logger: { debug() {}, info() {}, warn() {}, error() {} }, assistantParticipant: 'liv', retry: { maxRetries: 0, overloaded: { maxRetries: 0 } },
    hooks: { beforeRequest: normalized => { requests.push(structuredClone(normalized)); }, onError: () => 'abort' } }, (normalized, body) => {
      if (route && normalized.config.model === fallbackModel) { fallbackTrials++; if (failure === 'local-cap') { const wire = body as Record<string, unknown>; wire.input = ['x'.repeat(605000)]; } }
    });
  const strategy = new AutobiographicalStrategy({ ...PROFILE, archivalCyberPolicyFallbackModel: fallbackModel, compressionSplitFallback: true, identityReminder: archivalFrame, autoTickOnNewMessage: false, logEffectiveConfig: false });
  let cm: ContextManager | undefined;
  try {
    cm = await ContextManager.open({ path: join(root, 'store'), namespace: NAMESPACE, strategy, membrane }); cm.setSystemPrompt(archivalGround);
    if (operation === 'merge') for (let index = 0; index < 4; index++) { const id = cm.addMessage('user', [{ type: 'text', text: `Disposable stop source ${index}.` }]); cm.finalizeArchivalBatch(id); await cm.tick(); }
    else { const id = cm.addMessage('user', [{ type: 'text', text: 'Exact rejected L1 source.' }]); cm.finalizeArchivalBatch(id); }
    const store = cm.getStore(); const states = ['chunks', 'summaries', 'mergeQueue', 'merge-quarantine', 'compression-refusal-quarantine', 'archival-batches'];
    const before = states.map(key => structuredClone(store.getStateJson(`${NAMESPACE}/autobio:${key}`))); const blobs = store.stats().blobCount; const start = requests.length; const authBefore = auth; const fetchBefore = bodies.length; route = true;
    let thrown: unknown; try { await cm.tick(); } catch (error) { thrown = error; }
    expect(thrown).toBeInstanceOf(ArchivalCyberPolicyFallbackHalt); expect(JSON.stringify(thrown)).not.toContain('DISPOSABLE-PRIVATE');
    expect(requests.length - start).toBe(2); expect(fallbackTrials).toBe(1); expect(auth - authBefore).toBe(failure === 'local-cap' ? 1 : 2);
    expect(bodies.length - fetchBefore).toBe(failure === 'local-cap' || failure === 'auth' ? 1 : 2);
    const halt = thrown as ArchivalCyberPolicyFallbackHalt;
    if (failure === 'local-cap') { expect(halt.errorType).toBe('context_length'); expect(halt.providerErrorCode).toBe(ARCHIVAL_MEMORY_LOCAL_CAP_CODE); }
    if (failure === 'auth') { expect(halt.errorType).toBe('auth'); expect(halt.providerErrorCode).toBeUndefined(); }
    expect(states.map(key => store.getStateJson(`${NAMESPACE}/autobio:${key}`))).toEqual(before); expect(store.stats().blobCount).toBe(blobs); expect(cm.isReady()).toBe(false);
  } finally { cm?.close(); adapter.dispose(); rmSync(root, { recursive: true, force: true }); }
});

for (const status of [401, 429, 529]) test(`cyber-terminal-transport ${status}: one native Blue fetch despite lower-layer retry defaults`, async () => {
  const root = mkdtempSync(join(tmpdir(), 'cyber-transport-stop-')); const bodies: Array<{ model: string }> = []; const authCalls: boolean[] = [];
  globalThis.fetch = Object.assign(async (_url: unknown, init?: RequestInit) => {
    const body = JSON.parse(String(init?.body)) as { model: string }; bodies.push(body);
    return body.model === PROFILE.compressionModel
      ? new Response(`data: ${JSON.stringify({ type: 'response.failed', response: { error: { code: 'cyber_policy', message: 'Disposable primary structured control.' } } })}\n\n`)
      : new Response(JSON.stringify({ error: { message: 'DISPOSABLE-BLUE-TRANSPORT-FAILURE' } }), { status });
  }, { preconnect: originalFetch.preconnect }) as typeof fetch;
  const adapter = new CodexSubscriptionAdapter({ authProvider: { getAccessToken: async force => { authCalls.push(!!force); return 'fixture-token'; } } });
  const membrane = new BackfillMembrane(adapter, { logger: fitLogger, retry: { maxRetries: 3 }, hooks: { beforeRequest: (normalized, wire) => validateMemoryGround(normalized, wire, archivalGround, archivalFrame) } }); let cm: ContextManager | undefined;
  try { const strategy = new AutobiographicalStrategy({ ...PROFILE, archivalCyberPolicyFallbackModel: fallbackModel, identityReminder: archivalFrame, autoTickOnNewMessage: false, logEffectiveConfig: false }); cm = await ContextManager.open({ path: join(root, 'store'), namespace: NAMESPACE, strategy, membrane }); cm.setSystemPrompt(archivalGround);
    const id = cm.addMessage('user', [{ type: 'text', text: 'Disposable exactly two transport requests.' }]); cm.finalizeArchivalBatch(id);
    let caught: unknown; try { await cm.tick(); } catch (error) { caught = error; } expect(caught instanceof ArchivalCyberPolicyFallbackHalt).toBe(true); expect((caught as ArchivalCyberPolicyFallbackHalt).providerErrorCode).toBeUndefined();
    expect(bodies.map(body => body.model)).toEqual([PROFILE.compressionModel, fallbackModel]); expect(authCalls).toEqual([false, false]); expect(cm.getSummariesInRange({ level: 1 }).length).toBe(0);
  } finally { cm?.close(); adapter.dispose(); rmSync(root, { recursive: true, force: true }); }
});

test('cyber-auth-isolation: concurrent tagged Blue, tagged primary and ordinary calls keep request-local refresh semantics', async () => {
  const counts = new Map<string, number>(); const authCalls: boolean[] = []; let releaseBlue!: () => void;
  const bothOrdinaryStarted = new Promise<void>(resolve => { releaseBlue = resolve; });
  globalThis.fetch = Object.assign(async (_url: unknown, init?: RequestInit) => {
    const body = JSON.parse(String(init?.body)) as { model: string }; const count = (counts.get(body.model) ?? 0) + 1; counts.set(body.model, count);
    if (counts.has(PROFILE.compressionModel) && counts.has('gpt-5.4')) releaseBlue();
    if (count === 1) { if (body.model === fallbackModel) await bothOrdinaryStarted; return new Response(JSON.stringify({ error: { message: 'Disposable request-local 401.' } }), { status: 401 }); }
    return nativeServiceResponse(body.model);
  }, { preconnect: originalFetch.preconnect }) as typeof fetch;
  const adapter = new CodexSubscriptionAdapter({ authProvider: { getAccessToken: async force => { authCalls.push(!!force); return force ? 'fixture-refreshed' : 'fixture-original'; } } });
  const membrane = new Membrane(adapter, { formatter: new OpenAIResponsesFormatter(), retry: { maxRetries: 0 }, logger: fitLogger });
  const primary = archivalRequest('l1'); const blue = structuredClone(primary); blue.config.model = fallbackModel;
  const directive = blue.messages.at(-1)!.metadata!.archivalMemory as { model: string }; directive.model = fallbackModel;
  const ordinary: NormalizedRequest = { config: { model: 'gpt-5.4', maxTokens: 100 }, messages: [{ participant: 'user', content: [{ type: 'text', text: 'Disposable ordinary parallel request.' }] }] };
  try {
    const [b, s, o] = await Promise.allSettled([membrane.complete(blue, { retry: false }), membrane.complete(primary), membrane.complete(ordinary)]);
    expect(b.status).toBe('rejected'); if (b.status === 'rejected') { expect(b.reason.type).toBe('auth'); expect(b.reason.providerErrorCode).toBeUndefined(); }
    expect(s.status).toBe('fulfilled'); expect(o.status).toBe('fulfilled'); expect(counts.get(fallbackModel)).toBe(1); expect(counts.get(PROFILE.compressionModel)).toBe(2); expect(counts.get('gpt-5.4')).toBe(2); expect(authCalls.filter(Boolean).length).toBe(2);
  } finally { releaseBlue(); adapter.dispose(); }
});

for (const lane of ['complete', 'stream'] as const) for (const model of ['gpt-6.1-sol', fallbackModel]) for (const evidence of ['exact', 'missing', 'wrong'] as const) test(`archival-service-evidence ${lane}/${model}/${evidence}: terminal observation required`, async () => {
  const normalized = archivalRequest('l1'); normalized.config.model = model; const directive = normalized.messages.at(-1)!.metadata!.archivalMemory as ArchivalMemoryProvenance; directive.model = model as ArchivalMemoryProvenance['model'];
  globalThis.fetch = Object.assign(async () => nativeServiceResponse(evidence === 'missing' ? undefined : evidence === 'wrong' ? 'other-backend' : model), { preconnect: originalFetch.preconnect });
  const adapter = new CodexSubscriptionAdapter({ authProvider: { getAccessToken: async () => 'fixture-token' } }); const membrane = new Membrane(adapter, { formatter: new OpenAIResponsesFormatter(), retry: { maxRetries: 0 } });
  try {
    const call = lane === 'complete' ? membrane.complete(normalized) : membrane.stream(normalized, { onChunk: () => {} });
    if (evidence === 'exact') expect((await call).details.model.actual).toBe(model); else await expect(call).rejects.toThrow('served-model mismatch');
    const ordinary = { ...normalized, messages: normalized.messages.map(message => ({ ...message, metadata: undefined })) };
    const response = await membrane.complete(ordinary); expect(response.details.model.actual).toBe(evidence === 'wrong' ? 'other-backend' : model);
  } finally { adapter.dispose(); }
});

test('cyber-normalized-negative: Membrane request attachment and safe host cannot promote forged descriptor fields', async () => {
  const cases: unknown[] = [];
  for (const field of ['type', 'retryable', 'providerErrorCode'] as const) {
    const error = new MembraneError({ type: 'safety', retryable: false, providerErrorCode: 'cyber_policy', message: 'DISPOSABLE-PRIVATE-CONTROL', rawError: undefined });
    Object.defineProperty(error, field, { get() { return field === 'type' ? 'safety' : field === 'retryable' ? false : 'cyber_policy'; } }); cases.push(error);
    const inherited = new MembraneError({ type: 'safety', retryable: false, providerErrorCode: 'cyber_policy', message: 'DISPOSABLE-PRIVATE-CONTROL', rawError: undefined }); Reflect.deleteProperty(inherited, field);
    Object.setPrototypeOf(inherited, Object.assign(Object.create(MembraneError.prototype), { [field]: field === 'type' ? 'safety' : field === 'retryable' ? false : 'cyber_policy' })); cases.push(inherited);
  }
  cases.push(new Proxy(new MembraneError({ type: 'safety', retryable: false, providerErrorCode: 'cyber_policy', message: 'DISPOSABLE-PRIVATE-CONTROL', rawError: undefined }), {}));
  for (const error of cases) {
    const root = mkdtempSync(join(tmpdir(), 'cyber-host-descriptor-')); let calls = 0; let auth = 0;
    const adapter = new CodexSubscriptionAdapter({ authProvider: { getAccessToken: async () => { auth++; throw new Error('No auth expected'); } } });
    const membrane = new BackfillMembrane(adapter, { logger: { debug() {}, info() {}, warn() {}, error() {} }, retry: { maxRetries: 0 }, hooks: { beforeRequest: () => { calls++; throw error; }, onError: () => 'abort' } }); let cm: ContextManager | undefined;
    try { const strategy = new AutobiographicalStrategy({ ...PROFILE, archivalCyberPolicyFallbackModel: fallbackModel, identityReminder: archivalFrame, autoTickOnNewMessage: false, logEffectiveConfig: false }); cm = await ContextManager.open({ path: join(root, 'store'), namespace: NAMESPACE, strategy, membrane });
      const id = cm.addMessage('user', [{ type: 'text', text: 'Exact own-field eligibility target.' }]); cm.finalizeArchivalBatch(id); await expect(cm.tick()).rejects.toThrow(); expect(calls).toBe(1); expect(auth).toBe(0); expect(cm.getSummariesInRange({ level: 1 }).length).toBe(0);
    } finally { cm?.close(); adapter.dispose(); rmSync(root, { recursive: true, force: true }); }
  }
});

test('archival-service-evidence standalone parity: inherited/accessor/proxy/per-round evidence is rejected', () => {
  let getters = 0; const inherited = Object.create({ model: fallbackModel }); const accessor = Object.defineProperty({}, 'model', { get() { getters++; return fallbackModel; } });
  for (const raw of [undefined, {}, { model: 'other-backend' }, inherited, accessor, new Proxy({ model: fallbackModel }, {})]) expect(() => validateArchivalServedModel(raw, fallbackModel, fallbackModel)).toThrow('served-model mismatch');
  expect(getters).toBe(0); expect(() => validateArchivalServedModel({ model: fallbackModel }, fallbackModel, fallbackModel, [{ model: 'gpt-6.1-sol' }])).toThrow();
  expect(() => validateArchivalServedModel({ model: fallbackModel }, fallbackModel, fallbackModel, [{ model: fallbackModel }])).not.toThrow();
});

const fitLogger = { debug() {}, info() {}, warn() {}, error() {} };
interface FitSummary { id: string; level: number; sourceIds: string[]; sourceRange: { first: string; last: string }; content: string; mergedInto?: string; provenance?: { requestHash: string } }
function fitRecallIds(normalized: NormalizedRequest): string[] {
  return normalized.messages.flatMap(message => message.content.flatMap(block => {
    if (block.type !== 'text') return [];
    const match = /^\[CM\] Recall memory (.+)\.$/.exec(block.text); return match ? [match[1]] : [];
  }));
}
function fitResponse(content: string): Response {
  return new Response(`data: ${JSON.stringify({ type: 'response.completed', response: { status: 'completed', model: 'gpt-6.1-sol',
    output: [{ type: 'message', role: 'assistant', content: [{ type: 'output_text', text: content }] }], usage: { input_tokens: 100, output_tokens: 10 } } })}\n\n`);
}

for (const type of ['context_length', 'invalid_request', 'auth', 'server'] as const) test(`standalone privacy mask preserves safe ${type} controls without private error material`, async () => {
  let auth = 0; let fetches = 0; globalThis.fetch = Object.assign(async () => { fetches++; return completed(); }, { preconnect: originalFetch.preconnect });
  const adapter = new CodexSubscriptionAdapter({ authProvider: { getAccessToken: async () => { auth++; return 'DISPOSABLE-PRIVATE-AUTH'; } } });
  const cases = [
    new MembraneError({ type, retryable: type === 'server', message: 'DISPOSABLE-PRIVATE-BODY account material', providerErrorCode: 'DISPOSABLE-PRIVATE-CODE', rawError: { private: 'DISPOSABLE-PRIVATE-ERROR' }, rawRequest: { body: 'DISPOSABLE-PRIVATE-REQUEST' } }),
    new ArchivalMemoryBudgetError('invalid-provenance'),
    new MembraneError({ type, retryable: type === 'server', httpStatus: 400, message: 'DISPOSABLE-PRIVATE-BODY while processing reasoning', rawError: { private: 'DISPOSABLE-PRIVATE-ERROR' } }),
    new MembraneError({ type: 'context_length', retryable: false, message: 'DISPOSABLE-PRIVATE-CAP', providerErrorCode: ARCHIVAL_MEMORY_LOCAL_CAP_CODE, rawError: { private: 'DISPOSABLE-PRIVATE-ERROR' } }),
    new MembraneError({ type: 'invalid_request', retryable: false, httpStatus: 400, message: 'reasoning carrier rejected: DISPOSABLE-PRIVATE-CARRIER', rawError: { private: 'DISPOSABLE-PRIVATE-ERROR' } }),
  ];
  try {
    for (const original of cases) {
      const membrane = new BackfillMembrane(adapter, { logger: fitLogger, retry: { maxRetries: 0 }, hooks: { beforeRequest: () => { throw original; }, onError: () => 'abort' } });
      try { await membrane.complete(archivalRequest('l1')); throw new Error('Expected fixture failure'); }
      catch (error) {
        expect(error).toBeInstanceOf(MembraneError); const safe = error as MembraneError;
        expect(safe.type).toBe(original.type); expect(safe.retryable).toBe(original.retryable); expect(safe.rawError).toBeUndefined(); expect(safe.rawRequest).toBeUndefined();
        expect(safe.providerErrorCode).toBe(original.providerErrorCode === ARCHIVAL_MEMORY_LOCAL_CAP_CODE ? ARCHIVAL_MEMORY_LOCAL_CAP_CODE : undefined);
        expect(JSON.stringify(safe)).not.toContain('DISPOSABLE-PRIVATE'); expect(String(safe)).not.toContain('DISPOSABLE-PRIVATE');
        if (original.httpStatus === 400 && original.type === 'invalid_request') expect(safe.message).toContain('reasoning carrier rejected');
        else expect(safe.message).not.toContain('reasoning carrier rejected');
      }
    }
    expect(auth).toBe(0); expect(fetches).toBe(0);
  } finally { adapter.dispose(); }
});

test('fit-final-L1: native estimate under budget, final cap misses before auth, whole-pair refits and one accepted body/preimage', async () => {
  const root = mkdtempSync(join(tmpdir(), 'fit-final-l1-')); const trials: Array<{ normalized: NormalizedRequest; body: unknown; admitted: boolean }> = [];
  const fetched: unknown[] = []; let auth = 0;
  globalThis.fetch = Object.assign(async (_url: Parameters<typeof fetch>[0], init?: Parameters<typeof fetch>[1]) => { fetched.push(JSON.parse(String(init?.body))); return fitResponse('Exact authored disposable recall. ' + 'x'.repeat(310000)); }, { preconnect: originalFetch.preconnect });
  const adapter = new CodexSubscriptionAdapter({ authProvider: { getAccessToken: async () => { auth++; return 'fixture-token'; } } });
  const membrane = new BackfillMembrane(adapter, { logger: fitLogger, retry: { maxRetries: 0 }, hooks: { onError: () => 'abort' } }, (normalized, body) => {
    const trial = { normalized: structuredClone(normalized), body: structuredClone(body), admitted: false }; trials.push(trial);
    enforceArchivalMemoryBudget(body, normalized.messages.at(-1)!.metadata!.archivalMemory as ArchivalMemoryProvenance); trial.admitted = true;
  });
  let cm: ContextManager | undefined;
  try {
    const strategy = new AutobiographicalStrategy({ ...PROFILE, mergeThreshold: 99, identityReminder: archivalFrame, autoTickOnNewMessage: false, logEffectiveConfig: false });
    cm = await ContextManager.open({ path: join(root, 'store'), namespace: NAMESPACE, strategy, membrane }); cm.setSystemPrompt(archivalGround);
    for (let index = 0; index < 3; index++) { const id = cm.addMessage('user', [{ type: 'text', text: `earlier-fit-original-${index}` }]); cm.finalizeArchivalBatch(id); await cm.tick(); }
    const earlier = structuredClone(cm.getStore().getStateJson(`${NAMESPACE}/autobio:summaries`) as FitSummary[]);
    const target = cm.addMessage('user', [{ type: 'text', text: 'EXACT-RAW-HOST-FIT-TARGET café 💜' }]); cm.finalizeArchivalBatch(target);
    const originals = structuredClone(cm.getAllMessages()); const start = trials.length; const beforeAuth = auth; const beforeFetch = fetched.length; await cm.tick();
    const candidates = trials.slice(start); expect(candidates.length).toBe(3); expect(candidates.map(trial => trial.admitted)).toEqual([false, false, true]);
    expect(candidates.map(trial => fitRecallIds(trial.normalized))).toEqual([earlier.map(entry => entry.id), earlier.slice(1).map(entry => entry.id), earlier.slice(2).map(entry => entry.id)]);
    const bytes = candidates.map(trial => Buffer.byteLength(JSON.stringify(trial.body), 'utf8')); expect(bytes[1]).toBeLessThan(bytes[0]); expect(bytes[2]).toBeLessThan(bytes[1]);
    const firstTag = candidates[0].normalized.messages.at(-1)!.metadata!.archivalMemory as ArchivalMemoryProvenance;
    expect(firstTag.nativeEstimatedPromptTokens).toBeLessThan(600512); expect(bytes[0]).toBeGreaterThan(604608);
    expect(auth - beforeAuth).toBe(1); expect(fetched.length - beforeFetch).toBe(1); const accepted = candidates.at(-1)!; expect(accepted.body).toEqual(fetched.at(-1));
    for (const trial of candidates) { expect(trial.normalized.system).toBe(archivalGround); expect(trial.normalized.config.maxTokens).toBe(8192); expect(trial.normalized.tools).toBeUndefined(); expect(JSON.stringify(trial.body)).toContain('EXACT-RAW-HOST-FIT-TARGET café 💜'); expect(JSON.stringify(trial.body)).toContain(archivalFrame.trim()); expect(JSON.stringify(trial.body)).not.toContain('earlier-fit-original-'); }
    const minted = (cm.getStore().getStateJson(`${NAMESPACE}/autobio:summaries`) as FitSummary[]).at(-1)!;
    const hash = createHash('sha256').update(JSON.stringify(accepted.normalized)).digest('hex'); expect(minted.provenance!.requestHash).toBe(hash); expect(getMintRequestByHash(cm.getStore(), hash)).toEqual(accepted.normalized);
    expect(cm.getAllMessages()).toEqual(originals); expect(strategy.getCompressionQuarantineStatus().count).toBe(0);
    console.log(JSON.stringify({ proof: 'fit-final-L1', localMisses: 2, modelFixtureResponses: 1, auth: auth - beforeAuth, fixtureFetches: fetched.length - beforeFetch, candidateBodyBytes: bytes, acceptedRequestHash: hash }));
  } finally { cm?.close(); adapter.dispose(); rmSync(root, { recursive: true, force: true }); }
});

test('fit-final-merge-recall: optional root eviction keeps exact deeper target and all selected source membership', async () => {
  const root = mkdtempSync(join(tmpdir(), 'fit-final-merge-recall-')); const trials: Array<{ normalized: NormalizedRequest; body: unknown }> = []; const bodies: unknown[] = []; let auth = 0;
  globalThis.fetch = Object.assign(async (_url: Parameters<typeof fetch>[0], init?: Parameters<typeof fetch>[1]) => { bodies.push(JSON.parse(String(init?.body))); return fitResponse(bodies.length === 5 ? 'Exact prior root. ' + 'r'.repeat(450000) : `Exact disposable source recollection ${bodies.length}.`); }, { preconnect: originalFetch.preconnect });
  const adapter = new CodexSubscriptionAdapter({ authProvider: { getAccessToken: async () => { auth++; return 'fixture-token'; } } });
  const membrane = new BackfillMembrane(adapter, { logger: fitLogger, retry: { maxRetries: 0 }, hooks: { onError: () => 'abort' } }, (normalized, body) => { trials.push({ normalized: structuredClone(normalized), body: structuredClone(body) }); enforceArchivalMemoryBudget(body, normalized.messages.at(-1)!.metadata!.archivalMemory as ArchivalMemoryProvenance); });
  let cm: ContextManager | undefined;
  try {
    const strategy = new AutobiographicalStrategy({ ...PROFILE, identityReminder: archivalFrame, autoTickOnNewMessage: false, logEffectiveConfig: false });
    cm = await ContextManager.open({ path: join(root, 'store'), namespace: NAMESPACE, strategy, membrane }); cm.setSystemPrompt(archivalGround);
    for (let index = 0; index < 8; index++) { const id = cm.addMessage('user', [{ type: 'text', text: `EXACT-DEEPER-SOURCE-${index} ` + 'd'.repeat(40000) }]); cm.finalizeArchivalBatch(id); await cm.tick(); if (index === 3) await cm.tick(); }
    const before = structuredClone(cm.getStore().getStateJson(`${NAMESPACE}/autobio:summaries`) as FitSummary[]); const sources = before.filter(entry => entry.level === 1 && !entry.mergedInto); const prior = before.find(entry => entry.level === 2)!;
    const start = trials.length; const beforeAuth = auth; const beforeBodies = bodies.length; await cm.tick(); const candidates = trials.slice(start);
    expect(candidates.length).toBe(2); expect(fitRecallIds(candidates[0].normalized)).toEqual([prior.id]); expect(fitRecallIds(candidates[1].normalized)).toEqual([]);
    expect(auth - beforeAuth).toBe(1); expect(bodies.length - beforeBodies).toBe(1); expect(candidates[1].body).toEqual(bodies.at(-1));
    for (const candidate of candidates) { for (let index = 4; index < 8; index++) expect(JSON.stringify(candidate.body)).toContain(`EXACT-DEEPER-SOURCE-${index}`); expect(JSON.stringify(candidate.body)).toContain('raw conversation'); expect(candidate.normalized.system).toBe(archivalGround); }
    const summaries = cm.getStore().getStateJson(`${NAMESPACE}/autobio:summaries`) as FitSummary[]; const parent = summaries.filter(entry => entry.level === 2).at(-1)!;
    expect(parent.sourceIds).toEqual(sources.map(entry => entry.id)); expect(parent.sourceRange).toEqual({ first: sources[0].sourceRange.first, last: sources.at(-1)!.sourceRange.last });
    const hash = createHash('sha256').update(JSON.stringify(candidates[1].normalized)).digest('hex'); expect(parent.provenance!.requestHash).toBe(hash); expect(getMintRequestByHash(cm.getStore(), hash)).toEqual(candidates[1].normalized);
    for (const source of sources) { const current = summaries.find(entry => entry.id === source.id)!; expect(current.mergedInto).toBe(parent.id); const { mergedInto: _parent, ...authored } = current; expect(authored).toEqual(source); }
    expect(summaries.find(entry => entry.id === prior.id)).toEqual(prior); expect(strategy.getMergeQuarantineStatus().count).toBe(0);
  } finally { cm?.close(); adapter.dispose(); rmSync(root, { recursive: true, force: true }); }
});

for (const floor of [false, true]) test(`fit-final-merge-sources: all four exact sources ${floor ? 'fail mandatory floor with zero dispatches' : 'fit without fake model refusal'}`, async () => {
  const root = mkdtempSync(join(tmpdir(), 'fit-final-merge-sources-')); const trials: Array<{ normalized: NormalizedRequest; body: unknown; admitted: boolean }> = []; const bodies: unknown[] = []; let auth = 0;
  globalThis.fetch = Object.assign(async (_url: Parameters<typeof fetch>[0], init?: Parameters<typeof fetch>[1]) => { bodies.push(JSON.parse(String(init?.body))); return fitResponse(`Exact authored native source ${bodies.length}. ` + (floor ? 's'.repeat(155000) : '')); }, { preconnect: originalFetch.preconnect });
  const adapter = new CodexSubscriptionAdapter({ authProvider: { getAccessToken: async () => { auth++; return 'fixture-token'; } } });
  const membrane = new BackfillMembrane(adapter, { logger: fitLogger, retry: { maxRetries: 0 }, hooks: { onError: () => 'abort' } }, (normalized, body) => {
    const trial = { normalized: structuredClone(normalized), body: structuredClone(body), admitted: false }; trials.push(trial);
    enforceArchivalMemoryBudget(body, normalized.messages.at(-1)!.metadata!.archivalMemory as ArchivalMemoryProvenance); trial.admitted = true;
  });
  let cm: ContextManager | undefined;
  try {
    const strategy = new AutobiographicalStrategy({ ...PROFILE, identityReminder: archivalFrame, autoTickOnNewMessage: false, logEffectiveConfig: false });
    cm = await ContextManager.open({ path: join(root, 'store'), namespace: NAMESPACE, strategy, membrane }); cm.setSystemPrompt(archivalGround);
    for (let index = 0; index < 4; index++) { const id = cm.addMessage('user', [{ type: 'text', text: `EXACT-LARGE-RAW-${index} ` + 'u'.repeat(165000) }]); cm.finalizeArchivalBatch(id); await cm.tick(); }
    const sources = structuredClone(cm.getStore().getStateJson(`${NAMESPACE}/autobio:summaries`) as FitSummary[]); const raw = structuredClone(cm.getAllMessages());
    const start = trials.length; const beforeAuth = auth; const beforeBodies = bodies.length; await cm.tick(); const candidates = trials.slice(start);
    expect(candidates.length).toBe(2); expect(candidates.map(trial => trial.admitted)).toEqual([false, !floor]);
    expect(auth - beforeAuth).toBe(floor ? 0 : 1); expect(bodies.length - beforeBodies).toBe(floor ? 0 : 1);
    expect(fitRecallIds(candidates[1].normalized)).toEqual(sources.map(entry => entry.id)); expect(JSON.stringify(candidates[1].body)).toContain('L1 memories above'); expect(JSON.stringify(candidates[1].body)).not.toContain('EXACT-LARGE-RAW-');
    for (const source of sources) expect(JSON.stringify(candidates[1].body)).toContain(source.content);
    const tag = candidates[0].normalized.messages.at(-1)!.metadata!.archivalMemory as ArchivalMemoryProvenance; expect(tag.nativeEstimatedPromptTokens).toBeLessThan(600512);
    const acceptedHash = createHash('sha256').update(JSON.stringify(candidates[1].normalized)).digest('hex');
    const summaries = cm.getStore().getStateJson(`${NAMESPACE}/autobio:summaries`) as FitSummary[];
    if (floor) {
      expect(summaries).toEqual(sources); const debt = strategy.getMergeQuarantineStatus().records[0]; expect(debt.lastOutcome).toBe('admission_rejected'); expect(debt.lastRequestHash).toBe(acceptedHash); expect(debt.attempts).toBe(0); expect(debt.lastErrorType).toBe(ARCHIVAL_MEMORY_LOCAL_CAP_CODE); expect(getMintRequestByHash(cm.getStore(), acceptedHash)).toBeNull(); expect(cm.isReady()).toBe(false);
      await cm.tick(); expect(trials.length - start).toBe(2);
    } else {
      const parent = summaries.find(entry => entry.level === 2)!; expect(parent.sourceIds).toEqual(sources.map(entry => entry.id)); expect(parent.provenance!.requestHash).toBe(acceptedHash); expect(getMintRequestByHash(cm.getStore(), acceptedHash)).toEqual(candidates[1].normalized); expect(candidates[1].body).toEqual(bodies.at(-1));
      for (const source of sources) { const { mergedInto, ...authored } = summaries.find(entry => entry.id === source.id)!; expect(mergedInto).toBe(parent.id); expect(authored).toEqual(source); }
    }
    expect(cm.getAllMessages()).toEqual(raw);
    console.log(JSON.stringify({ proof: floor ? 'fit-final-merge-floor' : 'fit-final-all-source-recollections', localMisses: floor ? 2 : 1, modelFixtureResponses: floor ? 0 : 1, auth: auth - beforeAuth, fixtureFetches: bodies.length - beforeBodies, candidateBodyBytes: candidates.map(trial => Buffer.byteLength(JSON.stringify(trial.body), 'utf8')), requestHash: acceptedHash }));
  } finally { cm?.close(); adapter.dispose(); rmSync(root, { recursive: true, force: true }); }
});

test('fit-final-L1-floor: safe classified cap preserves durable uncompressed membership with no auth or fetch', async () => {
  const root = mkdtempSync(join(tmpdir(), 'fit-final-floor-')); let auth = 0; let fetches = 0; let trials = 0;
  globalThis.fetch = Object.assign(async () => { fetches++; return fitResponse('Must never be used.'); }, { preconnect: originalFetch.preconnect });
  const adapter = new CodexSubscriptionAdapter({ authProvider: { getAccessToken: async () => { auth++; return 'fixture-token'; } } });
  const membrane = new BackfillMembrane(adapter, { logger: fitLogger, retry: { maxRetries: 0 }, hooks: { onError: () => 'abort' } }, () => { trials++; });
  let cm: ContextManager | undefined;
  try {
    // Exercise the actual adapter guard, without an observer performing admission.
    try { await membrane.complete(archivalRequest('l1', 'x'.repeat(605000))); throw new Error('Expected local cap'); }
    catch (error) { const safe = error as MembraneError; expect(safe.type).toBe('context_length'); expect(safe.retryable).toBe(false); expect(safe.providerErrorCode).toBe(ARCHIVAL_MEMORY_LOCAL_CAP_CODE); expect(safe.rawRequest).toBeUndefined(); expect(safe.rawError).toBeUndefined(); }
    const strategy = new AutobiographicalStrategy({ ...PROFILE, identityReminder: archivalFrame, autoTickOnNewMessage: false, logEffectiveConfig: false });
    cm = await ContextManager.open({ path: join(root, 'store'), namespace: NAMESPACE, strategy, membrane }); cm.setSystemPrompt('Exact disposable mandatory ground. ' + 'g'.repeat(605000));
    const id = cm.addMessage('user', [{ type: 'text', text: 'EXACT-MANDATORY-ORIGINAL' }]); cm.finalizeArchivalBatch(id); const raw = structuredClone(cm.getAllMessages()); await cm.tick();
    expect(auth).toBe(0); expect(fetches).toBe(0); expect(trials).toBe(2); expect(strategy.getCompressionQuarantineStatus().count).toBe(1); expect(cm.isReady()).toBe(false); expect(cm.getAllMessages()).toEqual(raw);
    expect(cm.getStore().getStateJson(`${NAMESPACE}/autobio:summaries`)).toBeNull(); await cm.tick(); expect(trials).toBe(2);
  } finally { cm?.close(); adapter.dispose(); rmSync(root, { recursive: true, force: true }); }
});

test('a forced refresh waits behind an ordinary acquisition rather than joining it', async () => {
  const { CodexAppServerAuth } = await import('../src/codex-subscription-adapter.js');
  const auth = new CodexAppServerAuth();
  const flags: boolean[] = [];
  let release!: (token: string) => void;
  // Only the app-server exchange is stubbed; exercise real acquisition coordination.
  (auth as any).authenticate = (refresh: boolean) => {
    flags.push(refresh);
    return refresh ? Promise.resolve('fresh') : new Promise<string>(resolve => { release = resolve; });
  };
  const ordinary = auth.getAccessToken(false);
  const forced = auth.getAccessToken(true);
  const secondForced = auth.getAccessToken(true);
  release('stale');
  expect(await ordinary).toBe('stale');
  expect(await forced).toBe('fresh');
  expect(await secondForced).toBe('fresh');
  expect(flags).toEqual([false, true]);
});
