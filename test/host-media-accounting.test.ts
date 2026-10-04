import { describe, expect, spyOn, test } from 'bun:test';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { JsStore } from '@animalabs/chronicle';
import { ContextManager, AutobiographicalStrategy, WindowedPassthroughStrategy, MessageStore, defaultTokenEstimator, jsonTokenEstimator } from '@animalabs/context-manager';
import type { MetadataCompileResultWithProvenance, SummaryEntry, TokenBudget } from '@animalabs/context-manager';
import { Agent, AgentFramework } from '@animalabs/agent-framework';
import { Membrane, MockAdapter, NativeFormatter } from '@animalabs/membrane';
import type { ContentBlock, ImageContent } from '@animalabs/membrane';
import { measureContent } from '../src/content-accounting.js';
import { buildContextCurve, runContextPreview, PanelError, type PanelAppRef } from '../src/web/panel-data.js';

function image(data: string, tokenEstimate?: number): ImageContent {
  return { type: 'image', source: { type: 'base64', mediaType: 'image/png', data }, ...(tokenEstimate !== undefined ? { tokenEstimate } : {}) };
}

function withMessageStore<T>(run: (messages: MessageStore, store: JsStore) => T): T {
  const dir = mkdtempSync(join(tmpdir(), 'host-media-cost-'));
  const store = JsStore.openOrCreate({ path: join(dir, 'store') });
  try {
    MessageStore.register(store);
    return run(new MessageStore(store), store);
  } finally {
    store.close();
    rmSync(dir, { recursive: true, force: true });
  }
}

// Only selection is a fixture: the typed snapshot prices referenced Chronicle
// originals synchronously. Post-selection live reads fail rather than mask races.
async function curve(content: ContentBlock[][], selected: Array<{ participant: string; content: ContentBlock[] }>, sourceGroups?: number[][], calibration = 1) {
  const dir = mkdtempSync(join(tmpdir(), 'host-media-curve-'));
  const store = JsStore.openOrCreate({ path: join(dir, 'store') });
  MessageStore.register(store);
  const messages = new MessageStore(store);
  const ids = content.map(blocks => messages.append('user', blocks).id);
  const cm = {
    compileMetadata: async (_budget: TokenBudget, options: { provenance: true }): Promise<MetadataCompileResultWithProvenance> => {
      expect(options).toEqual({ provenance: true });
      const entries = selected.map((entry, i) => ({
        renderedTokens: messages.estimateContentTokens(entry.content, calibration),
        sourceMessageIds: sourceGroups ? sourceGroups[i].map(index => ids[index]) : [ids[i]],
        sourceSummaryIds: [], summaryLevel: null,
      }));
      const view = messages.createMetadataView();
      const sources = [...new Set(entries.flatMap(entry => entry.sourceMessageIds))].map(id => {
        const source = view.get(id)!;
        return { id, tokens: messages.estimateContentTokens(source.content, calibration), timestamp: new Date(source.timestamp) };
      });
      const branch = store.currentBranch();
      return { tokenCalibration: calibration, estimatedTokens: entries.reduce((sum, entry) => sum + entry.renderedTokens, 0),
        messages: selected.map((entry, i) => ({ ...entry,
          ...(sourceGroups ? { sourceMessageIds: [...entries[i].sourceMessageIds] } : { sourceMessageId: ids[i] }),
        })), provenance: { branch: { id: branch.id, name: branch.name, head: branch.head }, entries, sources } };
    },
    estimateContentTokens: () => { throw new Error('curve repriced after snapshot'); },
    getMessageCount: () => { throw new Error('curve read live count'); },
    getMessageWindow: () => { throw new Error('curve read live history'); },
    getStrategy: () => { throw new Error('curve read live summaries'); },
    currentBranch: () => { throw new Error('curve read live branch'); },
  };
  const app = {
    framework: { getAgent: () => ({ maxTokens: 4096, getContextManager: () => cm }), getAgentRuntimeSettings: () => ({ contextBudgetTokens: 123_456 }) },
    recipe: { agent: { contextBudgetTokens: 200_000, maxTokens: 4096 } },
  } as unknown as PanelAppRef;
  // Archive resolution here is a failure, not a permissive mock returning data.
  store.getBlob = () => { throw new Error('context curve resolved archived media'); };
  try {
    return await buildContextCurve(app, 'root') as {
      budget: { maxTokens: number; reserveForResponse: number };
      totals: { rendered: number; rawCovered: number };
      entries: Array<{ rendered: number; rawCovered: number; nImages: number; kind: string }>;
    };
  } finally {
    store.close();
    rmSync(dir, { recursive: true, force: true });
  }
}

function curveApp(cm: unknown, maxTokens = 20000, reserveForResponse = 2000): PanelAppRef {
  return { framework: { getAgent: () => ({ maxTokens: reserveForResponse, getContextManager: () => cm }),
    getAgentRuntimeSettings: () => ({ contextBudgetTokens: maxTokens }) }, recipe: { agent: {} } } as unknown as PanelAppRef;
}

function holdCurveSelection(cm: ContextManager) {
  let release!: () => void;
  let ready!: (result: MetadataCompileResultWithProvenance) => void;
  const gate = new Promise<void>(resolve => { release = resolve; });
  const selected = new Promise<MetadataCompileResultWithProvenance>(resolve => { ready = resolve; });
  const facade = {
    compileMetadata: async (budget: TokenBudget, options: { provenance: true }): Promise<MetadataCompileResultWithProvenance> => {
      expect(options).toEqual({ provenance: true });
      const result = await cm.compileMetadata(budget, options);
      ready(result);
      await gate;
      return result;
    },
    getMessageCount: () => { throw new Error('post-selection live count'); },
    getMessageWindow: () => { throw new Error('post-selection live history'); },
    getStrategy: () => { throw new Error('post-selection live strategy'); },
    currentBranch: () => { throw new Error('post-selection live branch'); },
    estimateContentTokens: () => { throw new Error('post-selection live price'); },
  };
  return { selected, release, facade };
}

type CurveSnapshot = {
  branch: string;
  totals: { rendered: number; rawCovered: number };
  entries: Array<{ kind: string; id: string | null; text: string; rendered: number; rawCovered: number;
    msgCount: number; nImages: number; dateFirst: Date | null; dateLast: Date | null }>;
};

class CurveSummaryStrategy extends AutobiographicalStrategy {
  seedSummary(entry: SummaryEntry): void { this.pushSummary(entry); }
}

describe('real same-selection context curve snapshots', () => {
  for (const mutation of ['edit', 'branch'] as const) {
    test(`holds old branch/content/prices/dates while awaiting a real ${mutation}`, async () => {
      const dir = mkdtempSync(join(tmpdir(), 'host-curve-snapshot-race-'));
      const cm = await ContextManager.open({ path: join(dir, 'store'), strategy: new AutobiographicalStrategy({
        headWindowTokens: 0, recentWindowTokens: 100000, targetChunkTokens: 100000,
        adaptiveResolution: true, autoTickOnNewMessage: false,
        maxLiveImages: 0, maxLiveImageBytes: 0, imageStripDepthTokens: 0,
      }) });
      try {
        const id = cm.addMessage('User', [{ type: 'text', text: 'x'.repeat(400) }]);
        const store = cm.getStore();
        const main = cm.currentBranch().name;
        if (mutation === 'branch') {
          store.createBranch('curve-side');
          await cm.switchBranch('curve-side');
          cm.editMessage(id, [{ type: 'text', text: 'y'.repeat(4000) }]);
          await cm.switchBranch(main);
        }
        const oldDate = cm.getMessage(id)!.timestamp;
        const held = holdCurveSelection(cm);
        const pending = buildContextCurve(curveApp(held.facade), 'root');
        const captured = await held.selected;
        if (mutation === 'edit') cm.editMessage(id, [{ type: 'text', text: 'y'.repeat(4000) }]);
        else await cm.switchBranch('curve-side');
        const sequence = store.currentSequence();
        const stats = cm.getRenderStats();
        const work = cm.getPendingWork();
        store.getBlob = () => { throw new Error('curve resolved a blob'); };
        held.release();
        const result = await pending as CurveSnapshot;
        expect(result.branch).toBe(main);
        expect(result.entries[0].text).toBe('x'.repeat(400));
        expect(result.entries[0].dateFirst).toEqual(oldDate);
        expect(result.totals.rendered).toBe(captured.estimatedTokens);
        expect(result.totals.rawCovered).toBe(captured.provenance.sources[0].tokens);
        expect(store.currentSequence()).toBe(sequence);
        expect(cm.getRenderStats()).toEqual(stats);
        expect(cm.getPendingWork()).toEqual(work);
        const newer = await buildContextCurve(curveApp(cm), 'root') as CurveSnapshot;
        expect(newer.entries[0].text).toBe('y'.repeat(4000));
        expect(newer.totals.rawCovered).toBeGreaterThan(result.totals.rawCovered);
        expect(newer.branch).toBe(mutation === 'branch' ? 'curve-side' : main);
      } finally { cm.close(); rmSync(dir, { recursive: true, force: true }); }
    });
  }

  test('stripped auxiliary base64 is rendered19 but retains its original948 coverage', async () => {
    const dir = mkdtempSync(join(tmpdir(), 'host-curve-auxiliary-'));
    const writer = await ContextManager.open({ path: join(dir, 'store'), strategy: new WindowedPassthroughStrategy() });
    const reader = await ContextManager.open({ store: writer.getStore(), namespace: 'subconscious/primary',
      isolate: true, auxiliaryMessageViews: [{}], strategy: new AutobiographicalStrategy({
        headWindowTokens: 0, recentWindowTokens: 100000, targetChunkTokens: 100000, adaptiveResolution: true,
        autoTickOnNewMessage: false, maxLiveImages: 1, maxLiveImageBytes: 1, imageStripDepthTokens: 100000,
      }) });
    try {
      const png = 'iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNkYPhfDwAChwGA60e6kgAAAABJRU5ErkJggg==';
      const id = writer.addMessage('User', [{ type: 'text', text: 'auxiliary original' }, image(png, 941)]);
      const timestamp = writer.getMessageWindow(0, writer.getMessageCount(), { resolveBlobs: false }).messages[0].timestamp;
      const store = writer.getStore();
      const sequence = store.currentSequence();
      store.getBlob = () => { throw new Error('auxiliary curve resolved an image'); };
      const result = await buildContextCurve(curveApp(reader), 'Subconscious') as CurveSnapshot;
      expect(reader.getMessageCount()).toBe(0);
      expect(result.entries).toHaveLength(1);
      expect(result.entries[0]).toMatchObject({ id, rendered: 19, rawCovered: 948, nImages: 0,
        dateFirst: timestamp, dateLast: timestamp });
      expect(result.totals).toMatchObject({ rendered: 19, rawCovered: 948 });
      expect(store.currentSequence()).toBe(sequence);
    } finally { reader.close(); writer.close(); rmSync(dir, { recursive: true, force: true }); }
  });

  for (const mode of ['adaptive', 'positioned', 'combined'] as const) {
    test(`${mode} answers keep exact summary/auxiliary leaf identity across await`, async () => {
      const dir = mkdtempSync(join(tmpdir(), 'host-curve-summary-snapshot-'));
      const writer = await ContextManager.open({ path: join(dir, 'store'), strategy: new WindowedPassthroughStrategy() });
      const strategy = new CurveSummaryStrategy({ headWindowTokens: 0, recentWindowTokens: 8,
        targetChunkTokens: 100000, autoTickOnNewMessage: false, adaptiveResolution: mode === 'adaptive',
        positionedRecallPairs: mode !== 'combined', recallHeaderTemplate: 'Identical custom header',
        summaryContextLabel: 'Identical custom header', maxLiveImages: 1, maxLiveImageBytes: 1, imageStripDepthTokens: 100000 });
      const reader = await ContextManager.open({ store: writer.getStore(), namespace: 'subconscious/summary',
        isolate: true, auxiliaryMessageViews: [{}], strategy });
      try {
        const ids = [0, 1, 2, 3].map(i => (i % 2 ? reader : writer).addMessage('user',
          [{ type: 'text', text: ('raw-' + i + ' ').repeat(180) }]));
        reader.addMessage('user', [{ type: 'text', text: 'latest ' + 'Z'.repeat(60) }]);
        const seed = (id: string, level: number, sourceLevel: number, sourceIds: string[], range: string[],
          extra: Partial<SummaryEntry> = {}) => strategy.seedSummary({ id, level, sourceLevel, sourceIds,
          sourceRange: { first: range[0], last: range.at(-1)! }, created: 0,
          content: 'Same prefix '.repeat(12) + id, tokens: 45, ...extra });
        seed('L1-0', 1, 0, [ids[0]], [ids[0]], { mergedInto: 'L2-100', parentId: 'L2-100' });
        seed('L1-1', 1, 0, [ids[1]], [ids[1]], { mergedInto: 'L2-100', parentId: 'L2-100' });
        seed('L2-100', 2, 1, ['L1-0', 'L1-1'], ids.slice(0, 2));
        seed('L1-101', 1, 0, [ids[2]], [ids[2]]);
        seed('L1-102', 1, 0, [ids[3]], [ids[3]]);
        const held = holdCurveSelection(reader);
        const pending = buildContextCurve(curveApp(held.facade, mode === 'adaptive' ? 600 : 20000, 0), 'Subconscious');
        const captured = await held.selected;
        const capturedBranch = { ...captured.provenance.branch };
        strategy.getSummary('L2-100')!.sourceIds.splice(0, 2, 'L1-102');
        const store = writer.getStore();
        const sequence = store.currentSequence();
        store.getBlob = () => { throw new Error('summary curve opened an archive'); };
        held.release();
        const result = await pending as CurveSnapshot;
        const answers = result.entries.filter(entry => entry.kind !== 'raw');
        if (mode === 'combined') {
          expect(answers).toHaveLength(1);
          expect(answers[0]).toMatchObject({ kind: 'L2', id: 'L2-100', msgCount: 4 });
        } else {
          expect(answers.map(answer => answer.id)).toEqual(mode === 'adaptive'
            ? ['L2-100', 'L1-101'] : ['L2-100', 'L1-101', 'L1-102']);
          expect(answers[0]).toMatchObject({ kind: 'L2', msgCount: 2 });
        }
        expect(result.totals.rendered).toBe(captured.estimatedTokens);
        const scaffolding = captured.provenance.entries.filter(row => row.summaryLevel === null && !row.sourceMessageIds.length)
          .reduce((sum, row) => sum + row.renderedTokens, 0);
        expect(result.totals.rawCovered).toBe(captured.provenance.sources.reduce((sum, row) => sum + row.tokens, scaffolding));
        expect(result.branch).toBe(capturedBranch.name);
        expect(store.currentSequence()).toBe(sequence);
        result.entries.forEach((entry, i) => expect(entry.rendered).toBe(captured.provenance.entries[i].renderedTokens));
      } finally { reader.close(); writer.close(); rmSync(dir, { recursive: true, force: true }); }
    });
  }

  test('unavailable originals and generated scaffolding retain distinct explicit fallbacks', async () => {
    const content: ContentBlock[] = [{ type: 'text', text: 'unavailable or generated' }];
    const snapshot: MetadataCompileResultWithProvenance = {
      messages: [{ participant: 'user', content, sourceMessageId: 'removed' }, { participant: 'root', content },
        { participant: 'Context Manager', content }], tokenCalibration: 1.7, estimatedTokens: 30,
      provenance: { branch: { id: 'captured-branch-id', name: 'captured-name', head: 123 }, sources: [],
        entries: [{ renderedTokens: 7, sourceMessageIds: ['removed'], sourceSummaryIds: [], summaryLevel: null },
          { renderedTokens: 9, sourceMessageIds: ['removed'], sourceSummaryIds: ['L2-missing-leaves'], summaryLevel: 2 },
          { renderedTokens: 14, sourceMessageIds: [], sourceSummaryIds: [], summaryLevel: null }] },
    };
    const cm = { compileMetadata: async (_budget: TokenBudget, options: { provenance: true }) => {
      expect(options).toEqual({ provenance: true }); return snapshot;
    } };
    const result = await buildContextCurve(curveApp(cm), 'root') as CurveSnapshot;
    expect(result.entries.map(entry => [entry.kind, entry.rawCovered, entry.msgCount])).toEqual([
      ['raw', 7, 1], ['L2', 0, 0], ['raw', 14, 1],
    ]);
    expect(result.entries.every(entry => entry.dateFirst === null && entry.dateLast === null)).toBe(true);
    expect(result.totals).toMatchObject({ rendered: 30, rawCovered: 21 });
  });
});

describe('host semantic content accounting', () => {
  test('base64 size is irrelevant for top-level and deeply nested images', () => {
    for (const data of ['AA==', 'A'.repeat(1_000_000)]) {
      expect(measureContent([image(data)])).toEqual({ tokens: 1600, nImages: 1 });
      expect(measureContent([{ type: 'tool_result', content: [{ type: 'tool_result', content: [image(data, 731)] }] }]))
        .toEqual({ tokens: 731, nImages: 1 });
    }
    expect(measureContent([{ type: 'image', source: { type: 'url', url: 'https://example.test/photo.png' }, tokenEstimate: 0 }]))
      .toEqual({ tokens: 0, nImages: 1 });
  });

  test('never inspects rawItem or image bytes to estimate normalized or archived content', () => {
    const carrier = { type: 'text', text: '', get rawItem(): never { throw new Error('read rawItem'); } };
    const media = { type: 'image', tokenEstimate: 907, get source(): never { throw new Error('read source'); }, get rawItem(): never { throw new Error('read rawItem'); } };
    const ref = { type: 'blob_ref', ref: { originalType: 'image', hash: 'unopened' }, tokenEstimate: 907, get rawItem(): never { throw new Error('read rawItem'); } };
    expect(measureContent([media, ref, carrier]).tokens).toBe(1814);
    expect(measureContent([{ type: 'text', text: 'Visible words', get rawItem(): never { throw new Error('read rawItem'); } }]).tokens)
      .toBe(defaultTokenEstimator('Visible words'));
    expect(measureContent([{ type: 'text', text: '', get rawItem(): never { throw new Error('read rawItem'); } }]).tokens).toBe(0);
  });

  test('matches canonical uncalibrated MessageStore text/tool/thinking/media costs', () => {
    const content: ContentBlock[] = [
      { type: 'text', text: 'ordinary prose and {"dense":"json"}' },
      { type: 'tool_use', id: 'call', name: 'read', input: { path: '/a/b', count: 2 } },
      { type: 'tool_result', toolUseId: 'call', content: [{ type: 'text', text: 'Tool output' }, image('AA==', 2193)] },
      { type: 'tool_result', toolUseId: 'empty', content: '' },
      { type: 'thinking', thinking: 'short', signature: 's'.repeat(3300) },
      { type: 'thinking', thinking: 'visible', tokenEstimate: 419 } as ContentBlock,
      { type: 'redacted_thinking', data: 'X'.repeat(3600) },
      { type: 'redacted_thinking', data: '' },
      { type: 'audio', source: { type: 'base64', mediaType: 'audio/wav', data: 'AAAA' } },
    ];
    withMessageStore(messages => {
      expect(measureContent(content).tokens).toBe(messages.estimateContentTokens(content));
    });
    expect(measureContent([{ type: 'tool_result', content: '{"result":true}' }]).tokens).toBe(jsonTokenEstimator('{"result":true}'));
  });
});

describe('live agent response reserves in context diagnostics', () => {
  let previewClock = Date.now();

  test('real Agent defaults and explicit reserves govern dry curve and framework preview despite stale recipes', async () => {
    const dir = mkdtempSync(join(tmpdir(), 'host-reserve-parity-'));
    const store = JsStore.openOrCreate({ path: join(dir, 'store') });
    MessageStore.register(store);
    const messages = new MessageStore(store);
    messages.append('user', [{ type: 'text', text: 'A retained observation' }, image('AAAA', 731)]);
    const cm = await ContextManager.open({
      store,
      strategy: new AutobiographicalStrategy({
        compressionModel: 'unused-no-model', autoTickOnNewMessage: false, adaptiveResolution: true,
        headWindowTokens: 10_000, recentWindowTokens: 10_000,
      }),
    });
    const membrane = new Membrane(new MockAdapter(), { formatter: new NativeFormatter() });
    membrane.complete = async () => { throw new Error('diagnostics ran inference'); };
    membrane.stream = async () => { throw new Error('diagnostics ran streaming inference'); };
    store.getBlob = () => { throw new Error('diagnostics resolved archived media'); };
    cm.compile = async () => { throw new Error('diagnostics called committing compile'); };
    const compileMetadata = cm.compileMetadata.bind(cm);
    const previewContext = cm.previewContext.bind(cm);
    const curveBudgets: unknown[] = [];
    const previewBudgets: unknown[] = [];
    cm.compileMetadata = (...args) => { curveBudgets.push(args[0]); return compileMetadata(...args); };
    cm.previewContext = (...args) => { previewBudgets.push(args[0]); return previewContext(...args); };
    const clock = spyOn(Date, 'now').mockImplementation(() => previewClock);
    try {
      const before = store.currentSequence();
      const pendingBefore = cm.getPendingWork();
      for (const [index, fixture] of [
        { maxTokens: undefined, recipeReserve: 1000, reserve: 4096 },
        { maxTokens: 2048, recipeReserve: 16_384, reserve: 2048 },
        { maxTokens: 2000, recipeReserve: 1000, reserve: 2000 },
      ].entries()) {
        const agent = new Agent({
          name: 'root', model: 'unused-no-model', systemPrompt: '', contextBudgetTokens: 16_000,
          ...(fixture.maxTokens === undefined ? {} : { maxTokens: fixture.maxTokens }),
        }, cm, membrane);
        // Only the framework registry is adapted; the actual Framework preview
        // implementation, Agent default, and CM dry selectors execute unchanged.
        const agents: Record<string, typeof agent> = { root: agent };
        const registry = { agents: { get: (name: string) => agents[name] } } as unknown as AgentFramework;
        const app = {
          framework: {
            getAgent: () => agent,
            getAgentRuntimeSettings: () => ({ contextBudgetTokens: 16_000 }),
            previewContextSettings: (...args: Parameters<AgentFramework['previewContextSettings']>) =>
              AgentFramework.prototype.previewContextSettings.call(registry, ...args),
          },
          recipe: { agent: { contextBudgetTokens: 99_999, maxTokens: fixture.recipeReserve } },
        } as unknown as PanelAppRef;
        const curve = await buildContextCurve(app, 'root');
        expect(curve.budget).toEqual({ maxTokens: 16_000, reserveForResponse: fixture.reserve });
        expect(curveBudgets[index]).toEqual(curve.budget);
        expect(curve.entries).toHaveLength(1);
        previewClock += 3001; // Advance past the real operator cooldown without waiting.
        const result = runContextPreview(app, 'root', { budget: 16_000 });
        const preview = result.preview as { finalTokens: number; budgetTokens: number; exhausted: boolean };
        const effective = 16_000 - fixture.reserve;
        expect(previewBudgets[index]).toEqual(curve.budget);
        expect(result.accounting).toEqual({
          requestedBudgetTokens: 16_000, reserveForResponseTokens: fixture.reserve,
          effectiveBudgetTokens: effective, rejectionBudgetTokens: preview.budgetTokens,
          fitsRequested: preview.finalTokens <= effective,
          withinGrace: preview.finalTokens <= preview.budgetTokens,
          unreachable: preview.exhausted && preview.finalTokens > effective,
        });
        try {
          runContextPreview(app, 'root', { budget: 16_000 });
          throw new Error('preview cooldown was bypassed');
        } catch (error) {
          expect(error).toBeInstanceOf(PanelError);
          expect((error as PanelError).status).toBe(429);
        }
        expect(store.currentSequence()).toBe(before);
        expect(cm.getPendingWork()).toEqual(pendingBefore);
      }
    } finally {
      clock.mockRestore();
      cm.close();
      store.close();
      rmSync(dir, { recursive: true, force: true });
    }
  }, 15_000);

  test('requested fit, grace tolerance, exhaustion and zero floor use the framework reserve, not recipe', () => {
    const reserve = 2000;
    const clock = spyOn(Date, 'now').mockImplementation(() => previewClock);
    try {
      for (const fixture of [
        { budget: 16_000, finalTokens: 14_500, rejection: 14_280, exhausted: true, fitsRequested: false, withinGrace: false, unreachable: true },
        { budget: 16_000, finalTokens: 14_100, rejection: 14_280, exhausted: true, fitsRequested: false, withinGrace: true, unreachable: true },
        { budget: 16_000, finalTokens: 14_000, rejection: 14_280, exhausted: true, fitsRequested: true, withinGrace: true, unreachable: false },
        { budget: 1000, finalTokens: 1, rejection: 0, exhausted: false, fitsRequested: false, withinGrace: false, unreachable: false },
      ]) {
        // Fixed selector outputs exercise the hard/grace boundary independently
        // of the real Agent/CM default and selection evidence above.
        const cm = { previewContext: (budget: { maxTokens: number; reserveForResponse: number }) => {
          expect(budget).toEqual({ maxTokens: fixture.budget, reserveForResponse: reserve });
          return { finalTokens: fixture.finalTokens, budgetTokens: fixture.rejection, exhausted: fixture.exhausted, fits: fixture.withinGrace };
        } };
        const agent = { maxTokens: reserve, getContextManager: () => cm };
        const agents: Record<string, typeof agent> = { root: agent };
        const registry = { agents: { get: (name: string) => agents[name] } } as unknown as AgentFramework;
        const app = {
          framework: {
            getAgent: () => agent,
            previewContextSettings: (...args: Parameters<AgentFramework['previewContextSettings']>) =>
              AgentFramework.prototype.previewContextSettings.call(registry, ...args),
          },
          recipe: { agent: { maxTokens: 1000 } },
        } as unknown as PanelAppRef;
        previewClock += 3001;
        expect(runContextPreview(app, 'root', { budget: fixture.budget }).accounting).toEqual({
          requestedBudgetTokens: fixture.budget, reserveForResponseTokens: reserve,
          effectiveBudgetTokens: Math.max(0, fixture.budget - reserve), rejectionBudgetTokens: fixture.rejection,
          fitsRequested: fixture.fitsRequested, withinGrace: fixture.withinGrace, unreachable: fixture.unreachable,
        });
      }
    } finally {
      clock.mockRestore();
    }
  });
});

describe('context curve snapshot calibration parity', () => {
  for (const fixture of [
    { calibration: 1.7, custom: false, mixed: false },
    ...[0.6, 1.7].flatMap(calibration => [false, true].map(custom => ({ calibration, custom, mixed: true }))),
  ]) {
    test(`real metadata and curve agree without mutation (calibration=${fixture.calibration}, custom=${fixture.custom}, mixed=${fixture.mixed})`, async () => {
      const dir = mkdtempSync(join(tmpdir(), 'host-calibrated-curve-'));
      const cm = await ContextManager.open({
        path: join(dir, 'store'), namespace: 'diagnostic-fixture',
        ...(fixture.custom ? { tokenEstimator: (value: string) => value.length + 3 } : {}),
        strategy: new AutobiographicalStrategy({
          adaptiveResolution: true, autoTickOnNewMessage: false,
          headWindowTokens: 0, recentWindowTokens: 100_000, targetChunkTokens: 100_000,
          maxLiveImages: 10, maxLiveImageBytes: 0, imageStripDepthTokens: 0,
        }),
      });
      const content: ContentBlock[] = [{ type: 'text', text: 'x'.repeat(400) }];
      if (fixture.mixed) content.push(
        { type: 'text', text: 'a' }, { type: 'text', text: 'b' },
        { type: 'tool_use', id: 'vision', name: 'inspect', input: { path: 'photo' } },
        { type: 'tool_result', toolUseId: 'vision', content: [
          { type: 'text', text: 'nested observation' }, image('AAAA', 731),
          { type: 'tool_result', toolUseId: 'recalled', content: [image('AQID', 907), { type: 'text', text: 'inside' }] },
        ] },
        image('AAAA', 0),
        { type: 'thinking', thinking: '', signature: 's'.repeat(3300) },
        { type: 'thinking', thinking: 'not the stamped price', signature: 's'.repeat(3300), tokenEstimate: 7 } as ContentBlock,
        { type: 'redacted_thinking', data: '', tokenEstimate: 9 } as ContentBlock,
      );
      try {
        if (fixture.mixed) {
          // A real tool cycle has a preceding assistant use and a separate
          // user result; bundled assistant results cause generated repairs,
          // whose existing unattributed coverage policy is not archive-only.
          cm.addMessage('root', content.filter(block => block.type === 'tool_use'));
          cm.addMessage('user', content.filter(block => block.type === 'tool_result'));
          cm.addMessage('root', content.filter(block => block.type !== 'tool_use' && block.type !== 'tool_result'));
        } else {
          cm.addMessage('user', content);
        }
        const store = cm.getStore();
        store.setStateJson('diagnostic-fixture/autobio:calibration', { multiplier: fixture.calibration });
        const membrane = new Membrane(new MockAdapter(), { formatter: new NativeFormatter() });
        membrane.complete = async () => { throw new Error('curve ran inference'); };
        membrane.stream = async () => { throw new Error('curve streamed inference'); };
        const agent = new Agent({ name: 'root', model: 'unused-no-model', systemPrompt: '' }, cm, membrane);
        const app = {
          framework: { getAgent: () => agent, getAgentRuntimeSettings: () => ({ contextBudgetTokens: 100_000 }) },
          recipe: { agent: { maxTokens: 1000, contextBudgetTokens: 99_999 } },
        } as unknown as PanelAppRef;
        store.getBlob = () => { throw new Error('calibrated curve opened archived media'); };
        cm.compile = async () => { throw new Error('calibrated curve called committing compile'); };
        const liveEstimate = cm.getLiveImagePolicy()!.estimateTokens!;
        const liveBefore = liveEstimate(content);
        const stats = cm.getRenderStats();
        const pending = cm.getPendingWork();
        const sequence = store.currentSequence();
        const archive = cm.getMessageWindow(0, cm.getMessageCount(), { resolveBlobs: false }).messages;
        const metadata = await cm.compileMetadata({ maxTokens: 100_000, reserveForResponse: 4096 });
        const curve = await buildContextCurve(app, 'root') as {
          budget: { maxTokens: number; reserveForResponse: number };
          totals: { rendered: number; rawCovered: number };
          entries: Array<{ rendered: number; rawCovered: number; nImages: number }>;
        };
        expect(metadata.tokenCalibration).toBe(fixture.calibration);
        expect(curve.budget).toEqual({ maxTokens: 100_000, reserveForResponse: 4096 });
        expect(curve.totals.rendered).toBe(metadata.estimatedTokens);
        expect(curve.entries.reduce((sum, entry) => sum + entry.rendered, 0)).toBe(metadata.estimatedTokens);
        expect(curve.totals.rawCovered).toBe(archive.reduce((sum, message) =>
          sum + cm.estimateContentTokens(message.content, metadata.tokenCalibration), 0));
        expect(curve.entries.reduce((sum, entry) => sum + entry.nImages, 0)).toBe(fixture.mixed ? 3 : 0);
        if (!fixture.mixed) {
          expect(metadata.estimatedTokens).toBe(235);
          expect(curve.totals).toEqual({ entries: curve.entries.length, rendered: 235, rawCovered: 235 });
        }
        if (fixture.custom) {
          expect(cm.estimateContentTokens([{ type: 'text', text: 'abc' }], metadata.tokenCalibration))
            .toBe(Math.round(6 * fixture.calibration));
          expect(curve.totals.rendered).not.toBe(Math.round(measureContent(content).tokens * fixture.calibration));
        }
        expect(await cm.compileMetadata({ maxTokens: 100_000, reserveForResponse: 4096 })).toEqual(metadata);
        expect(liveEstimate(content)).toBe(liveBefore);
        expect(cm.getRenderStats()).toEqual(stats);
        expect(cm.getPendingWork()).toEqual(pending);
        expect(store.currentSequence()).toBe(sequence);
        expect(cm.getMessageWindow(0, cm.getMessageCount(), { resolveBlobs: false }).messages).toEqual(archive);
      } finally {
        cm.close();
        rmSync(dir, { recursive: true, force: true });
      }
    });
  }
});

describe('context curve image accounting at the panel boundary', () => {
  test('real Autobiographical selection of new sized refs stays blob-free and read-only through the panel consumer', async () => {
    const dir = mkdtempSync(join(tmpdir(), 'host-real-media-curve-'));
    const store = JsStore.openOrCreate({ path: join(dir, 'store') });
    MessageStore.register(store);
    const messages = new MessageStore(store);
    messages.append('user', [image('AAAA')]);
    messages.append('user', [image('AQID', 731)]);
    messages.append('root', [{ type: 'tool_use', id: 'photo', name: 'screenshot', input: {} }]);
    messages.append('user', [{ type: 'tool_result', toolUseId: 'photo', content: [image('A'.repeat(1_000_000), 907)] }]);
    const cm = await ContextManager.open({
      store,
      strategy: new AutobiographicalStrategy({
        compressionModel: 'unused-no-model', autoTickOnNewMessage: false,
        headWindowTokens: 10_000, recentWindowTokens: 10_000,
      }),
    });
    let blobReads = 0;
    store.getBlob = () => { blobReads++; throw new Error('diagnostic selected through resolved archive media'); };
    // Ordinary compile is not a metadata API, even if a warm blob cache masks
    // some archive reads. The real metadata strategy path remains unstubbed.
    cm.compile = async () => { throw new Error('panel called ordinary compile'); };
    try {
      const before = store.currentSequence();
      const metadata = await cm.compileMetadata({ maxTokens: 20_000, reserveForResponse: 64 });
      let refs = 0;
      for (const message of metadata.messages) {
        for (const block of message.content) {
          expect(block.type).not.toBe('image');
          if (block.type === 'blob_ref') refs++;
          if (block.type === 'tool_result' && Array.isArray(block.content)) {
            for (const child of block.content) {
              expect(child.type).toBe('blob_ref');
              refs++;
            }
          }
        }
      }
      expect(refs).toBe(3);
      const app = {
        framework: { getAgent: () => ({ maxTokens: 64, getContextManager: () => cm }) },
        recipe: { agent: { contextBudgetTokens: 20_000, maxTokens: 64 } },
      } as unknown as PanelAppRef;
      const result = await buildContextCurve(app, 'root') as { totals: { rendered: number; rawCovered: number }; entries: Array<{ nImages: number }> };
      const expected = 3238 + jsonTokenEstimator('{}') + 20;
      expect(result.totals).toMatchObject({ rendered: expected, rawCovered: expected });
      expect(result.entries.reduce((sum, entry) => sum + entry.nImages, 0)).toBe(3);
      expect(blobReads).toBe(0);
      expect(store.currentSequence()).toBe(before);
    } finally {
      cm.close();
      store.close();
      rmSync(dir, { recursive: true, force: true });
    }
  });

  for (const gate of ['count', 'depth', 'bytes-disabled'] as const) {
    test(`legacy panel size inspection respects ${gate} eligibility without encoding or writes`, async () => {
      const dir = mkdtempSync(join(tmpdir(), 'host-legacy-media-curve-'));
      const store = JsStore.openOrCreate({ path: join(dir, 'store') });
      MessageStore.register(store);
      const messages = new MessageStore(store);
      const pngs = [
        'iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAusB9YG0/e8AAAAASUVORK5CYII=',
        'iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNkYPhfDwAChwGA60e6kgAAAABJRU5ErkJggg==',
      ];
      const legacy = pngs.map((data, i) => ({
        type: 'blob_ref' as const,
        ref: { hash: store.storeBlob(Buffer.from(data, 'base64'), 'image/png'), mediaType: 'image/png', originalType: 'image' as const },
        tokenEstimate: i === 0 ? 907 : 731,
      }));
      // Public Chronicle/MessageStore fixtures reproduce the stored historical
      // schema, backed by real PNG blobs, without patching CM private state.
      for (const ref of legacy) messages.append('user', [ref] as unknown as ContentBlock[]);
      const cm = await ContextManager.open({
        store,
        strategy: new AutobiographicalStrategy({
          compressionModel: 'unused-no-model', autoTickOnNewMessage: false,
          headWindowTokens: 0, recentWindowTokens: 20_000,
          maxLiveImages: gate === 'depth' ? 0 : 1,
          imageStripDepthTokens: gate === 'depth' ? 731 : 0,
          maxLiveImageBytes: gate === 'bytes-disabled' ? 0 : pngs[1].length,
        }),
      });
      const getBlob = store.getBlob.bind(store);
      const reads: string[] = [];
      cm.compile = async () => { throw new Error('legacy panel called ordinary compile'); };
      try {
        store.getBlob = hash => {
          reads.push(hash);
          expect(hash).toBe(legacy[1].ref.hash);
          if (gate === 'bytes-disabled') throw new Error('disabled byte policy inspected a legacy blob');
          const bytes = getBlob(hash);
          if (bytes) bytes.toString = () => { throw new Error('metadata encoded legacy image bytes'); };
          return bytes;
        };
        const before = store.currentSequence();
        const metadata = await cm.compileMetadata({ maxTokens: 20_000, reserveForResponse: 64 });
        const blocks = metadata.messages.flatMap(message => message.content);
        expect(blocks.some(block => block.type === 'image')).toBe(false);
        const selected = blocks.filter(block => block.type === 'blob_ref');
        expect(selected).toHaveLength(1);
        expect(selected[0]).toMatchObject(legacy[1]);
        const app = {
          framework: { getAgent: () => ({ maxTokens: 64, getContextManager: () => cm }) },
          recipe: { agent: { contextBudgetTokens: 20_000, maxTokens: 64 } },
        } as unknown as PanelAppRef;
        const result = await buildContextCurve(app, 'root') as {
          totals: { rendered: number; rawCovered: number }; entries: Array<{ nImages: number }>;
        };
        expect(result.totals.rawCovered).toBe(1638);
        expect(result.totals.rendered).toBeGreaterThanOrEqual(731);
        expect(result.totals.rendered).toBeLessThan(1638);
        expect(result.entries.reduce((sum, entry) => sum + entry.nImages, 0)).toBe(1);
        if (gate === 'bytes-disabled') expect(reads).toHaveLength(0);

        expect(reads.every(hash => hash === legacy[1].ref.hash)).toBe(true);
        expect(messages.getWindow(0, 2, { resolveBlobs: false }).messages.map(message => message.content))
          .toEqual(legacy.map(ref => [ref]));
        expect(store.currentSequence()).toBe(before);
      } finally {
        cm.close();
        store.close();
        rmSync(dir, { recursive: true, force: true });
      }
    });
  }

  test('archived/resolved and nested images have the same explicit costs without blob resolution', async () => {
    const top = image('AA==', 731);
    const nested: ContentBlock = { type: 'tool_result', toolUseId: 'call', content: [image('A'.repeat(1_000_000), 731)] };
    const result = await curve([[top], [nested]], [
      { participant: 'user', content: [top] },
      { participant: 'user', content: [nested] },
    ]);
    expect(result.entries.map(entry => [entry.rendered, entry.rawCovered, entry.nImages])).toEqual([[731, 731, 1], [731, 731, 1]]);
    expect(result.totals).toMatchObject({ rendered: 1462, rawCovered: 1462 });
    expect(result.budget).toEqual({ maxTokens: 123_456, reserveForResponse: 4096 });
  });

  test('grouped raw provenance covers original images even when rendering strips one', async () => {
    const placeholder = '[Image omitted by the configured live-image policy]';
    const result = await curve([[image('AA==', 731)], [image('AQID', 907)]], [{
      participant: 'user', content: [{ type: 'text', text: placeholder }, image('AQID', 907)],
    }], [[0, 1]], 0.6);
    expect(result.entries).toHaveLength(1);
    expect(result.entries[0]).toMatchObject({
      kind: 'raw', nImages: 1,
      rendered: Math.round(defaultTokenEstimator(placeholder) * 0.6) + Math.round(907 * 0.6),
      rawCovered: Math.round(731 * 0.6) + Math.round(907 * 0.6),
    });
  });

  test('normalization splits rendered parts without double charging their shared source history', async () => {
    const text = 'A tool observation';
    const result = await curve([[image('AA==', 731), { type: 'text', text }]], [
      { participant: 'user', content: [image('AA==', 731)] },
      { participant: 'root', content: [{ type: 'text', text }] },
    ], [[0], [0]], 1.7);
    const original = Math.round(731 * 1.7) + Math.round(defaultTokenEstimator(text) * 1.7);
    expect(result.totals).toMatchObject({ rendered: original, rawCovered: original });
    expect(result.entries.map(entry => entry.rawCovered)).toEqual([original, 0]);
  });

  test('summary rendered cost differs from the archived image-bearing history it covers', async () => {
    const defaultImage = image('AA==');
    const nested: ContentBlock = { type: 'tool_result', toolUseId: 'call', content: [image('A'.repeat(1_000_000), 2103), { type: 'text', text: 'Tool observation' }] };
    const dir = mkdtempSync(join(tmpdir(), 'host-summary-curve-'));
    const store = JsStore.openOrCreate({ path: join(dir, 'store') });
    try {
      MessageStore.register(store);
      const messages = new MessageStore(store);
      const sourceIds = [messages.append('user', [defaultImage]).id, messages.append('user', [nested]).id];
      const summaryText = 'A concise account of two observations';
      const calibration = 1.7;
      const cm = {
        compileMetadata: async (_budget: TokenBudget, options: { provenance: true }): Promise<MetadataCompileResultWithProvenance> => {
          expect(options).toEqual({ provenance: true });
          const content: ContentBlock[] = [{ type: 'text', text: summaryText }];
          const renderedTokens = messages.estimateContentTokens(content, calibration);
          const view = messages.createMetadataView();
          const branch = store.currentBranch();
          return { tokenCalibration: calibration, estimatedTokens: renderedTokens,
            messages: [{ participant: 'root', content }], provenance: {
              branch: { id: branch.id, name: branch.name, head: branch.head },
              entries: [{ renderedTokens, sourceMessageIds: [...sourceIds], sourceSummaryIds: ['L2-a'], summaryLevel: 2 }],
              sources: sourceIds.map(id => {
                const source = view.get(id)!;
                return { id, tokens: messages.estimateContentTokens(source.content, calibration), timestamp: new Date(source.timestamp) };
              }),
            } };
        },
        estimateContentTokens: () => { throw new Error('summary curve repriced live'); },
        getMessageCount: () => { throw new Error('summary curve read live count'); },
        getMessageWindow: () => { throw new Error('summary curve read live history'); },
        getStrategy: () => { throw new Error('summary curve read live summaries'); },
        currentBranch: () => { throw new Error('summary curve read live branch'); },
      };
      store.getBlob = () => { throw new Error('summary curve resolved archived media'); };
      const app = { framework: { getAgent: () => ({ maxTokens: 4096, getContextManager: () => cm }) }, recipe: { agent: {} } } as unknown as PanelAppRef;
      const result = await buildContextCurve(app, 'root') as { entries: Array<{ kind: string; rendered: number; rawCovered: number; nImages: number; msgCount: number }> };
      expect(result.entries).toHaveLength(1);
      expect(result.entries[0]).toMatchObject({
        kind: 'L2', rendered: Math.round(defaultTokenEstimator(summaryText) * calibration),
        rawCovered: Math.round(1600 * calibration) + Math.round((2103 + defaultTokenEstimator('Tool observation')) * calibration),
        nImages: 0, msgCount: 2,
      });
    } finally {
      store.close();
      rmSync(dir, { recursive: true, force: true });
    }
  });
});
