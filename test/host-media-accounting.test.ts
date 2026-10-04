import { describe, expect, test } from 'bun:test';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { JsStore } from '@animalabs/chronicle';
import { ContextManager, AutobiographicalStrategy, MessageStore, defaultTokenEstimator, jsonTokenEstimator } from '@animalabs/context-manager';
import type { ContentBlock, ImageContent } from '@animalabs/membrane';
import { measureContent } from '../src/content-accounting.js';
import { buildContextCurve, type PanelAppRef } from '../src/web/panel-data.js';

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

// The panel really reads Chronicle's unresolved message window; only selection
// of the already-compiled window is a fixture. No model or archive reads occur.
async function curve(content: ContentBlock[][], selected: Array<{ participant: string; content: ContentBlock[] }>, sourceGroups?: number[][]) {
  const dir = mkdtempSync(join(tmpdir(), 'host-media-curve-'));
  const store = JsStore.openOrCreate({ path: join(dir, 'store') });
  MessageStore.register(store);
  const messages = new MessageStore(store);
  const ids = content.map(blocks => messages.append('user', blocks).id);
  const cm = {
    compileMetadata: async () => ({ messages: selected.map((entry, i) => ({
      ...entry,
      ...(sourceGroups ? { sourceMessageIds: sourceGroups[i].map(index => ids[index]) } : { sourceMessageId: ids[i] }),
    })) }),
    getMessageCount: () => messages.length(),
    getMessageWindow: (offset: number, limit: number, options: { resolveBlobs?: boolean }) => {
      expect(options.resolveBlobs).toBe(false);
      return messages.getWindow(offset, limit, options);
    },
    getStrategy: () => ({}),
    currentBranch: () => ({ name: 'main' }),
  };
  const app = {
    framework: { getAgent: () => ({ getContextManager: () => cm }), getAgentRuntimeSettings: () => ({ contextBudgetTokens: 123_456 }) },
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
        framework: { getAgent: () => ({ getContextManager: () => cm }) },
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
          framework: { getAgent: () => ({ getContextManager: () => cm }) },
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
    }], [[0, 1]]);
    expect(result.entries).toHaveLength(1);
    expect(result.entries[0]).toMatchObject({
      kind: 'raw', nImages: 1,
      rendered: defaultTokenEstimator(placeholder) + 907, rawCovered: 1638,
    });
  });

  test('normalization splits rendered parts without double charging their shared source history', async () => {
    const text = 'A tool observation';
    const result = await curve([[image('AA==', 731), { type: 'text', text }]], [
      { participant: 'user', content: [image('AA==', 731)] },
      { participant: 'root', content: [{ type: 'text', text }] },
    ], [[0], [0]]);
    const original = 731 + defaultTokenEstimator(text);
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
      const cm = {
        compileMetadata: async () => ({ messages: [{ participant: 'root', content: [{ type: 'text', text: summaryText }] }] }),
        getMessageCount: () => messages.length(),
        getMessageWindow: (offset: number, limit: number, options: { resolveBlobs?: boolean }) => {
          expect(options.resolveBlobs).toBe(false);
          return messages.getWindow(offset, limit, options);
        },
        getStrategy: () => ({ summaries: [
          { id: 'L1-a', level: 1, content: 'Earlier synopsis', sourceLevel: 0, sourceIds },
          { id: 'L2-a', level: 2, content: summaryText, sourceLevel: 1, sourceIds: ['L1-a'] },
        ] }),
        currentBranch: () => ({ name: 'main' }),
      };
      store.getBlob = () => { throw new Error('summary curve resolved archived media'); };
      const app = { framework: { getAgent: () => ({ getContextManager: () => cm }) }, recipe: { agent: {} } } as unknown as PanelAppRef;
      const result = await buildContextCurve(app, 'root') as { entries: Array<{ kind: string; rendered: number; rawCovered: number; nImages: number; msgCount: number }> };
      expect(result.entries).toHaveLength(1);
      expect(result.entries[0]).toMatchObject({
        kind: 'L2', rendered: defaultTokenEstimator(summaryText),
        rawCovered: 1600 + 2103 + defaultTokenEstimator('Tool observation'), nImages: 0, msgCount: 2,
      });
    } finally {
      store.close();
      rmSync(dir, { recursive: true, force: true });
    }
  });
});
