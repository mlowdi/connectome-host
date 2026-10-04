import { test, expect } from 'bun:test';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { Agent, AgentFramework } from '@animalabs/agent-framework';
import type { ModuleContext } from '@animalabs/agent-framework';
import { ContextManager, AutobiographicalStrategy, MessageStore } from '@animalabs/context-manager';
import { Membrane, MockAdapter, NativeFormatter } from '@animalabs/membrane';
import type { ContentBlock } from '@animalabs/membrane';
import { measureContent } from '../src/content-accounting.js';
import { buildContextCurve, buildMediaBlock, type PanelAppRef } from '../src/web/panel-data.js';
import { WebUiModule, __getSharedServerPortForTests, __resetSharedServerForTests } from '../src/modules/web-ui-module.js';

const PNG = 'iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNkYPhfDwAChwGA60e6kgAAAABJRU5ErkJggg==';
const strategy = (adaptiveResolution = false) => new AutobiographicalStrategy({ headWindowTokens: 0, recentWindowTokens: 100000,
  targetChunkTokens: 100000, adaptiveResolution, autoTickOnNewMessage: false,
  maxLiveImages: 0, maxLiveImageBytes: 0, imageStripDepthTokens: 0 });
const generatedContent: ContentBlock[] = [{ type: 'text', text: 'before' },
  { type: 'generated_image', data: PNG, mimeType: 'image/png', isPreview: true, tokenEstimate: 0 },
  { type: 'text', text: 'between' }, { type: 'generated_image', data: PNG, mimeType: 'image/png', isPreview: false, tokenEstimate: 731 },
  { type: 'text', text: 'after' }];

test('falsifier: actual generated host refs include original shards and survive archive/framework/HTTP fixture restart', async () => {
  const dir = mkdtempSync(join(tmpdir(), 'host-generated-serving-'));
  const storePath = join(dir, 'store');
  let generatedId = '';
  let nestedId = '';
  let shardIds: string[] = [];
  try {
    for (const reopened of [false, true]) {
      const framework = await AgentFramework.create({ storePath, membrane: new Membrane(new MockAdapter()),
        agents: [{ name: 'fixture', model: 'unused-no-model', systemPrompt: 'fixture', strategy: strategy() }], modules: [], syncIntervalMs: 0 });
      const module = new WebUiModule({ host: '127.0.0.1', port: 0 });
      let socket: WebSocket | undefined;
      try {
        const cm = framework.getAgent('fixture')!.getContextManager();
        if (!reopened) {
          generatedId = cm.addMessage('fixture', generatedContent);
          cm.addMessage('fixture', [{ type: 'tool_use', id: 'outer', name: 'fixture', input: {} }]);
          nestedId = cm.addMessage('user', [{ type: 'tool_result', toolUseId: 'outer', content: [
            { type: 'text', text: 'nested before' }, { type: 'tool_result', toolUseId: 'inner', content: [
              { type: 'generated_image', data: PNG, mimeType: 'image/webp', tokenEstimate: 907 }, { type: 'text', text: 'nested after' }],
            },
          ] }]);
          const messages = new MessageStore(cm.getStore());
          const shards: ContentBlock[][] = [[{ type: 'text', text: 'be' }], [
            { type: 'text', text: 'fore' }, { type: 'generated_image', data: PNG, mimeType: 'image/png', tokenEstimate: 731 }], [
            { type: 'tool_result', toolUseId: 'sharded', content: [{ type: 'tool_result', toolUseId: 'inner', content: [
              { type: 'generated_image', data: PNG, mimeType: 'image/png', tokenEstimate: 907 }],
            }] }], [{ type: 'text', text: 'af' }, { type: 'text', text: 'ter' }]];
          shardIds = shards.map((content, shardIndex) => messages.append('fixture', content, undefined, undefined,
            { bodyGroupId: 'generated-fixture-shards', shardIndex }).id);
        }
        await module.start({} as ModuleContext);
        const app = { framework, recipe: { name: 'fixture', agent: { name: 'fixture', contextBudgetTokens: 100000, maxTokens: 64 } },
          sessionManager: { getActiveSession: () => ({ id: 'synthetic-fixture', name: 'synthetic fixture', manuallyNamed: true }) } };
        module.setApp(app as never);
        const port = __getSharedServerPortForTests()!;
        const welcome = await new Promise<{ messages: Array<{ id: string; blocks: Array<{ kind: string; ref?: string; mediaType?: string }> }> }>((resolve, reject) => {
          socket = new WebSocket(`ws://127.0.0.1:${port}/ws`);
          socket.onerror = () => reject(new Error('fixture WebSocket failed'));
          socket.onmessage = event => {
            const message = JSON.parse(String(event.data));
            if (message.type === 'welcome') resolve(message);
            if (message.type === 'error') reject(new Error(message.message));
          };
        });
        const generatedEntry = welcome.messages.find(message => message.id === generatedId)!;
        expect(generatedEntry.blocks.filter(block => block.kind === 'media').map(block => block.ref)).toEqual([
          `${encodeURIComponent(generatedId)}/1`, `${encodeURIComponent(generatedId)}/3`,
        ]);
        const nestedEntry = welcome.messages.find(message => message.id === nestedId)!;
        expect(nestedEntry.blocks.filter(block => block.kind === 'media').map(block => block.ref)).toEqual([`${encodeURIComponent(nestedId)}/0.1.0`]);
        const shardRefs = [`${encodeURIComponent(shardIds[1])}/1`, `${encodeURIComponent(shardIds[2])}/0.0.0`];
        const shardedEntry = welcome.messages.find(message => message.id === shardIds[0])!;
        expect(shardedEntry.blocks.filter(block => block.kind === 'media').map(block => block.ref)).toEqual(shardRefs);
        expect(shardedEntry.blocks.filter(block => block.kind === 'text').flatMap(block => 'text' in block && typeof block.text === 'string' ? [block.text] : [])).toEqual(['before', 'after']);
        expect(JSON.stringify(welcome)).not.toContain(PNG);
        const page = await new Promise<{ entries: typeof welcome.messages }>((resolve, reject) => {
          socket!.onmessage = event => {
            const message = JSON.parse(String(event.data));
            if (message.type === 'history-page') resolve(message);
            if (message.type === 'error') reject(new Error(message.message));
          };
          socket!.send(JSON.stringify({ type: 'request-history', corrId: 'generated-shards', beforeIndex: cm.getMessageCount(), limit: cm.getMessageCount() }));
        });
        expect(page.entries.find(message => message.id === shardIds[0])!.blocks.filter(block => block.kind === 'media').map(block => block.ref)).toEqual(shardRefs);
        expect(JSON.stringify(page)).not.toContain(PNG);
        for (const ref of [`${encodeURIComponent(generatedId)}/1`, `${encodeURIComponent(generatedId)}/3`, `${encodeURIComponent(nestedId)}/0.1.0`, ...shardRefs]) {
          const response = await fetch(`http://127.0.0.1:${port}/media/${ref}`);
          expect(response.status).toBe(200);
          expect(response.headers.get('content-type')).toBe('image/png');
          expect(response.headers.get('x-content-type-options')).toBe('nosniff');
          expect(Buffer.from(await response.arrayBuffer()).equals(Buffer.from(PNG, 'base64'))).toBe(true);
        }
        const bytes = buildMediaBlock(app as unknown as PanelAppRef, 'fixture', { messageId: nestedId, path: '0.1.0' });
        expect(bytes).toEqual({ mediaType: 'image/png', base64: PNG });
      } finally { socket?.close(); await module.stop(); await __resetSharedServerForTests(); await framework.stop(); }
    }
  } finally { rmSync(dir, { recursive: true, force: true }); }
}, 20000);

test('generated host raw/ref/metadata counts preserve explicit zero and calibrated curve parity without blob I/O or writes', async () => {
  const dir = mkdtempSync(join(tmpdir(), 'host-generated-accounting-'));
  const cm = await ContextManager.open({ path: join(dir, 'store'), namespace: 'generated-fixture', strategy: strategy(true) });
  try {
    cm.addMessage('root', generatedContent);
    cm.addMessage('root', [{ type: 'tool_use', id: 'outer', name: 'fixture', input: {} }]);
    cm.addMessage('user', [{ type: 'tool_result', toolUseId: 'outer', content: [{ type: 'tool_result', toolUseId: 'inner', content: [
      { type: 'generated_image', data: PNG, mimeType: 'image/png', tokenEstimate: 907 }, { type: 'text', text: 'caption' }],
    }] }]);
    const store = cm.getStore();
    store.setStateJson('generated-fixture/autobio:calibration', { multiplier: 1.75 });
    const original = cm.getAllMessages().flatMap(message => message.content);
    const unresolved = cm.getMessageWindow(0, cm.getMessageCount(), { resolveBlobs: false }).messages.flatMap(message => message.content);
    expect(measureContent(original)).toEqual(measureContent(unresolved));
    expect(measureContent(original).nImages).toBe(3);
    const membrane = new Membrane(new MockAdapter(), { formatter: new NativeFormatter() });
    membrane.complete = async () => { throw new Error('pure curve must not infer'); };
    const agent = new Agent({ name: 'root', model: 'unused-no-model', systemPrompt: '' }, cm, membrane);
    const app = { framework: { getAgent: () => agent }, recipe: { agent: { name: 'root', contextBudgetTokens: 100000, maxTokens: 64 } } } as unknown as PanelAppRef;
    const getBlob = store.getBlob.bind(store);
    let reads = 0;
    store.getBlob = () => { reads++; throw new Error('generated metadata curve must not hydrate'); };
    const before = store.currentSequence();
    const metadata = await cm.compileMetadata({ maxTokens: 100000, reserveForResponse: 64 });
    const curve = await buildContextCurve(app, 'root') as { entries: Array<{ nImages: number; rendered: number }>; totals: { rendered: number; rawCovered: number } };
    expect(metadata.tokenCalibration).toBe(1.75);
    expect(curve.entries.reduce((sum, entry) => sum + entry.nImages, 0)).toBe(3);
    expect(curve.totals.rendered).toBe(metadata.estimatedTokens);
    expect(curve.entries.reduce((sum, entry) => sum + entry.rendered, 0)).toBe(metadata.estimatedTokens);
    expect(curve.totals.rawCovered).toBe(cm.getMessageWindow(0, cm.getMessageCount(), { resolveBlobs: false }).messages
      .reduce((sum, message) => sum + cm.estimateContentTokens(message.content, 1.75), 0));
    expect(reads).toBe(0);
    expect(store.currentSequence()).toBe(before);
    expect(JSON.stringify(metadata)).not.toContain(PNG);
    store.getBlob = getBlob;
  } finally { cm.close(); rmSync(dir, { recursive: true, force: true }); }
});
