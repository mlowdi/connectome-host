import { describe, expect, test } from 'bun:test';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { AgentFramework } from '@animalabs/agent-framework';
import type { Module, ToolCall } from '@animalabs/agent-framework';
import { PassthroughStrategy, jsonTokenEstimator } from '@animalabs/context-manager';
import { Membrane, MockAdapter, NativeFormatter } from '@animalabs/membrane';
import type { ContentBlock, ImageContent } from '@animalabs/membrane';
import { SubagentModule, buildIntentionFramedForkResult } from '../src/modules/subagent-module.js';

async function harness(maxPromptTokens: number) {
  const dir = mkdtempSync(join(tmpdir(), 'host-media-preflight-'));
  const membrane = new Membrane(new MockAdapter({ defaultResponse: 'not used' }), { formatter: new NativeFormatter() });
  const subagent = new SubagentModule({ parentAgentName: 'parent', defaultModel: 'mock', maxPromptTokens, maxRetries: 0 });
  const framework = await AgentFramework.create({
    storePath: join(dir, 'store'), membrane,
    agents: [{ name: 'parent', model: 'mock', systemPrompt: '', strategy: new PassthroughStrategy() }],
    modules: [subagent as unknown as Module],
  });
  subagent.setFramework(framework);
  const create = framework.createEphemeralAgent.bind(framework);
  framework.createEphemeralAgent = config => create({ ...config, strategy: new PassthroughStrategy() });
  framework.getAllTools = () => [];
  let reachedRun = 0;
  let inherited: Array<{ participant: string; content: ContentBlock[] }> = [];
  // Stop only at the actual run boundary; inheritance and preflight are real.
  framework.runEphemeralToCompletion = async (_agent, cm) => {
    reachedRun++;
    inherited = (await cm.compile()).messages;
    return { speech: 'admitted', toolCallsCount: 0 };
  };
  return {
    framework, subagent,
    getRunCount: () => reachedRun,
    getInherited: () => inherited,
    cleanup: async () => {
      await framework.stop();
      rmSync(dir, { recursive: true, force: true });
    },
  };
}

const FORK_NAME = 'image-probe';
const FORK_TASK = 'inspect this image';
// 50 system + 3 * 50 message envelopes; canonical tool-use overhead stays 20.
const FORK_ENVELOPES = 200 + 20 + jsonTokenEstimator(JSON.stringify({ name: FORK_NAME, task: FORK_TASK })) +
  jsonTokenEstimator(buildIntentionFramedForkResult(FORK_NAME, FORK_TASK, 1, 3));

function forkCall(): ToolCall {
  return { id: 'fork-call', name: 'fork', callerAgentName: 'parent', input: { name: FORK_NAME, task: FORK_TASK, systemPrompt: '', sync: true, timeoutMs: 1000 } };
}

function media(data: string, tokenEstimate?: number): ImageContent {
  return { type: 'image', source: { type: 'base64', mediaType: 'image/png', data }, ...(tokenEstimate !== undefined ? { tokenEstimate } : {}) };
}

describe('subagent image prompt admission at the consumer boundary', () => {
  test('top-level and nested images pass at the exact prompt limit regardless of base64 size', async () => {
    for (const nested of [false, true]) {
      for (const data of ['AA==', 'A'.repeat(1_000_000)]) {
        const h = await harness(FORK_ENVELOPES + 1600);
        try {
          const image = media(data);
          const block: ContentBlock = nested ? { type: 'tool_result', toolUseId: 'photo', content: [image] } : image;
          h.framework.getAgent('parent')!.getContextManager().addMessage('user', [block]);
          const result = await h.subagent.handleToolCall(forkCall());
          expect(result.success).toBe(true);
          expect(h.getRunCount()).toBe(1);
          const inherited = h.getInherited()[0].content[0];
          if (nested) {
            if (inherited.type !== 'tool_result' || !Array.isArray(inherited.content)) throw new Error('nested media lost its typed tool-result container');
            expect(inherited.content[0]).toMatchObject({ type: 'image', source: { data } });
          } else {
            expect(inherited).toMatchObject({ type: 'image', source: { data } });
          }
        } finally {
          await h.cleanup();
        }
      }
    }
  });

  test('explicit image estimates admit exactly at the boundary and reject one token over it', async () => {
    for (const stamp of [731, 732]) {
      const h = await harness(FORK_ENVELOPES + 731);
      try {
        h.framework.getAgent('parent')!.getContextManager().addMessage('user', [media('A'.repeat(1_000_000), stamp)]);
        const result = await h.subagent.handleToolCall(forkCall());
        expect(result.success).toBe(stamp === 731);
        expect(h.getRunCount()).toBe(stamp === 731 ? 1 : 0);
        if (stamp === 732) expect(result.error).toContain('Prompt too large');
      } finally {
        await h.cleanup();
      }
    }
  });

  test('repeated identical image turns remain separate inherited history with intact native carriers', async () => {
    const h = await harness(FORK_ENVELOPES + 50 + 3200);
    try {
      const image = media('AA==');
      image.rawItem = { type: 'native-image-carrier', original: 'untouched' };
      const cm = h.framework.getAgent('parent')!.getContextManager();
      cm.addMessage('user', [image]);
      cm.addMessage('user', [image]);
      const result = await h.subagent.handleToolCall(forkCall());
      expect(result.success).toBe(true);
      const turns = h.getInherited().filter(message => message.content.some(block => block.type === 'image'));
      expect(turns).toHaveLength(2);
      expect(turns[0].content[0]).toEqual(image);
      expect(turns[1].content[0]).toEqual(image);
      expect(image.rawItem).toEqual({ type: 'native-image-carrier', original: 'untouched' });
    } finally {
      await h.cleanup();
    }
  });

  test('spawn rejects oversized ordinary task text before execution while admitting the small task', async () => {
    for (const task of ['inspect this image', 'ordinary task text '.repeat(2000)]) {
      const h = await harness(2000);
      try {
        const result = await h.subagent.handleToolCall({
          id: 'spawn-call', name: 'spawn', callerAgentName: 'parent',
          input: { name: 'text-probe', systemPrompt: 'inspect', task, sync: true, timeoutMs: 1000 },
        });
        const admitted = task === 'inspect this image';
        expect(result.success).toBe(admitted);
        expect(h.getRunCount()).toBe(admitted ? 1 : 0);
      } finally {
        await h.cleanup();
      }
    }
  });
});
