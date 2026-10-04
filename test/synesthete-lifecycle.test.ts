import { describe, expect, test, setSystemTime } from 'bun:test';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { AgentFramework } from '@animalabs/agent-framework';
import { PassthroughStrategy } from '@animalabs/context-manager';
import { Membrane, MembraneError, NativeFormatter } from '@animalabs/membrane';
import type { ProviderAdapter, ProviderRequest, ProviderResponse } from '@animalabs/membrane';
import { SessionManager } from '../src/session-manager.js';
import { generateSessionName, setupSynesthete, FleetActivitySummaries } from '../src/synesthete.js';

/** Records the real Membrane adapter boundary and rejects foreign model ids. */
class AuxiliaryRecorder implements ProviderAdapter {
  readonly calls: Array<{ request: ProviderRequest; resolve: (response: ProviderResponse) => void; reject: (error: Error) => void }> = [];
  readonly usageCacheConvention = 'cache-excluded' as const;
  constructor(readonly name: string, private readonly compatibleModel: string) {}

  supportsModel(model: string): boolean {
    return model === this.compatibleModel;
  }

  complete(request: ProviderRequest): Promise<ProviderResponse> {
    if (!this.supportsModel(request.model)) throw new Error(`foreign model ${request.model} on ${this.name}`);
    const { promise, resolve, reject } = Promise.withResolvers<ProviderResponse>();
    this.calls.push({ request, resolve, reject });
    return promise;
  }

  async stream(): Promise<ProviderResponse> {
    throw new Error('auxiliary regression unexpectedly streamed inference');
  }

  answer(index: number, text: string): void {
    this.calls[index].resolve({ content: [{ type: 'text', text }], stopReason: 'end_turn', usage: { inputTokens: 12, outputTokens: 4 } });
  }
}

async function flush(): Promise<void> {
  const { promise, resolve } = Promise.withResolvers<void>();
  setImmediate(resolve);
  await promise;
}

async function harness(provider = 'openai-codex', model = 'gpt-5.4-codex') {
  const dir = mkdtempSync(join(tmpdir(), 'host-auxiliary-'));
  const recorder = new AuxiliaryRecorder(provider, model);
  const membrane = new Membrane(recorder, { formatter: new NativeFormatter() });
  const sessions = new SessionManager(dir);
  const initial = sessions.createSession();
  sessions.setActiveSession(initial.id);
  const frameworks: AgentFramework[] = [];
  async function makeFramework() {
    const framework = await AgentFramework.create({
      storePath: join(dir, `framework-${frameworks.length}`), membrane, modules: [],
      agents: [{ name: 'resolved-root', model, systemPrompt: 'No agent inference in this test', strategy: new PassthroughStrategy() }],
    });
    frameworks.push(framework);
    return framework;
  }
  const app = {
    framework: await makeFramework(), membrane, sessionManager: sessions,
    agentName: 'resolved-root', recipe: { sessionNaming: { examples: ['Follow The Map'] } }, userMessageCount: 0,
  };
  function message(text: string, source = 'external-message', framework = app.framework): void {
    const id = framework.getAgent(app.agentName)!.getContextManager().addMessage('user', [{ type: 'text', text }]);
    // Emit the actual framework trace deterministically without starting its
    // event loop or scheduling any agent inference.
    const traceSource = framework as unknown as { emitTrace: (event: { type: 'message:added'; messageId: string; source: string }) => void };
    traceSource.emitTrace({ type: 'message:added', messageId: id, source });
  }
  return {
    app, recorder, sessions, initial, makeFramework, message,
    cleanup: async () => {
      for (const framework of frameworks) await framework.stop();
      rmSync(dir, { recursive: true, force: true });
    },
  };
}

describe('session auto-naming on the active compatible transport', () => {
  test('third external message names the session using the actual agent model, not a provider default', async () => {
    for (const [provider, model] of [
      ['openai-codex', 'gpt-5.4-codex'],
      ['bedrock', 'us.anthropic.claude-sonnet-4-5-20250929-v1:0'],
      ['openrouter', 'google/gemini-2.5-pro'],
    ]) {
      const h = await harness(provider, model);
      try {
        setupSynesthete(h.app);
        h.message('not an operator message', 'routing-notice');
        h.message('First discussion');
        h.message('Second discussion');
        await flush();
        expect(h.recorder.calls).toHaveLength(0);
        h.message('Third discussion');
        await flush();
        expect(h.recorder.calls).toHaveLength(1);
        expect(h.recorder.calls[0].request.model).toBe(model);
        expect(JSON.stringify(h.recorder.calls[0].request.system)).toContain('Follow The Map');
        h.recorder.answer(0, 'Memory Budget Pathways');
        await flush();
        expect(h.sessions.getActiveSession()).toMatchObject({ name: 'Memory Budget Pathways', manuallyNamed: false });
        h.message('Fourth discussion');
        await flush();
        expect(h.recorder.calls).toHaveLength(1);
      } finally {
        await h.cleanup();
      }
    }
  });

  test('manual naming before or during a pending auto-name always wins', async () => {
    for (const during of [false, true]) {
      const h = await harness();
      try {
        setupSynesthete(h.app);
        if (!during) h.sessions.renameSession(h.initial.id, 'Operator Chosen Name');
        for (let i = 0; i < 3; i++) h.message(`Discussion ${i}`);
        await flush();
        expect(h.recorder.calls).toHaveLength(during ? 1 : 0);
        if (during) {
          h.sessions.renameSession(h.initial.id, 'Operator Chosen Name');
          h.recorder.answer(0, 'Late Automatic Title');
          await flush();
        }
        expect(h.sessions.getActiveSession()).toMatchObject({ name: 'Operator Chosen Name', manuallyNamed: true });
      } finally {
        await h.cleanup();
      }
    }
  });

  test('old framework traces and in-flight names cannot write across session switches', async () => {
    const h = await harness();
    try {
      setupSynesthete(h.app);
      for (let i = 0; i < 3; i++) h.message(`Original discussion ${i}`);
      await flush();
      const oldFramework = h.app.framework;
      const next = h.sessions.createSession();
      h.sessions.setActiveSession(next.id);
      h.app.framework = await h.makeFramework();
      h.app.userMessageCount = 0;
      setupSynesthete(h.app);
      h.message('stale old framework event', 'external-message', oldFramework);
      expect(h.app.userMessageCount).toBe(0);
      for (let i = 0; i < 3; i++) h.message(`New discussion ${i}`);
      await flush();
      expect(h.recorder.calls).toHaveLength(2);
      h.recorder.answer(0, 'Old Session Late Name');
      await flush();
      expect(h.sessions.findSession(h.initial.id)?.name).toBe(h.initial.name);
      expect(h.sessions.getActiveSession()?.name).toBe(next.name);
      h.recorder.answer(1, 'New Session Timely Name');
      await flush();
      expect(h.sessions.getActiveSession()?.name).toBe('New Session Timely Name');
    } finally {
      await h.cleanup();
    }
  });

  test('creating an inactive session does not invalidate the active session naming request', async () => {
    const h = await harness();
    try {
      setupSynesthete(h.app);
      for (let i = 0; i < 3; i++) h.message(`Active discussion ${i}`);
      await flush();
      const inactive = h.sessions.createSession();
      h.recorder.answer(0, 'Still Active Session Name');
      await flush();
      expect(h.sessions.getActiveSession()).toMatchObject({ id: h.initial.id, name: 'Still Active Session Name' });
      expect(h.sessions.findSession(inactive.id)?.name).toBe(inactive.name);
      expect(h.recorder.calls).toHaveLength(1);
    } finally {
      await h.cleanup();
    }
  });

  test('returning to the same session id with a new framework does not revive a stale name', async () => {
    const h = await harness();
    try {
      setupSynesthete(h.app);
      for (let i = 0; i < 3; i++) h.message(`Discussion ${i}`);
      await flush();
      const intermediate = h.sessions.createSession();
      h.sessions.setActiveSession(intermediate.id);
      h.sessions.setActiveSession(h.initial.id);
      h.app.framework = await h.makeFramework();
      h.app.userMessageCount = 0;
      setupSynesthete(h.app);
      h.recorder.answer(0, 'Stale Revisited Title');
      await flush();
      expect(h.sessions.getActiveSession()?.name).toBe(h.initial.name);
      expect(h.app.userMessageCount).toBe(0);
    } finally {
      await h.cleanup();
    }
  });

  test('failed or malformed names do not create a fake title or another attempt', async () => {
    for (const response of [null, '', 'Too\nmany\nlines', 'x'.repeat(61)]) {
      const h = await harness();
      try {
        setupSynesthete(h.app);
        for (let i = 0; i < 3; i++) h.message(`Discussion ${i}`);
        await flush();
        if (response === null) h.recorder.calls[0].reject(new Error('bounded provider failure'));
        else h.recorder.answer(0, response);
        await flush();
        h.message('More discussion');
        await flush();
        expect(h.sessions.getActiveSession()?.name).toBe(h.initial.name);
        expect(h.sessions.getActiveSession()?.manuallyNamed).toBe(false);
        expect(h.recorder.calls).toHaveLength(1);
      } finally {
        await h.cleanup();
      }
    }
  });

  test('naming excerpts never resolve the historical image archive', async () => {
    const h = await harness();
    try {
      const cm = h.app.framework.getAgent(h.app.agentName)!.getContextManager();
      cm.addMessage('user', [{ type: 'image', source: { type: 'base64', mediaType: 'image/png', data: 'AAAA' } }]);
      const readWindow = cm.getMessageWindow.bind(cm);
      cm.getMessageWindow = (offset, limit, options) => {
        expect(options?.resolveBlobs).toBe(false);
        expect(limit).toBeLessThanOrEqual(16);
        return readWindow(offset, limit, options);
      };
      cm.queryMessages = () => { throw new Error('naming queried and resolved full history'); };
      setupSynesthete(h.app);
      for (let i = 0; i < 3; i++) h.message(`Text discussion ${i}`);
      await flush();
      expect(h.recorder.calls).toHaveLength(1);
      expect(JSON.stringify(h.recorder.calls[0].request.messages)).not.toContain('AAAA');
      h.recorder.answer(0, 'Text Only Naming');
      await flush();
      expect(h.sessions.getActiveSession()?.name).toBe('Text Only Naming');
    } finally {
      await h.cleanup();
    }
  });
});

describe('TUI fleet activity summary lifecycle', () => {
  test('uses the active root model and requests again only for meaningful new activity', async () => {
    const h = await harness();
    try {
      const summaries = new FleetActivitySummaries(h.app, h.app.agentName);
      summaries.append('child', 'working '.repeat(1500));
      const first = summaries.update('child');
      await flush();
      expect(h.recorder.calls[0].request.model).toBe('gpt-5.4-codex');
      expect(JSON.stringify(h.recorder.calls[0].request.messages).length).toBeLessThan(11_000);
      h.recorder.answer(0, 'Tracing the concrete archive accounting seam');
      expect(await first).toBe(true);
      expect(summaries.get('child')).toBe('Tracing the concrete archive accounting seam');
      summaries.append('child', 'X'.repeat(1999));
      expect(await summaries.update('child')).toBe(false);
      expect(h.recorder.calls).toHaveLength(1);
      summaries.append('child', 'Y');
      const second = summaries.update('child');
      await flush();
      expect(h.recorder.calls).toHaveLength(2);
      h.recorder.answer(1, 'Completed the nested image accounting correction');
      expect(await second).toBe(true);
      expect(summaries.get('child')).toBe('Completed the nested image accounting correction');
    } finally {
      await h.cleanup();
    }
  });

  test('external switches discard old caller transcripts before a new-session request can start', async () => {
    for (const fail of [false, true]) {
      const h = await harness();
      try {
        const summaries = new FleetActivitySummaries(h.app, h.app.agentName);
        summaries.append('child', 'old activity '.repeat(100));
        const old = summaries.update('child');
        await flush();
        const next = h.sessions.createSession();
        h.sessions.setActiveSession(next.id);
        // No reset, and no new append: a TUI poll must not resend old activity.
        expect(await summaries.update('child')).toBe(false);
        expect([...summaries.agentNames()]).toEqual([]);
        expect(h.recorder.calls).toHaveLength(1);
        summaries.append('child', 'fresh activity '.repeat(100));
        const fresh = summaries.update('child');
        await flush();
        expect(h.recorder.calls).toHaveLength(2);
        expect(JSON.stringify(h.recorder.calls[1].request.messages)).not.toContain('old activity');
        if (fail) h.recorder.calls[0].reject(new Error('old-session failure'));
        else h.recorder.answer(0, 'Stale old session activity');
        expect(await old).toBe(false);
        expect(summaries.get('child')).toBeUndefined();
        expect(summaries.isPending('child')).toBe(true);
        summaries.append('child', 'additional activity '.repeat(200));
        expect(await summaries.update('child')).toBe(false);
        expect(h.recorder.calls).toHaveLength(2);
        h.recorder.answer(1, 'Current session activity and no stale bookkeeping');
        expect(await fresh).toBe(true);
        expect(summaries.get('child')).toBe('Current session activity and no stale bookkeeping');
        expect(summaries.isPending('child')).toBe(false);
      } finally {
        await h.cleanup();
      }
    }
  });

  test('reset invalidates transcripts and pending calls even when the framework and session id are unchanged', async () => {
    const h = await harness();
    try {
      const summaries = new FleetActivitySummaries(h.app, h.app.agentName);
      summaries.append('root', 'old activity '.repeat(100));
      const old = summaries.update('root');
      await flush();
      summaries.reset();
      expect(await summaries.update('root')).toBe(false);
      summaries.append('root', 'new activity '.repeat(100));
      const fresh = summaries.update('root');
      await flush();
      h.recorder.answer(0, 'Late result before reset');
      expect(await old).toBe(false);
      expect(summaries.isPending('root')).toBe(true);
      h.recorder.answer(1, 'Only the post reset activity survives');
      expect(await fresh).toBe(true);
      expect(summaries.get('root')).toBe('Only the post reset activity survives');
    } finally {
      await h.cleanup();
    }
  });

  test('polling a failed unchanged transcript never becomes an automatic retry loop', async () => {
    const h = await harness();
    try {
      const summaries = new FleetActivitySummaries(h.app, h.app.agentName);
      summaries.append('child', 'failed activity '.repeat(100));
      const attempt = summaries.update('child');
      await flush();
      h.recorder.calls[0].reject(new Error('bounded failure'));
      expect(await attempt).toBe(false);
      setSystemTime(Date.now() + 60_000);
      expect(await summaries.update('child')).toBe(false);
      expect(h.recorder.calls).toHaveLength(1);
      summaries.append('child', 'new activity '.repeat(300));
      const earned = summaries.update('child');
      await flush();
      expect(h.recorder.calls).toHaveLength(2);
      h.recorder.answer(1, 'New activity earned a separate summary request');
      expect(await earned).toBe(true);
    } finally {
      setSystemTime();
      await h.cleanup();
    }
  });

  test('default Membrane rate-limit and overloaded schedules are disabled per auxiliary call', async () => {
    for (const status of [429, 529]) {
      const h = await harness();
      try {
        const error = new MembraneError({
          type: status === 429 ? 'rate_limit' : 'server', httpStatus: status,
          message: status === 429 ? 'rate_limit_error' : 'overloaded_error', retryable: true,
        });
        const naming = generateSessionName(h.app.membrane, 'A real naming request', 'gpt-5.4-codex');
        await flush();
        h.recorder.calls[0].reject(error);
        expect(await naming).toBeNull();
        expect(h.recorder.calls).toHaveLength(1);
        const summaries = new FleetActivitySummaries(h.app, h.app.agentName);
        summaries.append('child', 'A real activity stream '.repeat(100));
        const summary = summaries.update('child');
        await flush();
        h.recorder.calls[1].reject(error);
        expect(await summary).toBe(false);
        expect(h.recorder.calls).toHaveLength(2);
        expect(h.sessions.getActiveSession()?.name).toBe(h.initial.name);
        expect(summaries.get('child')).toBeUndefined();
      } finally {
        await h.cleanup();
      }
    }
  }, 2000);
});
