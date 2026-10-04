import { expect, test } from 'bun:test';
import { spawn, type ChildProcess } from 'node:child_process';
import { existsSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { connect, type Socket } from 'node:net';
import { tmpdir } from 'node:os';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { JsStore } from '@animalabs/chronicle';
import { ContextManager } from '@animalabs/context-manager';
import { SessionManager } from '../src/session-manager.js';

interface WireEvent {
  type: string;
  text?: string;
  style?: string;
  agentName?: string;
  tree?: { nodes: Array<{ name: string; phase: string }> };
}

function parseEvent(line: string): WireEvent {
  const value: unknown = JSON.parse(line);
  if (!value || typeof value !== 'object' || !('type' in value) || typeof value.type !== 'string') {
    throw new Error('Invalid headless event');
  }
  return {
    type: value.type,
    ...('text' in value && typeof value.text === 'string' ? { text: value.text } : {}),
    ...('style' in value && typeof value.style === 'string' ? { style: value.style } : {}),
    ...('agentName' in value && typeof value.agentName === 'string' ? { agentName: value.agentName } : {}),
    ...('tree' in value && value.tree && typeof value.tree === 'object' && 'nodes' in value.tree && Array.isArray(value.tree.nodes)
      ? { tree: value.tree as WireEvent['tree'] } : {}),
  };
}

async function waitFor(check: () => boolean, child: ChildProcess, timeout = 10_000): Promise<void> {
  const deadline = Date.now() + timeout;
  while (!check()) {
    if (child.exitCode !== null || child.signalCode !== null) throw new Error(`Host exited with ${child.exitCode ?? child.signalCode}`);
    if (Date.now() >= deadline) throw new Error('Headless scenario timed out');
    await Bun.sleep(10);
  }
}

test('failed session creation restores archive/checkpoints; concurrent switches cannot corrupt active selection', async () => {
  const dir = mkdtempSync(join(tmpdir(), 'headless-session-recovery-'));
  const sessions = new SessionManager(dir);
  const initial = sessions.createSession('Original');
  const broken = sessions.createSession('Broken');
  const next = sessions.createSession('Next');
  sessions.setActiveSession(initial.id);
  const marker = 'original-session-archive-anchor';
  const store = JsStore.openOrCreate({ path: sessions.getStorePath(initial.id) });
  try {
    const cm = await ContextManager.open({ store });
    try { cm.addMessage('user', [{ type: 'text', text: marker }]); }
    finally { await cm.close(); }
  } finally { store.close(); }
  // A real invalid Chronicle target fails the production factory, not a mocked switch callback.
  writeFileSync(sessions.getStorePath(broken.id), 'not a Chronicle directory');
  const recipe = join(dir, 'recipe.json');
  const extension = join(dir, 'observer-fault.mjs');
  // Register a throwing observer before the headless observers through the real WebUI's public binding.
  writeFileSync(extension, `
    import { WebUiModule } from ${JSON.stringify(new URL('../src/modules/web-ui-module.ts', import.meta.url).href)};
    const original = WebUiModule.prototype.setApp;
    const registered = new WeakSet();
    export function register() {
      WebUiModule.prototype.setApp = function(app) {
        original.call(this, app);
        if (registered.has(app)) return;
        registered.add(app);
        app.onFrameworkChanged(() => { throw new Error('expected-observer-rebind-failure'); });
      };
    }
  `);
  writeFileSync(recipe, JSON.stringify({
    name: 'Session recovery regression',
    agent: {
      name: 'session-smoke', provider: 'mock', model: 'mock-recovery',
      mock: { echoMode: false, defaultResponse: 'Offline recovery provider completed.' },
      systemPrompt: 'An isolated offline session recovery scenario.',
      strategy: { type: 'passthrough' },
    },
    modules: { webui: { host: '127.0.0.1', port: 0 }, subagents: false, lessons: false, retrieval: false, wake: false, workspace: false, identity: false, mcplAdmin: false },
    extensions: { observerFault: { kind: 'module', path: extension } },
  }));
  const index = resolve(dirname(fileURLToPath(import.meta.url)), '../src/index.ts');
  const env = { ...process.env, DATA_DIR: dir };
  delete env.MODEL;
  let child: ChildProcess | undefined;
  let socket: Socket | undefined;
  const events: WireEvent[] = [];
  let buffer = '';
  let diagnostics = '';
  try {
    child = spawn('bun', [index, recipe, '--headless'], { cwd: dir, env, stdio: ['ignore', 'ignore', 'pipe'] });
    child.stderr!.on('data', chunk => { diagnostics += chunk.toString(); });
    const process = child;
    await waitFor(() => existsSync(join(dir, 'ipc.sock')), process, 15_000);
    socket = connect(join(dir, 'ipc.sock'));
    socket.on('data', chunk => {
      buffer += chunk.toString();
      let end: number;
      while ((end = buffer.indexOf('\n')) >= 0) {
        const line = buffer.slice(0, end);
        buffer = buffer.slice(end + 1);
        if (line) events.push(parseEvent(line));
      }
    });
    await new Promise<void>((resolve, reject) => {
      socket!.once('connect', resolve);
      socket!.once('error', reject);
    });
    async function command(text: string, predicate: (event: WireEvent) => boolean): Promise<WireEvent[]> {
      const start = events.length;
      socket!.write(JSON.stringify({ type: 'command', command: text }) + '\n');
      await waitFor(() => events.slice(start).some(predicate), process);
      return events.slice(start);
    }
    const hasText = (needle: string) => (event: WireEvent) => event.type === 'command-output' && event.text?.includes(needle) === true;
    async function assertLiveTelemetry(label: string): Promise<void> {
      const start = events.length;
      socket!.write(JSON.stringify({ type: 'text', content: label }) + '\n');
      await waitFor(() => events.slice(start).some(event => event.type === 'inference:completed' && event.agentName === 'session-smoke'), process);
      expect(events.slice(start).some(event => event.type === 'inference:started' && event.agentName === 'session-smoke')).toBe(true);
      const snapshotStart = events.length;
      socket!.write(JSON.stringify({ type: 'describe', corrId: label }) + '\n');
      await waitFor(() => events.slice(snapshotStart).some(event => event.type === 'snapshot'), process);
      const snapshot = events.slice(snapshotStart).find(event => event.type === 'snapshot');
      expect(snapshot?.tree?.nodes.find(node => node.name === 'session-smoke')?.phase).toBe('done');
    }
    await command('/history', hasText(marker));
    await command('/checkpoint survives-failure', hasText('survives-failure'));
    const failure = await command(`/session switch ${broken.id}`, event => event.type === 'command-output' && event.style === 'error');
    expect(failure.some(event => event.text === 'Session switched.')).toBe(false);
    expect(sessions.getActiveSession()?.id).toBe(initial.id);
    await command('/history', hasText(marker));
    await command('/branches', hasText('survives-failure'));
    await assertLiveTelemetry('after-failed-switch');


    const start = events.length;
    socket.write([
      JSON.stringify({ type: 'command', command: `/session switch ${next.id}` }),
      JSON.stringify({ type: 'command', command: `/session switch ${initial.id}` }),
      '',
    ].join('\n'));
    await waitFor(() => events.slice(start).some(event => event.text === 'Session switched.')
      && events.slice(start).some(event => event.style === 'error'), process);
    expect(sessions.getActiveSession()?.id).toBe(next.id);
    expect(events.slice(start).filter(event => event.text === 'Session switched.')).toHaveLength(1);
    await assertLiveTelemetry('after-concurrent-switch');
    await command(`/session switch ${initial.id}`, event => event.text === 'Session switched.');
    await command('/history', hasText(marker));
    expect(sessions.getActiveSession()?.id).toBe(initial.id);
    socket.write(JSON.stringify({ type: 'shutdown', graceful: true }) + '\n');
    await new Promise<void>(resolve => process.once('exit', () => resolve()));
    expect(process.exitCode).toBe(0);
    expect(diagnostics.includes('expected-observer-rebind-failure')).toBe(true);
  } catch (error) {
    const log = join(dir, 'headless.log');
    throw new Error(`${String(error)}\n${existsSync(log) ? readFileSync(log, 'utf8').slice(-6000) : 'No headless log'}`, { cause: error });
  } finally {
    socket?.destroy();
    if (child && child.exitCode === null && child.signalCode === null) {
      const exited = new Promise<void>(resolve => child!.once('exit', () => resolve()));
      child.kill('SIGKILL');
      await exited;
    }
    rmSync(dir, { recursive: true, force: true });
  }
}, 45_000);
