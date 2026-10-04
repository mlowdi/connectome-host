/** Session naming and fleet activity summaries on the host's active transport. */

import type { AgentFramework } from '@animalabs/agent-framework';
import type { Membrane } from '@animalabs/membrane';
import type { SessionManager } from './session-manager.js';

interface AuxiliaryApp {
  framework: AgentFramework;
  membrane: Membrane;
  sessionManager: Pick<SessionManager, 'getActiveSession' | 'renameSession'>;
}

interface SynestheteApp extends AuxiliaryApp {
  agentName: string;
  recipe: { sessionNaming?: { examples?: string[] } };
  userMessageCount: number;
}

const DEFAULT_EXAMPLES = [
  'Context Window Budgeting',
  'Agent Memory Architecture',
  'Pipeline Debug Session',
  'Schema Migration Plan',
  'API Integration Review',
];

function buildNamingPrompt(examples?: string[]): string {
  const exampleList = (examples ?? DEFAULT_EXAMPLES)
    .map(e => `- "${e}"`)
    .join('\n');

  return `You are a session naming assistant. Given a brief summary of a conversation, generate a short, evocative name (2-4 words) that captures its essence. The name should be memorable and descriptive, like a chapter title.

Examples of good names:
${exampleList}

Respond with ONLY the name, nothing else. No quotes, no explanation.`;
}

/** Generate a name using a model already configured on this Membrane's transport. */
export async function generateSessionName(
  membrane: Membrane,
  conversationSummary: string,
  model: string,
  examples?: string[],
): Promise<string | null> {
  try {
    const response = await membrane.complete({
      messages: [{ participant: 'user', content: [{ type: 'text', text: conversationSummary }] }],
      system: buildNamingPrompt(examples),
      config: { model, maxTokens: 30, temperature: 0.8 },
    }, { retry: false });

    const text = response.content
      .filter((b): b is { type: 'text'; text: string } => b.type === 'text')
      .map(b => b.text)
      .join('')
      .trim();

    if (!text || text.length > 60 || text.includes('\n')) return null;
    return text;
  } catch {
    return null;
  }
}

/** One best-effort naming attempt on the third operator message of this session. */
export function setupSynesthete(app: SynestheteApp): void {
  const framework = app.framework;
  const sessionId = app.sessionManager.getActiveSession()?.id;
  if (!sessionId) return;

  framework.onTrace((event) => {
    if (event.type !== 'message:added' || event.source !== 'external-message') return;
    // Old frameworks can still emit their final traces during session teardown.
    if (app.framework !== framework || app.sessionManager.getActiveSession()?.id !== sessionId) return;
    app.userMessageCount++;
    if (app.userMessageCount !== 3) return;

    const session = app.sessionManager.getActiveSession();
    if (!session || session.manuallyNamed) return;
    const agent = framework.getAgent(app.agentName);
    const cm = agent?.getContextManager();
    if (!cm || !agent?.model) return;

    // Only six short text excerpts are needed. Never resolve a session's media
    // archive merely to name it, nor materialize the full history in one query.
    const excerpts: string[] = [];
    const count = cm.getMessageCount();
    for (let offset = 0; offset < count && excerpts.length < 6; offset += 16) {
      const { messages } = cm.getMessageWindow(offset, Math.min(16, count - offset), { resolveBlobs: false });
      for (const message of messages) {
        let text = '';
        let hasText = false;
        for (const block of message.content) {
          if (block.type !== 'text') continue;
          hasText = true;
          if (text.length < 200) text += (text ? ' ' : '') + block.text.slice(0, 200 - text.length);
        }
        if (hasText) excerpts.push(`${message.participant}: ${text.slice(0, 200)}`);
        if (excerpts.length === 6) break;
      }
    }
    if (!excerpts.length) return;

    void generateSessionName(app.membrane, excerpts.join('\n'), agent.model, app.recipe.sessionNaming?.examples)
      .then(name => {
        const current = app.sessionManager.getActiveSession();
        if (!name || app.framework !== framework || current?.id !== sessionId || current.manuallyNamed || current.name !== session.name) return;
        app.sessionManager.renameSession(sessionId, name, false);
      })
      .catch(() => { /* A deleted session or failed persistence must not leak an async rejection. */ });
  });
}

/**
 * Session-scoped fleet summaries. The generation guard protects cache writes
 * AND pending/backoff bookkeeping when an old request completes after reset.
 */
export class FleetActivitySummaries {
  private readonly transcripts = new Map<string, string>();
  private readonly transcriptTotalLen = new Map<string, number>();
  private readonly cache = new Map<string, string>();
  private readonly snapshotLen = new Map<string, number>();
  private readonly pending = new Set<string>();
  private readonly backoffUntil = new Map<string, number>();
  private generation = 0;
  private framework: AgentFramework;
  private sessionId: string | undefined;

  constructor(private readonly app: AuxiliaryApp, private readonly rootAgentName: string) {
    this.framework = app.framework;
    this.sessionId = app.sessionManager.getActiveSession()?.id;
  }

  append(agentName: string, text: string): void {
    this.syncSession();
    const next = (this.transcripts.get(agentName) ?? '') + text;
    this.transcripts.set(agentName, next.length > 30_000 ? next.slice(-30_000) : next);
    this.transcriptTotalLen.set(agentName, (this.transcriptTotalLen.get(agentName) ?? 0) + text.length);
  }

  agentNames(): IterableIterator<string> {
    this.syncSession();
    return this.transcripts.keys();
  }

  get(agentName: string): string | undefined {
    this.syncSession();
    return this.cache.get(agentName);
  }

  isPending(agentName: string): boolean {
    this.syncSession();
    return this.pending.has(agentName);
  }

  private syncSession(): void {
    if (this.framework !== this.app.framework || this.sessionId !== this.app.sessionManager.getActiveSession()?.id) this.reset();
  }

  reset(): void {
    this.generation++;
    this.framework = this.app.framework;
    this.sessionId = this.app.sessionManager.getActiveSession()?.id;
    this.transcripts.clear();
    this.transcriptTotalLen.clear();
    this.cache.clear();
    this.snapshotLen.clear();
    this.pending.clear();
    this.backoffUntil.clear();
  }

  /** Returns true only when this session's visible summary changed. */
  async update(agentName: string): Promise<boolean> {
    this.syncSession();
    const transcript = this.transcripts.get(agentName);
    if (!transcript || this.pending.has(agentName) || Date.now() < (this.backoffUntil.get(agentName) ?? 0) || transcript.length < 50) return false;
    const totalLen = this.transcriptTotalLen.get(agentName) ?? 0;
    const lastLen = this.snapshotLen.get(agentName);
    // Failed calls also consume a snapshot: polling an unchanged transcript
    // must not be a retry loop. New activity can earn another attempt.
    if (lastLen !== undefined && totalLen - lastLen < 2000) return false;

    const framework = this.app.framework;
    const sessionId = this.app.sessionManager.getActiveSession()?.id;
    const model = framework.getAgent(this.rootAgentName)?.model;
    if (!sessionId || !model) return false;
    const generation = this.generation;
    const isCurrent = () => this.generation === generation && this.app.framework === framework &&
      this.app.sessionManager.getActiveSession()?.id === sessionId;

    this.pending.add(agentName);
    this.snapshotLen.set(agentName, totalLen);
    try {
      const response = await this.app.membrane.complete({
        messages: [{
          participant: 'user',
          content: [{ type: 'text', text: `Agent activity stream:\n\n${transcript.slice(-10_000)}\n\nWhat is this agent doing right now? Answer in 5-10 words.` }],
        }],
        system: 'You distill an agent\'s activity into a terse status phrase. 5-10 words max. No punctuation. Specific, not generic.',
        config: { model, maxTokens: 40, temperature: 0.3 },
      }, { retry: false });
      if (!isCurrent()) return false;
      const text = response.content
        .filter((b): b is { type: 'text'; text: string } => b.type === 'text')
        .map(b => b.text).join('').trim();
      if (!text) return false;
      this.cache.set(agentName, text.length > 60 ? text.slice(0, 57) + '...' : text);
      this.backoffUntil.delete(agentName);
      return true;
    } catch {
      if (isCurrent()) this.backoffUntil.set(agentName, Date.now() + 30_000);
      return false;
    } finally {
      if (isCurrent()) this.pending.delete(agentName);
    }
  }
}
