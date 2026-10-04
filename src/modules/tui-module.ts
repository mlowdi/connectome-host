/**
 * TuiModule — handles external-message events from the TUI/CLI.
 *
 * Converts them to context messages and triggers inference.
 * Follows the same pattern as ApiModule's handleMessage().
 */

import type { ContentBlock } from '@animalabs/membrane';
import type {
  Module,
  ModuleContext,
  ProcessState,
  ProcessEvent,
  EventResponse,
  ToolDefinition,
  ToolCall,
  ToolResult,
} from '@animalabs/agent-framework';

export class TuiModule implements Module {
  readonly name = 'tui';

  async start(_ctx: ModuleContext): Promise<void> {}
  async stop(): Promise<void> {}
  getTools(): ToolDefinition[] { return []; }

  async handleToolCall(_call: ToolCall): Promise<ToolResult> {
    return { success: false, error: 'TuiModule has no tools', isError: true };
  }

  async onProcess(event: ProcessEvent, _state: ProcessState): Promise<EventResponse> {
    if (event.type !== 'external-message') return {};

    const source = (event as { source: string }).source;
    if (source !== 'tui' && source !== 'cli' && source !== 'system' && source !== 'headless') return {};

    const content = (event as { content: unknown }).content;
    const blocks: ContentBlock[] = Array.isArray(content)
      ? [...content]
      : [{ type: 'text', text: typeof content === 'string' ? content : JSON.stringify(content) }];
    const triggerInference = (event as { triggerInference?: boolean }).triggerInference;
    const targetAgents = (event as { targetAgents?: string[] }).targetAgents;

    // IPC has an operator output surface, not an external-channel locus.
    // Do not confuse absence of a publish target with failed delivery or
    // invite a reply to a stale/default external audience.
    if (source === 'headless') {
      blocks.push({
        type: 'text',
        text:
          '[host note: this message arrived over IPC without an external-channel locus. ' +
          'Replies are retained in the archive and exposed to connected operator clients; ' +
          'this does not establish whether any particular person has read them. Do not publish ' +
          'to an external channel unless requested; use an explicit send tool for a chosen recipient.]',
      });
    }

    const response: EventResponse = {
      addMessages: [
        {
          participant: 'user',
          content: blocks,
        },
      ],
    };

    if (triggerInference !== false) {
      response.requestInference = targetAgents ?? true;
    }

    return response;
  }
}
