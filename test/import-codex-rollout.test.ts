import { describe, expect, test } from 'bun:test';
import { OpenAIResponsesFormatter, NativeFormatter } from '@animalabs/membrane';
import {
  projectItem,
  reconstructEffectiveHistory,
} from '../scripts/import-codex-rollout.js';

describe('Codex rollout import', () => {
  test('uses replacement_history as the authoritative compaction boundary', () => {
    const history = reconstructEffectiveHistory([
      { type: 'response_item', payload: { type: 'message', id: 'discarded' } },
      {
        type: 'compacted', timestamp: '2026-07-13T00:00:00Z',
        payload: { replacement_history: [
          { type: 'message', role: 'user', content: 'kept' },
          { type: 'compaction', id: 'cmp_1', encrypted_content: 'opaque' },
        ] },
      },
      { type: 'response_item', payload: { type: 'reasoning', id: 'rs_2', encrypted_content: 'tail' } },
    ]);

    expect(history.map(entry => entry.item.id ?? entry.item.type)).toEqual([
      'message', 'cmp_1', 'rs_2',
    ]);
    expect(history[0]!.restoredByCompaction).toBe(true);
    expect(history[2]!.restoredByCompaction).toBe(false);
  });

  test('native imported tool and user images stay visual for primary replay and auxiliary compression', () => {
    const data = 'iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNkYPhfDwAChwGA60e6kgAAAABJRU5ErkJggg==';
    const call = { type: 'function_call', id: 'call-item', call_id: 'vision', name: 'inspect', arguments: '{}' };
    const output = { type: 'function_call_output', id: 'output-item', call_id: 'vision', output: [
      { type: 'input_text', text: 'before' }, { type: 'input_image', image_url: `data:image/png;base64,${data}` },
      { type: 'input_text', text: 'between' }, { type: 'input_image', image_url: 'https://example.test/image.png' },
      { type: 'input_text', text: 'after' },
    ] };
    const user = { type: 'message', id: 'user-item', role: 'user', content: [
      { type: 'input_text', text: 'user image' }, { type: 'input_image', image_url: `data:image/png;base64,${data}` },
    ] };
    const messages = [
      { participant: 'Codex', content: projectItem(call) },
      { participant: 'User', content: projectItem(output) },
      { participant: 'User', content: projectItem(user) },
    ];
    const primary = new OpenAIResponsesFormatter().buildMessages(messages,
      { participantMode: 'multiuser', assistantParticipant: 'Codex' });
    expect(primary.messages).toEqual([call, output, user]);
    const auxiliary = new NativeFormatter().buildMessages(messages,
      { participantMode: 'multiuser', assistantParticipant: 'Codex', promptCaching: false });
    const blocks = auxiliary.messages.flatMap(message => message.content as Array<Record<string, unknown>>);
    const result = blocks.find(block => block.type === 'tool_result');
    expect(result?.tool_use_id).toBe('vision');
    expect(result?.content).toEqual([
      { type: 'text', text: 'before' }, { type: 'image', source: { type: 'base64', media_type: 'image/png', data } },
      { type: 'text', text: 'between' }, { type: 'image', source: { type: 'url', url: 'https://example.test/image.png' } },
      { type: 'text', text: 'after' },
    ]);
    expect(blocks.filter(block => block.type === 'image')).toEqual([
      { type: 'image', source: { type: 'base64', media_type: 'image/png', data } },
    ]);
    expect(blocks.filter(block => block.type === 'text').map(block => block.text).join('\n')).not.toContain(data);
    expect(output.output[1]).toEqual({ type: 'input_image', image_url: `data:image/png;base64,${data}` });
  });

  test('unsupported non-image items remain exact opaque native replay carriers', () => {
    const item = { type: 'future-provider-item', id: 'opaque-id', payload: { untouched: true } };
    const built = new OpenAIResponsesFormatter().buildMessages([{ participant: 'Codex', content: projectItem(item) }],
      { participantMode: 'multiuser', assistantParticipant: 'Codex' });
    expect(built.messages).toEqual([item]);
  });

  test('keeps the exact native item on the Chronicle projection block', () => {
    const item = {
      type: 'message', id: 'msg_1', role: 'assistant', phase: 'commentary',
      content: [{ type: 'output_text', text: 'hello' }],
    };
    const [block] = projectItem(item);
    expect(block).toMatchObject({ type: 'text', text: 'hello', rawItem: item });
  });
});
