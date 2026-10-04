import { expect, test } from 'bun:test';
import type { ProcessEvent, ProcessState } from '@animalabs/agent-framework';
import { OpenAIResponsesFormatter, type ContentBlock } from '@animalabs/membrane';
import { TuiModule } from '../src/modules/tui-module.js';

const png = 'iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mP8z8DwHwAFBQIAX8jx0gAAAABJRU5ErkJggg==';

for (const source of ['tui', 'headless']) {
  test(`${source} image content remains image input, not encoded prompt text`, async () => {
    const content: ContentBlock[] = [
      { type: 'text', text: 'What color is this pixel?' },
      { type: 'image', source: { type: 'base64', mediaType: 'image/png', data: png } },
    ];
    Object.freeze(content);
    const result = await new TuiModule().onProcess({
      type: 'external-message', source, content, triggerInference: false,
    } as ProcessEvent, {} as ProcessState);
    const projected = new OpenAIResponsesFormatter().buildMessages(
      result.addMessages ?? [],
      { participantMode: 'multiuser', assistantParticipant: 'connectome-trial' },
    ).messages as Array<{ type: string; role: string; content: Array<{ type: string; text?: string; image_url?: string }> }>;
    const wireContent = projected[0]!.content;
    expect(wireContent[0]).toEqual({ type: 'input_text', text: 'What color is this pixel?' });
    expect(wireContent[1]).toEqual({ type: 'input_image', image_url: `data:image/png;base64,${png}` });
    expect(wireContent.filter(part => part.type === 'input_text').some(part => part.text?.includes(png))).toBe(false);
  });
}
