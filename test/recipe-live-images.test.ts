import { describe, expect, test } from 'bun:test';
import { filterImageMessages, type ContentBlock } from '@animalabs/membrane';
import { validateRecipe } from '../src/recipe.js';
import { buildFrameworkStrategy } from '../src/framework-strategy.js';

const keys = ['maxLiveImages', 'imageStripDepthTokens', 'maxLiveImageBytes'] as const;
const PNG = 'iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNkYPhfDwAChwGA60e6kgAAAABJRU5ErkJggg==';
const image = (label: string): ContentBlock => ({ type: 'image', source: { type: 'url', url: `https://example.test/${label}.png` } });

function recipe(strategy: Record<string, unknown>) {
  return validateRecipe({
    name: 'live-image-limits',
    agent: { systemPrompt: 'sys', strategy },
  });
}

function apply(strategy: Record<string, unknown>, content: ContentBlock[][]): ContentBlock[][] {
  const built = buildFrameworkStrategy(recipe(strategy), 'some-model', 'UTC');
  if (!built.liveImagePolicy) throw new Error('The constructed strategy has no live-image policy');
  return filterImageMessages(content.map(content => ({ content })), built.liveImagePolicy).map(message => message.content);
}

function visible(content: ContentBlock[][]): string[] {
  return content.flatMap(blocks => blocks.flatMap(block => block.type === 'image'
    ? [block.source.type === 'url' ? block.source.url : block.source.data] : []));
}

describe('built-in recipe image-policy behavior', () => {
  for (const type of ['autobiographical', 'frontdesk', undefined]) {
    const label = type ?? 'omitted autobiographical type';
    test(`${label}: a configured count ceiling retains the newest image`, () => {
      const output = apply({ type, maxLiveImages: 1, maxLiveImageBytes: 0, imageStripDepthTokens: 0 }, [
        [image('old')], [image('middle'), image('new')],
      ]);
      expect(visible(output)).toEqual(['https://example.test/new.png']);
    });

    test(`${label}: zero disables count/depth/byte ceilings`, () => {
      const output = apply({ type, maxLiveImages: 0, maxLiveImageBytes: 0, imageStripDepthTokens: 0 }, [
        [image('old')], [image('middle'), image('new')],
      ]);
      expect(visible(output)).toEqual([
        'https://example.test/old.png', 'https://example.test/middle.png', 'https://example.test/new.png',
      ]);
    });

    test(`${label}: byte admission includes its exact boundary`, () => {
      const content: ContentBlock[][] = [[{ type: 'image', source: { type: 'base64', mediaType: 'image/png', data: PNG } }]];
      const strategy = { type, maxLiveImages: 0, imageStripDepthTokens: 0 };
      expect(visible(apply({ ...strategy, maxLiveImageBytes: PNG.length }, content))).toEqual([PNG]);
      expect(visible(apply({ ...strategy, maxLiveImageBytes: PNG.length - 1 }, content))).toEqual([]);
    });

    test(`${label}: depth excludes an old image behind the recent text boundary`, () => {
      const output = apply({ type, maxLiveImages: 0, maxLiveImageBytes: 0, imageStripDepthTokens: 1 }, [
        [image('old')], [{ type: 'text', text: 'new' }],
      ]);
      expect(visible(output)).toEqual([]);
    });

    for (const key of keys) {
      test(`${label}: ${key} rejects malformed limits at recipe load`, () => {
        for (const value of [-1, 0.5, NaN, Infinity, -Infinity, Number.MAX_SAFE_INTEGER + 1, '6', true, null, {}, []]) {
          expect(() => recipe({ type, [key]: value })).toThrow(Error);
        }
      });
    }
  }
});
