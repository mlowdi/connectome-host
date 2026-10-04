import { defaultTokenEstimator, jsonTokenEstimator, MessageStore } from '@animalabs/context-manager';
import { IMAGE_TOKEN_ESTIMATE, isImageReference } from '@animalabs/membrane';

export interface ContentEstimate {
  tokens: number;
  nImages: number;
}

/**
 * Uncalibrated MessageStore-style accounting for normalized or unresolved
 * stored content. Inspect semantic fields only: rawItem and binary media are
 * opaque carriers, not JSON text to tokenize. Blob refs cost what their
 * resolved media would cost, without opening the archive.
 */
export function measureContent(content: readonly unknown[]): ContentEstimate {
  const estimate: ContentEstimate = { tokens: 0, nImages: 0 };
  for (const block of content) addBlock(block, estimate);
  return estimate;
}

function addBlock(block: unknown, estimate: ContentEstimate): void {
  if (!block || typeof block !== 'object') return;
  const b = block as Record<string, unknown>;
  switch (b.type) {
    case 'text':
      estimate.tokens += defaultTokenEstimator(typeof b.text === 'string' ? b.text : '');
      break;
    case 'tool_use':
      estimate.tokens += jsonTokenEstimator(JSON.stringify(b.input ?? {})) + 20;
      break;
    case 'tool_result':
      if (typeof b.content === 'string') {
        estimate.tokens += jsonTokenEstimator(b.content);
      } else if (Array.isArray(b.content)) {
        for (const child of b.content) addBlock(child, estimate);
      }
      break;
    case 'image':
    case 'generated_image':
      estimate.tokens += typeof b.tokenEstimate === 'number' ? b.tokenEstimate : IMAGE_TOKEN_ESTIMATE;
      estimate.nImages++;
      break;
    case 'blob_ref': {
      const ref = b.ref as { originalType?: string } | undefined;
      if (isImageReference(b)) {
        estimate.tokens += typeof b.tokenEstimate === 'number' ? b.tokenEstimate : IMAGE_TOKEN_ESTIMATE;
        estimate.nImages++;
      } else if (ref?.originalType === 'document' || ref?.originalType === 'audio' || ref?.originalType === 'video') {
        estimate.tokens += 1000;
      }
      break;
    }
    case 'document':
    case 'audio':
    case 'video':
      estimate.tokens += 1000;
      break;
    case 'thinking': {
      if (typeof b.tokenEstimate === 'number') {
        estimate.tokens += b.tokenEstimate;
        break;
      }
      const textTokens = defaultTokenEstimator(typeof b.thinking === 'string' ? b.thinking : '');
      estimate.tokens += typeof b.signature === 'string' && b.signature.length > 0
        ? Math.max(textTokens, MessageStore.signedThinkingTokens(b.signature))
        : textTokens;
      break;
    }
    case 'redacted_thinking':
      estimate.tokens += typeof b.tokenEstimate === 'number'
        ? b.tokenEstimate
        : typeof b.data === 'string' && b.data.length > 0
          ? Math.round(b.data.length / MessageStore.ENCRYPTED_CARRIER_CHARS_PER_TOKEN)
          : MessageStore.HIDDEN_THINKING_TOKENS_DEFAULT;
      break;
  }
}
