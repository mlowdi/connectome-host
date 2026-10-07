import { MembraneError, type ProviderRequest } from '@animalabs/membrane';
import { ARCHIVAL_MEMORY_LOCAL_CAP_CODE } from '@animalabs/context-manager';
import { types } from 'node:util';

/** Non-wire provenance placed only on the native builder's final owned directive. */
export const ARCHIVAL_MEMORY_METADATA = 'archivalMemory';
export const ARCHIVAL_MEMORY_MODEL = 'gpt-6.1-sol';
export const ARCHIVAL_CYBER_POLICY_FALLBACK_MODEL = 'gpt-daybreak-blue-latest';
export function isArchivalMemoryModel(model: unknown): model is typeof ARCHIVAL_MEMORY_MODEL | typeof ARCHIVAL_CYBER_POLICY_FALLBACK_MODEL {
  return model === ARCHIVAL_MEMORY_MODEL || model === ARCHIVAL_CYBER_POLICY_FALLBACK_MODEL;
}
export interface ArchivalMemoryProvenance {
  version: 1;
  operation: 'l1' | 'merge' | 'transition';
  model: typeof ARCHIVAL_MEMORY_MODEL | typeof ARCHIVAL_CYBER_POLICY_FALLBACK_MODEL;
  inputBudgetTokens: number;
  maxOutputTokens: number;
  outputReserve: number;
  nativeEstimatedPromptTokens: number;
}
export interface ArchivalMemoryAccounting {
  operation: ArchivalMemoryProvenance['operation'];
  serializedBodyUtf8Bytes: number;
  promptTokenUpperBound: number;
  nativeEstimatedPromptTokens: number;
  outputReserve: number;
  totalTokenUpperBound: number;
  inputBudgetTokens: number;
  totalBudgetTokens: number;
  accountingBasis: 'serialized Responses body UTF-8 bytes plus conservative input-token allowances; not measured tokens or an API byte limit';
}
export class ArchivalMemoryBudgetError extends MembraneError {
  constructor(reason: 'invalid-provenance' | 'cap', readonly accounting?: ArchivalMemoryAccounting) {
    super({
      type: reason === 'cap' ? 'context_length' : 'invalid_request',
      retryable: false,
      ...(reason === 'cap' ? { providerErrorCode: ARCHIVAL_MEMORY_LOCAL_CAP_CODE } : {}),
      message: reason === 'cap' ? 'Archival memory request exceeds conservative cap; refusing without clamping' : 'Invalid archival memory provenance/budget',
      rawError: undefined,
    });
  }
}
const positiveInteger = (value: unknown): value is number => typeof value === 'number' && Number.isSafeInteger(value) && value > 0;
const record = (value: unknown): value is Record<string, unknown> => !!value && typeof value === 'object' && !Array.isArray(value);

/** Missing provenance leaves ordinary primary/live requests completely untouched. */
export function readArchivalMemoryProvenance(messages: unknown, model: string, outputReserve: number): ArchivalMemoryProvenance | undefined {
  if (!Array.isArray(messages)) return undefined;
  let provenance: ArchivalMemoryProvenance | undefined;
  for (let index = 0; index < messages.length; index++) {
    const message = messages[index];
    if (!record(message) || !record(message.metadata) || !Object.hasOwn(message.metadata, ARCHIVAL_MEMORY_METADATA)) continue;
    const value = message.metadata[ARCHIVAL_MEMORY_METADATA];
    if (provenance || index !== messages.length - 1 || message.participant !== 'Context Manager' || !record(value) ||
        Object.keys(value).sort().join(',') !== 'inputBudgetTokens,maxOutputTokens,model,nativeEstimatedPromptTokens,operation,outputReserve,version' ||
        value.version !== 1 || (value.operation !== 'l1' && value.operation !== 'merge' && value.operation !== 'transition') ||
        !isArchivalMemoryModel(value.model) || model !== value.model ||
        !positiveInteger(value.inputBudgetTokens) || value.inputBudgetTokens > 600512 ||
        !positiveInteger(value.maxOutputTokens) || value.maxOutputTokens > 8192 ||
        !positiveInteger(value.outputReserve) || value.outputReserve > value.maxOutputTokens || value.outputReserve !== outputReserve ||
        typeof value.nativeEstimatedPromptTokens !== 'number' || !Number.isFinite(value.nativeEstimatedPromptTokens) || value.nativeEstimatedPromptTokens < 0) {
      throw new ArchivalMemoryBudgetError('invalid-provenance');
    }
    provenance = {
      version: 1, operation: value.operation, model: value.model,
      inputBudgetTokens: value.inputBudgetTokens, maxOutputTokens: value.maxOutputTokens,
      outputReserve: value.outputReserve, nativeEstimatedPromptTokens: value.nativeEstimatedPromptTokens,
    };
  }
  return provenance;
}
export function providerArchivalMemoryProvenance(request: ProviderRequest): ArchivalMemoryProvenance | undefined {
  const provenance = readArchivalMemoryProvenance(request.extra?.normalizedMessages, request.model, request.maxTokens);
  if (provenance && (request.tools?.length || Object.keys(request.extra ?? {}).some(key => key !== 'normalizedMessages'))) {
    throw new ArchivalMemoryBudgetError('invalid-provenance');
  }
  return provenance;
}

/** Tagged requests require observed terminal service evidence, never parser defaults. */
export function validateArchivalServedModel(rawServiceResponse: unknown, requested: string, actual: string, perRound?: Array<{ model: string }>): void {
  const observed = rawServiceResponse && typeof rawServiceResponse === 'object' && !types.isProxy(rawServiceResponse)
    ? Object.getOwnPropertyDescriptor(rawServiceResponse, 'model')?.value : undefined;
  if (!isArchivalMemoryModel(requested) || observed !== requested || actual !== requested ||
      (perRound !== undefined && (!Array.isArray(perRound) || perRound.some(round => round?.model !== requested)))) throw new MembraneError({
    type: 'invalid_request', retryable: false,
    message: 'Archival memory served-model mismatch; exact service-reported model required', rawError: undefined,
  });
}

function hasNonWireMetadata(value: unknown): boolean {
  if (Array.isArray(value)) return value.some(hasNonWireMetadata);
  if (!record(value)) return false;
  return Object.entries(value).some(([key, child]) => key === ARCHIVAL_MEMORY_METADATA || key === 'normalizedMessages' || hasNonWireMetadata(child));
}

/** Admission uses the final subscription body, after native input normalization. */
export function enforceArchivalMemoryBudget(body: unknown, provenance: ArchivalMemoryProvenance): ArchivalMemoryAccounting {
  if (!record(body) || body.model !== provenance.model || !Array.isArray(body.input) ||
      typeof body.instructions !== 'string' || !body.instructions.trim() ||
      (Array.isArray(body.tools) ? body.tools.length > 0 : body.tools !== undefined) ||
      hasNonWireMetadata(body)) {
    throw new ArchivalMemoryBudgetError('invalid-provenance');
  }
  const serializedBodyUtf8Bytes = Buffer.byteLength(JSON.stringify(body), 'utf8');
  const promptTokenUpperBound = serializedBodyUtf8Bytes + 4096 + body.input.length * 512;
  const accounting: ArchivalMemoryAccounting = {
    operation: provenance.operation,
    serializedBodyUtf8Bytes,
    promptTokenUpperBound,
    nativeEstimatedPromptTokens: provenance.nativeEstimatedPromptTokens,
    outputReserve: provenance.outputReserve,
    totalTokenUpperBound: promptTokenUpperBound + provenance.outputReserve,
    inputBudgetTokens: provenance.inputBudgetTokens,
    totalBudgetTokens: provenance.inputBudgetTokens + 4096,
    accountingBasis: 'serialized Responses body UTF-8 bytes plus conservative input-token allowances; not measured tokens or an API byte limit',
  };
  if (promptTokenUpperBound > accounting.inputBudgetTokens || accounting.totalTokenUpperBound > accounting.totalBudgetTokens) {
    throw new ArchivalMemoryBudgetError('cap', accounting);
  }
  return accounting;
}
