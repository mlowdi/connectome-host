#!/usr/bin/env bun
/** Private, dated native backfill. Preparation/import do not invoke a model.
 * Runtime Codex calls exist ONLY in explicit `drain <instance> --apply`.
 */
import { prepare, deriveFiltered, ingest, drain, status, verify } from './lib/backfill-instance.js';
import { sha256 } from './lib/backfill-normalize.js';

export const HELP = `Usage: bun scripts/backfill-history.ts <operation> [options]

  prepare --manifest <sources.json> --recipe <full-recipe.json>
          --framing <parent-authored.md> --out <new-private-instance>
  derive <explicit-parent-instance> --exclude-events <explicit-decision.json>
         --framing <parent-authored.md> --out <new-private-instance> [--json]
  ingest <instance> --apply [--max-events <n> | --batch <n>]
  drain <instance> --apply [--max-steps <n>]
  status <instance> --json
  verify <instance> --json

Manifest v1: {version:1, cutoff:"2026-10-04T22:00:00.000Z", sources:[...]}
Each explicit file: id, kind (note|claude-code|omp|handoff), path, scope,
decision (include|exclude|candidate|quarantine), reason, timezone.
Included notes: author, date:{start,end?,precision:day|range|millisecond}.
date is WRITE date; optional coveredDateRange separately describes events.
Included conversations: sessionId, assistantAliases (empty = unattributed).
Optional: recordIds, branchHeads, mediaRoot, active (OMP newline prefix).
All selected notes through Oct5 remain eligible; cutoff is raw dialogue only.
The supplied handoff version:1 records[] is the final direct-dialogue bridge.
No glob/home/config/index/sidecar scanning and no credentials are copied.

prepare/derive never overwrite output or traverse output symlinks.
Unreadable/unsupported prepare inputs are audited.
derive uses validated immutable normalized events, NOT original source parsing.
Decision v1: originFingerprint, originSessionId, selectedSummaryIds (metadata
labels), eventKeys (exact stable keys), reason; all arrays nonempty and unique.
Optional audit counts/flags are validated and retained, never event selectors.
Only retained validated media is copied; originals remain in the parent.
New session/native store/checkpoint: no summaries, queues or preimage reuse.
omissions.json and derivation.json bind exact exclusions into the fingerprint.
Filtered manifests cannot be raw-prepared. Already filtered parents cannot be
re-derived: derive the complete reviewed union from the immutable original.
Omission metadata is NOT semantic-safety proof or a topic/string blacklist.
Immutable corpus/order/profile/ground changes require a NEW candidate.
ingest/drain without --apply report a dry run. No default live DATA_DIR.
Native source keys/shard ranges recover a killed append before its checkpoint.
Native sealed archival chunks include protected windows and partial tails.
status/verify never open/tick Chronicle or invoke inference; they validate the
last closed-writer native receipt. A crash is explicit recovery-required debt.

Real drain prerequisites (provisioned PRIVATELY by parent):
  CODEX_HOME=<private-auth-home> CODEX_BINARY=<supported-codex-binary>
  bun scripts/backfill-history.ts drain <instance> --apply --max-steps 1
Primary: openai-codex/gpt-6.1-sol; immutable prepared profile is unchanged.
Native archival only: confirmed structured cyber_policy permits ONE
 gpt-daybreak-blue-latest attempt; any fallback failure halts resumably.
No API-key/provider fallback, global model switch or automatic redaction.
No active tools/MCPL in staging or archival-lineage memory calls.
Generated primary Agent/CM system prompt is carrier ground ONLY; exact framing
belongs once in memory directives via strategy.identityReminder, including aux.
Ordinary live tool policy without archival lineage is unchanged.
Auth/provider errors fail resumably; active failures never report noWork.
Instance: recipe.json, ground.md, framing.md, source-path map in instance.json,
sources/, events.jsonl, dispositions.json, checkpoint.json, audit.json,
inspection.json, data/sessions.json, data/sessions/<id>/ (native Chronicle).
Parent alone owns real corpus selection, pilot execution and activation.
`;

export async function runCLI(args: string[]): Promise<number> {
  if (!args.length || args.includes('--help') || args[0] === 'help' || args[0] === '-h') { console.log(HELP); return 0; }
  const operation = args[0];
  if (!['prepare', 'derive', 'ingest', 'drain', 'status', 'verify'].includes(operation)) throw new Error('Unknown operation; use --help');
  const flags: Record<string, string | boolean> = {};
  let instance: string | undefined;
  for (let i = 1; i < args.length; i++) {
    const arg = args[i];
    if (!arg.startsWith('--')) { if (instance) throw new Error('Only one explicit instance is accepted'); instance = arg; continue; }
    if (flags[arg] !== undefined) throw new Error('Duplicate option');
    if (arg === '--apply' || arg === '--json') { flags[arg] = true; continue; }
    if (!['--manifest', '--recipe', '--framing', '--out', '--exclude-events', '--max-events', '--batch', '--max-steps'].includes(arg) || !args[i + 1] || args[i + 1].startsWith('--')) throw new Error('Unknown option or missing value; use --help');
    flags[arg] = args[++i];
  }
  const allowed: Record<string, string[]> = { prepare: ['--manifest', '--recipe', '--framing', '--out', '--json'], derive: ['--exclude-events', '--framing', '--out', '--json'], ingest: ['--apply', '--max-events', '--batch', '--json'], drain: ['--apply', '--max-steps', '--json'], status: ['--json'], verify: ['--json'] };
  if (Object.keys(flags).some(key => !allowed[operation].includes(key))) throw new Error('Option is not valid for this operation');
  const stringFlag = (name: string): string => { const value = flags[name]; if (typeof value !== 'string' || !value) throw new Error(`Required ${name}`); return value; };
  const numberFlag = (name: string): number | undefined => { if (flags[name] === undefined) return undefined; const value = Number(stringFlag(name)); if (!Number.isSafeInteger(value) || value <= 0) throw new Error(`${name} must be a positive integer`); return value; };
  let result: Record<string, unknown>;
  if (operation === 'prepare') {
    if (instance) throw new Error('prepare uses explicit --out, not a positional/default directory');
    result = await prepare({ manifest: stringFlag('--manifest'), recipe: stringFlag('--recipe'), framing: stringFlag('--framing'), out: stringFlag('--out') });
  } else if (operation === 'derive') {
    if (!instance) throw new Error('Explicit immutable parent instance path is required');
    result = await deriveFiltered({ parent: instance, excludeEvents: stringFlag('--exclude-events'), framing: stringFlag('--framing'), out: stringFlag('--out') });
  } else {
    if (!instance) throw new Error('Explicit private instance path is required');
    if (flags['--batch'] && flags['--max-events']) throw new Error('Use either --batch or --max-events');
    const options = { apply: flags['--apply'] === true, maxEvents: numberFlag('--max-events') ?? numberFlag('--batch'), maxSteps: numberFlag('--max-steps') };
    result = operation === 'ingest' ? await ingest(instance, options) : operation === 'drain' ? await drain(instance, options) : operation === 'verify' ? await verify(instance) : await status(instance);
  }
  console.log(JSON.stringify(result, null, 2));
  return (operation === 'verify' && result.verified === false) || ((operation === 'prepare' || operation === 'derive') && Number(result.errors) > 0) ? 2 : 0;
}
if (import.meta.main) {
  runCLI(process.argv.slice(2)).then(code => { process.exitCode = code; }).catch(error => {
    // Never dump source bodies, raw provider responses, auth or stack traces.
    const message = error instanceof Error ? error.message : 'Backfill failed';
    console.error(JSON.stringify({ error: 'Backfill operation failed; inspect private candidate state before acceptance', messageHash: sha256(message), failed: true })); process.exitCode = 1;
  });
}
