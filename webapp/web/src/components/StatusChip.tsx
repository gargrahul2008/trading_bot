import type { AccountRow } from "../lib/types";
import { age } from "../lib/format";

/** Status is never carried by colour alone: every chip pairs a glyph and a word
 *  with its colour, which is the rule for status palettes and the only way this
 *  reads correctly for a colourblind operator or in forced-colors mode.
 */
type Tone = "good" | "warning" | "critical";

const TONE_VAR: Record<Tone, string> = {
  good: "var(--status-good)",
  warning: "var(--status-warning)",
  critical: "var(--status-critical)",
};

const GLYPH: Record<Tone, string> = { good: "●", warning: "▲", critical: "✕" };

export function Chip({ tone, label, title }: { tone: Tone; label: string; title?: string }) {
  return (
    <span
      title={title}
      className="inline-flex items-center gap-1.5 rounded-full border px-2 py-0.5 text-xs font-medium"
      style={{ borderColor: "var(--border)", color: TONE_VAR[tone] }}
    >
      <span aria-hidden="true">{GLYPH[tone]}</span>
      {label}
    </span>
  );
}

/** What an account's row is actually telling you.
 *
 *  Three distinct states, and conflating them would be the dangerous kind of
 *  wrong: unreachable means we know nothing, stale means the numbers on screen
 *  are older than they should be, live means act on them.
 */
export function AccountStatus({ row }: { row: AccountRow }) {
  // Rebuilt from the store because the agent could not be reached. The figures
  // are real, but they are not now — the row must never pass for live.
  if (row.from_store) {
    const oldest = Object.values(row.sections)
      .map((meta) => meta.age_s ?? 0)
      .reduce((a, b) => Math.max(a, b), 0);
    return (
      <Chip
        tone="warning"
        label={`last seen ${age(oldest)}`}
        title={row.agent_error ?? "agent unreachable — showing what it last recorded"}
      />
    );
  }

  if (!row.reachable) {
    return <Chip tone="critical" label="unreachable" title={row.error ?? undefined} />;
  }

  // A dead token fails every section at once, so "stale positions" would be
  // both true and useless. Name the actual cause, because the fix is specific:
  // refresh the account's token.
  if (row.auth_ok === false) {
    return (
      <Chip
        tone="critical"
        label="token expired"
        title="The broker is rejecting this account's access token — run its fyers-auth unit."
      />
    );
  }

  const stalest = Object.entries(row.sections)
    .filter(([, meta]) => meta.stale)
    .sort((a, b) => (b[1].age_s ?? 0) - (a[1].age_s ?? 0))[0];

  if (stalest) {
    const [name, meta] = stalest;
    return (
      <Chip
        tone="warning"
        label={`stale ${name}`}
        title={`${name} last refreshed ${age(meta.age_s)}`}
      />
    );
  }

  const positions = row.sections.positions;
  return (
    <Chip
      tone="good"
      label={row.phase ? row.phase : "live"}
      title={`positions refreshed ${age(positions?.age_s)}`}
    />
  );
}
