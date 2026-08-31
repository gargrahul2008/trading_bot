/** Indian-format figures.
 *
 *  A null means "not known" and renders as a dash, never as zero — a zero reads
 *  as a real number, and on a screen you act from that is the difference
 *  between "flat" and "we could not reach this account".
 */

const inr = new Intl.NumberFormat("en-IN", {
  minimumFractionDigits: 2,
  maximumFractionDigits: 2,
});
const whole = new Intl.NumberFormat("en-IN", { maximumFractionDigits: 0 });
/** Quantities can be fractional for some instruments, but never to money's
 *  precision — four places is generous and stops 1e-9 dust rendering. */
const qtyFmt = new Intl.NumberFormat("en-IN", { maximumFractionDigits: 4 });

export const BLANK = "—";

/** A true minus (U+2212), not a hyphen. Intl gives a hyphen, which is narrower
 *  than the digits around it and sits at a different height — in a tabular
 *  column beside signed P&L figures the two glyphs visibly disagree. */
function minus(text: string): string {
  return text.replace(/^-/, "\u2212");
}

export function money(value: number | null | undefined): string {
  return value === null || value === undefined ? BLANK : minus(inr.format(value));
}

export function qty(value: number | null | undefined): string {
  return value === null || value === undefined ? BLANK : qtyFmt.format(value);
}

export function count(value: number | null | undefined): string {
  return value === null || value === undefined ? BLANK : whole.format(value);
}

/** P&L always carries its sign.
 *
 *  The gain/loss hues sit in the colourblind floor band, which is only legal
 *  with a second channel. The sign IS that channel, so it is never dropped —
 *  red-green is exactly the pair a third of colourblind readers cannot separate.
 */
export function signed(value: number | null | undefined): string {
  if (value === null || value === undefined) return BLANK;
  const sign = value > 0 ? "+" : value < 0 ? "\u2212" : "";
  return `${sign}${inr.format(Math.abs(value))}`;
}

export function pnlClass(value: number | null | undefined): string {
  if (value === null || value === undefined || value === 0) return "text-[var(--ink-secondary)]";
  return value > 0 ? "text-[var(--gain)]" : "text-[var(--loss)]";
}

/** How old a figure is, in words. Seconds matter here — this is a screen people
 *  act from, and "2m ago" on a live position is a warning, not a detail. */
export function age(seconds: number | null | undefined): string {
  if (seconds === null || seconds === undefined) return "never";
  if (seconds < 60) return `${Math.round(seconds)}s ago`;
  const minutes = Math.round(seconds / 60);
  if (minutes < 60) return `${minutes}m ago`;
  return `${Math.round(minutes / 60)}h ago`;
}

/** "1 order" / "2 orders". */
export function plural(n: number, one: string, many?: string): string {
  return `${count(n)} ${n === 1 ? one : (many ?? one + "s")}`;
}

/** Money arrives from the API as decimal strings. Parsing at the edge keeps the
 *  exact value in the payload and confines the float to display. */
export function num(value: string | null | undefined): number | null {
  if (value === null || value === undefined || value === "") return null;
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

/** Lakh and crore, for figures too large to read digit by digit. Indian
 *  grouping already helps, but a nine-digit number still needs a label. */
export function compact(value: number | null | undefined): string {
  if (value === null || value === undefined) return BLANK;
  const abs = Math.abs(value);
  const sign = value < 0 ? "\u2212" : "";
  if (abs >= 1e7) return `${sign}${(abs / 1e7).toFixed(2)} Cr`;
  if (abs >= 1e5) return `${sign}${(abs / 1e5).toFixed(2)} L`;
  return money(value);
}

export function percent(value: number | null | undefined): string {
  if (value === null || value === undefined) return BLANK;
  const sign = value > 0 ? "+" : value < 0 ? "\u2212" : "";
  return `${sign}${Math.abs(value).toFixed(2)}%`;
}
