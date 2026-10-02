import { useCallback, useEffect, useRef, useState } from "react";

/** Telling you a price moved, once, when it moves.
 *
 *  Two rules this exists to keep:
 *
 *  * **Only on the way in.** A toast and a sound fire when a symbol *crosses*
 *    the threshold, not for every symbol that is over it. The page refreshes on
 *    a timer, so notifying on presence would re-announce the same six positions
 *    every five minutes until they moved back — which is how an alert becomes
 *    something you learn to ignore.
 *  * **Never on arrival.** Opening the page with six positions already over the
 *    threshold is not six events. The first payload seeds the baseline silently;
 *    only what appears after that is news.
 *
 *  The sound is synthesised rather than a file: no asset to ship, nothing for a
 *  strict CSP to block, and it cannot 404 in production. Browsers refuse audio
 *  until the page has been interacted with, so the first beep may be swallowed —
 *  the toast is the part that always works, and the speaker button doubles as
 *  the gesture that unlocks it.
 */

const MUTE_KEY = "dashboard.alertsMuted";
/** Long enough to read across the room, short enough not to stack up. */
const TOAST_MS = 12_000;

export interface AlertEvent {
  key: string;
  symbol: string;
  account: string;
  pct: number;
  ltp: number;
  prev: number | null;
}

/** Peak amplitude of each note, 0..1.
 *
 *  Started at 0.09, which was inaudible across a room — a sine wave at a tenth
 *  of full scale carries far less than the number suggests, because there are no
 *  harmonics to cut through anything else in the room. Raised here, and a
 *  triangle wave used below, which is the bigger part of the fix.
 */
const VOLUME = 0.5;

/** A short two-tone chime, up for a rise and down for a fall.
 *
 *  Direction in the sound means a move can be heard without looking, which is
 *  the only reason a sound beats a toast on its own.
 *
 *  Triangle rather than sine: a pure sine has no upper harmonics, so at any
 *  sane volume it sits under ambient noise instead of over it. A triangle is
 *  still soft — nothing like a square — but it has enough edge to be noticed
 *  without being shrill. Each note also rings longer than it did, because a
 *  120ms blip reads as a click rather than a chime.
 */
function chime(up: boolean) {
  type WindowWithAudio = Window & { webkitAudioContext?: typeof AudioContext };
  const win = window as WindowWithAudio;
  const Ctor = window.AudioContext ?? win.webkitAudioContext;
  if (!Ctor) return;

  let ctx: AudioContext;
  try {
    ctx = new Ctor();
  } catch {
    return;
  }

  const now = ctx.currentTime;
  const notes = up ? [784, 1047] : [784, 523];
  const spacing = 0.16;
  const ring = 0.3;

  notes.forEach((frequency, index) => {
    const at = now + index * spacing;
    const osc = ctx.createOscillator();
    const gain = ctx.createGain();
    osc.type = "triangle";
    osc.frequency.value = frequency;
    // Ramped rather than switched: a square edge on a gain node clicks. The
    // second note is left slightly quieter so the pair reads as one chime
    // rather than two separate alerts.
    const peak = index === 0 ? VOLUME : VOLUME * 0.8;
    gain.gain.setValueAtTime(0.0001, at);
    gain.gain.exponentialRampToValueAtTime(peak, at + 0.015);
    gain.gain.exponentialRampToValueAtTime(0.0001, at + ring);
    osc.connect(gain).connect(ctx.destination);
    osc.start(at);
    osc.stop(at + ring + 0.02);
  });

  // Contexts are a limited resource and one is created per chime. Closed after
  // the last note has finished ringing, or it cuts itself off.
  const lifetime = (notes.length - 1) * spacing + ring + 0.1;
  window.setTimeout(() => void ctx.close().catch(() => {}), lifetime * 1000 + 200);
}

/** Watches a set of alert keys and reports only the ones that are new. */
export function useAlertEvents(alerts: AlertEvent[] | undefined, ready: boolean) {
  const seen = useRef<Set<string> | null>(null);
  const [events, setEvents] = useState<AlertEvent[]>([]);
  const [muted, setMuted] = useState(
    () => window.localStorage.getItem(MUTE_KEY) === "1",
  );

  useEffect(() => {
    window.localStorage.setItem(MUTE_KEY, muted ? "1" : "0");
  }, [muted]);

  useEffect(() => {
    if (!ready || alerts === undefined) return;
    const keys = new Set(alerts.map((alert) => alert.key));

    // First payload: remember what is already over the line, say nothing.
    if (seen.current === null) {
      seen.current = keys;
      return;
    }

    const fresh = alerts.filter((alert) => !seen.current!.has(alert.key));
    seen.current = keys;
    if (fresh.length === 0) return;

    setEvents((current) => [...fresh, ...current].slice(0, 6));
    if (!muted) {
      // One sound for the batch, not one per symbol — five at once should not
      // sound like a fire alarm. Direction follows the largest mover.
      const loudest = fresh.reduce((a, b) => (Math.abs(b.pct) > Math.abs(a.pct) ? b : a));
      chime(loudest.pct > 0);
    }
  }, [alerts, ready, muted]);

  const dismiss = useCallback((key: string) => {
    setEvents((current) => current.filter((event) => event.key !== key));
  }, []);

  return { events, dismiss, muted, setMuted, test: () => chime(true) };
}

export function AlertToasts({
  events,
  onDismiss,
}: {
  events: AlertEvent[];
  onDismiss: (key: string) => void;
}) {
  // Each toast clears itself. Keyed on the event so a re-trigger of the same
  // symbol restarts its own timer rather than inheriting the old one.
  useEffect(() => {
    const timers = events.map((event) =>
      window.setTimeout(() => onDismiss(event.key), TOAST_MS),
    );
    return () => timers.forEach((timer) => window.clearTimeout(timer));
  }, [events, onDismiss]);

  if (events.length === 0) return null;

  return (
    <div
      className="pointer-events-none fixed right-4 top-4 z-50 flex w-72 flex-col gap-2"
      // Announced to a screen reader as it arrives, not on a schedule.
      role="status"
      aria-live="polite"
    >
      {events.map((event) => {
        const up = event.pct > 0;
        return (
          <div
            key={event.key}
            className="pointer-events-auto rounded border px-3 py-2 shadow-lg backdrop-blur"
            style={{
              borderColor: up ? "var(--gain)" : "var(--loss)",
              background: "var(--surface)",
            }}
          >
            <div className="flex items-baseline gap-2">
              <span className="text-sm font-semibold">{event.symbol}</span>
              <span className="text-[10px] uppercase tracking-wide text-[var(--ink-muted)]">
                {event.account}
              </span>
              <span
                className={`tnum ml-auto text-sm font-semibold ${
                  up ? "text-[var(--gain)]" : "text-[var(--loss)]"
                }`}
              >
                {up ? "▲" : "▼"} {Math.abs(event.pct).toFixed(2)}%
              </span>
              <button
                type="button"
                onClick={() => onDismiss(event.key)}
                aria-label="Dismiss"
                className="text-xs text-[var(--ink-muted)] hover:underline"
              >
                ×
              </button>
            </div>
            <div className="tnum mt-0.5 text-xs text-[var(--ink-muted)]">
              {event.ltp.toFixed(2)}
              {event.prev !== null && ` from ${event.prev.toFixed(2)}`}
            </div>
          </div>
        );
      })}
    </div>
  );
}
