import { useCallback, useEffect, useRef, useState } from "react";

import { SessionGoneError, fetchEvents } from "@/lib/api";
import type { FeedState, RelicEvent } from "@/types";

export const POLL_INTERVAL_MS = 2000;

/**
 * How long a session may go without new events before it is questioned.
 * At a 2s poll that is ~15 consecutive empty responses -- long enough to sit
 * through a loading screen, short enough that a finished session stops
 * claiming to be live within half a minute.
 */
export const QUIET_THRESHOLD_MS = 30000;

interface FeedResult {
  events: RelicEvent[];
  state: FeedState;
  reviveCount: number;
  revive: () => Promise<void>;
  error: string | null;
}

export function useSessionFeed(sessionId: string | null): FeedResult {
  const [events, setEvents] = useState<RelicEvent[]>([]);
  const [state, setState] = useState<FeedState>("alive");
  const [reviveCount, setReviveCount] = useState(0);
  const [error, setError] = useState<string | null>(null);

  const cursor = useRef<string | undefined>(undefined);
  // Time of the last response that actually carried events -- NOT the last
  // request. Measuring from the last request would let a stream of 204s reset
  // the timer forever, and nothing would ever go downed.
  //
  // 0 is a sentinel meaning "not armed yet" -- Date.now() is impure and the
  // React Compiler forbids calling it during render, so it can't be the
  // useRef initializer. It gets armed in the ref-reset effect below, which
  // runs (synchronously, before paint) as soon as `sessionId` changes and
  // strictly before the polling effect's first tick -- so the quiet check
  // never sees the sentinel and never computes `Date.now() - 0`.
  const lastEventAt = useRef<number>(0);
  const autoRevived = useRef(false);

  // Bumped every time the selected session changes (see the ref-reset effect
  // below). A poll captures the generation it was issued under and, when its
  // promise resolves, discards the result if the generation has since moved
  // on. Without this, a poll in flight for a just-deselected session resolves
  // late and writes a DIFFERENT session's events into state and -- worse --
  // that session's last_seq into cursor.current, which then permanently
  // skips every event below that seq on the next real poll. A boolean
  // "cancelled" flag is not enough: it has to be checked at both write sites
  // (setEvents AND cursor.current), and it has to be per-request, not
  // per-effect, because a request already in flight when sessionId changes
  // is not stopped by the interval's own cleanup.
  const generation = useRef(0);

  // Reset state whenever the selected session CHANGES -- including changing
  // to null (deselecting a session must not leave the previous session's
  // events on screen). This is the documented "adjust state during render"
  // pattern: track the previous id in STATE (not a ref -- the React
  // Compiler forbids reading/writing refs during render) and, if it
  // differs, call setState conditionally and synchronously in the render
  // body. React sees the state update, discards this render's output, and
  // immediately re-renders with the reset state, instead of committing a
  // stale frame and cascading a second render from an effect.
  //
  // No truthiness guard on `sessionId` here: `prevSessionId` starts at
  // `null`, so mounting with `sessionId === null` compares null !== null
  // (false) and correctly does nothing, while mounting with a real id
  // compares null !== "s1" (true) and correctly resets once.
  const [prevSessionId, setPrevSessionId] = useState<string | null>(null);
  if (prevSessionId !== sessionId) {
    setPrevSessionId(sessionId);
    setEvents([]);
    setState("alive");
    setReviveCount(0);
    setError(null);
  }

  // Ref bookkeeping for the same reset is kept out of the render body (the
  // compiler forbids reading/writing refs and calling Date.now() there) and
  // done here instead. Effects still run before the browser paints and
  // before the polling effect's first tick can fire, so this is not a
  // user-visible cascading render -- it just can't live in the render path.
  //
  // Mirrors the state reset above: runs on every sessionId change, including
  // to null, so a stale cursor or timestamp from the old session can never
  // leak into a later session. Deselecting (sessionId null) clears the
  // cursor and revive flag but does not re-arm lastEventAt/autoRevived from
  // Date.now(), since there is no session to time against -- polling is
  // already stopped by the guard in the polling effect below, and whichever
  // real session id arrives next re-arms everything from this same effect.
  useEffect(() => {
    generation.current += 1;
    cursor.current = undefined;
    autoRevived.current = false;
    if (sessionId) {
      lastEventAt.current = Date.now();
    }
  }, [sessionId]);

  const poll = useCallback(async (): Promise<boolean> => {
    if (!sessionId) return false;

    // Captured BEFORE the await: this is the generation the request was
    // issued under. If the user switches sessions while this request is in
    // flight, the ref-reset effect above bumps generation.current, and the
    // comparison after the await below then correctly identifies this
    // result as stale.
    const myGeneration = generation.current;
    const isStale = () => generation.current !== myGeneration;

    try {
      const result = await fetchEvents(sessionId, cursor.current);
      if (isStale()) return false;
      // Clear a prior transport error on ANY successful response, including
      // a 204 (result === null) -- otherwise the error banner disappears
      // (setError(null) below) while the state chip is stuck reading
      // "error", since only the events branch used to reset state.
      setError(null);
      setState((prev) => (prev === "error" ? "alive" : prev));

      if (result && result.events.length > 0) {
        setEvents((prev) => [...prev, ...result.events]);
        cursor.current = result.last_seq;
        lastEventAt.current = Date.now();
        autoRevived.current = false;
        setState("alive");
        return true;
      }
      return false;
    } catch (err) {
      if (isStale()) return false;
      if (err instanceof SessionGoneError) {
        setState("expired");
        return false;
      }
      // Any other failure is a transport problem, not a dead session. Surface
      // it and keep polling: the game may be running perfectly.
      setError(err instanceof Error ? err.message : String(err));
      setState("error");
      return false;
    }
  }, [sessionId]);

  const revive = useCallback(async () => {
    setReviveCount((n) => n + 1);
    const found = await poll();
    if (!found) {
      setState("dead");
    }
  }, [poll]);

  useEffect(() => {
    if (!sessionId) return;
    // dead and expired are terminal: no interval at all, so a finished
    // session costs nothing.
    if (state === "dead" || state === "expired") return;

    let cancelled = false;

    const tick = async () => {
      const found = await poll();
      if (cancelled || found) return;

      const quietFor = Date.now() - lastEventAt.current;
      if (quietFor < QUIET_THRESHOLD_MS) return;

      if (!autoRevived.current) {
        // First time crossing the threshold: spend the free revive by
        // marking it used, but do NOT declare downed yet and do NOT poll
        // again right now -- an immediate re-poll happens microseconds
        // after the threshold, which is still mid-loading-screen and buys
        // nothing. Instead grant a full extra QUIET_THRESHOLD_MS of real
        // wall-clock time: the next several ticks keep calling poll() at
        // the normal cadence above, and any of them finding events resets
        // lastEventAt and autoRevived, returning the feed to alive without
        // ever showing downed. Only if the session is STILL quiet a full
        // threshold later does the branch below fire.
        autoRevived.current = true;
        return;
      }

      if (quietFor >= QUIET_THRESHOLD_MS * 2) {
        setState("downed");
      }
    };

    void tick();
    const id = setInterval(() => void tick(), POLL_INTERVAL_MS);
    return () => {
      cancelled = true;
      clearInterval(id);
    };
  }, [sessionId, state, poll]);

  return { events, state, reviveCount, revive, error };
}
