import { useCallback, useEffect, useState } from "react";

import { EventFeed } from "@/components/EventFeed";
import { ExpiredCard } from "@/components/ExpiredCard";
import { ReviveButton } from "@/components/ReviveButton";
import { SessionHeader } from "@/components/SessionHeader";
import { SessionList } from "@/components/SessionList";
import { fetchSessions } from "@/lib/api";
import { useSessionFeed } from "@/lib/useSessionFeed";
import type { Session } from "@/types";

import styles from "./App.module.css";

/**
 * The session list refreshes far less often than the event feed: a new
 * session_id only appears when the game restarts, so polling it every 2s
 * would be wasted requests.
 */
const SESSION_LIST_INTERVAL_MS = 10000;

export default function App() {
  const [sessions, setSessions] = useState<Session[]>([]);
  const [selectedId, setSelectedId] = useState<string | null>(null);
  const [listError, setListError] = useState<string | null>(null);

  const loadSessions = useCallback(async () => {
    try {
      const rows = await fetchSessions();
      setSessions(rows);
      setListError(null);
      // Auto-select the newest session so the common case -- you just
      // finished playing -- needs no clicks.
      setSelectedId((current) => current ?? rows[0]?.session_id ?? null);
    } catch (err) {
      setListError(err instanceof Error ? err.message : String(err));
    }
  }, []);

  useEffect(() => {
    let cancelled = false;

    // Both the immediate call and the interval tick go through this same
    // wrapper -- not just the interval -- so that the lint rule (and any
    // future reader) sees ONE codepath that ever calls loadSessions from
    // this effect, always via an async callback. Inlining `void
    // loadSessions()` directly in the effect body reads as a synchronous
    // setState-in-effect to the React Compiler's lint rule, even though the
    // actual state updates happen after the awaited fetch resolves.
    const load = async () => {
      if (cancelled) return;
      await loadSessions();
    };

    void load();
    const id = setInterval(() => void load(), SESSION_LIST_INTERVAL_MS);
    return () => {
      cancelled = true;
      clearInterval(id);
    };
  }, [loadSessions]);

  const { events, state, reviveCount, revive, error } =
    useSessionFeed(selectedId);

  const selected = sessions.find((s) => s.session_id === selectedId) ?? null;

  return (
    <div className={styles.page}>
      <aside className={styles.sidebar}>
        <h1 className={styles.brand}>relic</h1>
        {listError && <p className={styles.error}>{listError}</p>}
        <SessionList
          sessions={sessions}
          selectedId={selectedId}
          onSelect={setSelectedId}
        />
      </aside>

      <main className={styles.main}>
        {selected ? (
          <>
            <SessionHeader session={selected} state={state} />
            {error && <p className={styles.error}>{error}</p>}
            <ReviveButton
              state={state}
              reviveCount={reviveCount}
              onRevive={() => void revive()}
            />
            {state === "expired" ? (
              <ExpiredCard session={selected} />
            ) : (
              <EventFeed events={events} />
            )}
          </>
        ) : (
          <p className={styles.placeholder}>Select a session.</p>
        )}
      </main>
    </div>
  );
}
