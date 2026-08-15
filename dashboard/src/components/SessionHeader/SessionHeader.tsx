import type { FeedState, Session } from "@/types";

import styles from "./SessionHeader.module.css";

const LABELS: Record<FeedState, string> = {
  alive: "LIVE",
  downed: "DOWNED",
  dead: "DEAD",
  expired: "EXPIRED",
  error: "CONNECTION ERROR",
};

export function SessionHeader({
  session,
  state,
}: {
  session: Session;
  state: FeedState;
}) {
  const durationMs =
    Date.parse(session.last_seen_at) - Date.parse(session.started_at);
  const minutes = Math.max(0, Math.round(durationMs / 60000));

  return (
    <header className={styles.header}>
      <h2 className={styles.id}>{session.session_id.slice(0, 12)}</h2>
      <span className={`${styles.chip} ${styles[state]}`}>{LABELS[state]}</span>
      <span className={styles.meta}>
        {minutes} min &middot; {session.event_count} events
      </span>
    </header>
  );
}
