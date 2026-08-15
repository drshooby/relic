import type { Session } from "@/types";

import styles from "./SessionList.module.css";

interface Props {
  sessions: Session[];
  selectedId: string | null;
  onSelect: (sessionId: string) => void;
}

/** A session counts as live if it was updated within the quiet threshold. */
function isLive(session: Session): boolean {
  return Date.now() - Date.parse(session.last_seen_at) < 30000;
}

export function SessionList({ sessions, selectedId, onSelect }: Props) {
  if (sessions.length === 0) {
    return <p className={styles.empty}>No sessions. Is the pipeline applied?</p>;
  }

  return (
    <ul className={styles.list}>
      {sessions.map((session) => (
        <li key={session.session_id}>
          <button
            type="button"
            className={
              session.session_id === selectedId
                ? `${styles.item} ${styles.selected}`
                : styles.item
            }
            onClick={() => onSelect(session.session_id)}
          >
            <span className={styles.id}>{session.session_id.slice(0, 8)}</span>
            <span className={styles.count}>{session.event_count} events</span>
            {isLive(session) && <span className={styles.live}>LIVE</span>}
          </button>
        </li>
      ))}
    </ul>
  );
}
