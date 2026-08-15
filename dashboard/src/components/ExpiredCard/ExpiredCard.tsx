import type { Session } from "@/types";

import styles from "./ExpiredCard.module.css";

/**
 * Shown when a session's events have aged out of DynamoDB (~24h TTL) while
 * its summary row survives (~7d). The honest answer: this session happened,
 * here is how big it was, and its detail is gone from the hot path.
 */
export function ExpiredCard({ session }: { session: Session }) {
  return (
    <div className={styles.card}>
      <h3 className={styles.title}>Events expired</h3>
      <p className={styles.body}>
        This session recorded <strong>{session.event_count}</strong> events, but
        they have aged out of the 24-hour cache. The raw lines are still in the
        S3 archive.
      </p>
      <p className={styles.meta}>
        {session.started_at} &rarr; {session.last_seen_at}
      </p>
    </div>
  );
}
