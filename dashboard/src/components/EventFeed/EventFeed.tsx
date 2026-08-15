import { EventRow } from "@/components/EventRow";
import type { RelicEvent } from "@/types";

import styles from "./EventFeed.module.css";

export function EventFeed({ events }: { events: RelicEvent[] }) {
  if (events.length === 0) {
    return <p className={styles.empty}>No events yet.</p>;
  }

  return (
    <ul className={styles.feed}>
      {events.map((event) => (
        <li key={`${event.session_id}:${event.seq}`} className={styles.item}>
          <EventRow event={event} />
        </li>
      ))}
    </ul>
  );
}
