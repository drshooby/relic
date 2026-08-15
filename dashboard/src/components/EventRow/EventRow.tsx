import type { RelicEvent } from "@/types";

import styles from "./EventRow.module.css";

export function EventRow({ event }: { event: RelicEvent }) {
  const isReward = event.event_type === "reward.relic";
  const clock =
    event.game_time_s === undefined ? "" : event.game_time_s.toFixed(3);

  if (isReward) {
    return (
      <li className={`${styles.row} ${styles.reward}`}>
        <span className={styles.clock}>{clock}</span>
        <span className={styles.label}>REWARD</span>
        <span className={styles.item}>{event.attrs.item_name}</span>
      </li>
    );
  }

  return (
    <li className={styles.row}>
      <span className={styles.clock}>{clock}</span>
      <span className={styles.raw}>{event.raw}</span>
    </li>
  );
}
