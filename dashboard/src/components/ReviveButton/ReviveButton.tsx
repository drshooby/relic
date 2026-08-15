import type { FeedState } from "@/types";

import styles from "./ReviveButton.module.css";

interface Props {
  state: FeedState;
  reviveCount: number;
  onRevive: () => void;
}

export function ReviveButton({ state, reviveCount, onRevive }: Props) {
  if (state !== "downed" && state !== "dead") {
    return null;
  }

  return (
    <div className={styles.wrap}>
      <span className={styles.status}>
        {state === "downed" ? "Session downed" : "Session dead"}
      </span>
      <button type="button" className={styles.button} onClick={onRevive}>
        Revive
      </button>
      {reviveCount > 0 && (
        <span className={styles.count}>revived {reviveCount}x</span>
      )}
    </div>
  );
}
