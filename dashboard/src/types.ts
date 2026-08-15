/** One play session, as returned by GET /sessions. */
export interface Session {
  session_id: string;
  started_at: string;
  last_seen_at: string;
  event_count: number;
}

/** One parsed log line. `attrs` is empty for log.line events. */
export interface RelicEvent {
  session_id: string;
  /** Zero-padded to width 20. Passed back to the API verbatim as the cursor. */
  seq: string;
  event_type: "log.line" | "reward.relic";
  raw: string;
  attrs: Record<string, string>;
  game_time_s?: number;
  wall_time_utc?: string;
  v: number;
}

export interface SessionsResponse {
  sessions: Session[];
}

export interface EventsResponse {
  events: RelicEvent[];
  last_seq: string;
}

/**
 * Where a session sits in the revive mechanic.
 *  alive   - polling, events arriving
 *  downed  - quiet past the threshold; auto-revive already spent
 *  dead    - a manual revive found nothing; polling stopped
 *  expired - events TTL'd out; only the summary survives
 *  error   - a request failed. NOT death: the session may be fine.
 */
export type FeedState = "alive" | "downed" | "dead" | "expired" | "error";
