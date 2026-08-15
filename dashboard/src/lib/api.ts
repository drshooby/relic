import type { EventsResponse, Session, SessionsResponse } from "@/types";

/**
 * Thrown when the API returns 404: the session row itself is gone (its ~7d
 * TTL elapsed). Distinct from a 204, which means the row is alive but its
 * events aged out, and distinct from a network failure, which says nothing
 * about the session at all.
 */
export class SessionGoneError extends Error {
  constructor(sessionId: string) {
    super(`session ${sessionId} no longer exists`);
    this.name = "SessionGoneError";
  }
}

function baseUrl(): string {
  const url = import.meta.env.VITE_RELIC_API_URL;
  if (!url) {
    throw new Error(
      "VITE_RELIC_API_URL is not set. Get it from `terraform output api_invoke_url` in infra/pipeline and put it in dashboard/.env.local",
    );
  }
  return url.replace(/\/$/, "");
}

export async function fetchSessions(): Promise<Session[]> {
  const res = await fetch(`${baseUrl()}/sessions`);
  if (!res.ok) {
    throw new Error(`GET /sessions failed: ${res.status}`);
  }
  const body = (await res.json()) as SessionsResponse;
  return body.sessions;
}

/**
 * Events after `since`. Returns null for 204 -- "the session exists, nothing
 * is new" -- which is the signal the revive mechanic counts quiet time on.
 */
export async function fetchEvents(
  sessionId: string,
  since?: string,
): Promise<EventsResponse | null> {
  const url = new URL(`${baseUrl()}/sessions/${sessionId}/events`);
  if (since) {
    url.searchParams.set("since", since);
  }

  const res = await fetch(url.toString());

  // Check the status before touching the body: res.json() on a 204 throws.
  if (res.status === 204) {
    return null;
  }
  if (res.status === 404) {
    throw new SessionGoneError(sessionId);
  }
  if (!res.ok) {
    throw new Error(`GET events failed: ${res.status}`);
  }

  return (await res.json()) as EventsResponse;
}
