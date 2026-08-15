import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { SessionGoneError, fetchEvents, fetchSessions } from "@/lib/api";

const BASE = "https://example.execute-api.us-east-1.amazonaws.com";

beforeEach(() => {
  vi.stubEnv("VITE_RELIC_API_URL", BASE);
});

afterEach(() => {
  vi.unstubAllEnvs();
  vi.restoreAllMocks();
});

function mockFetch(status: number, body?: unknown) {
  const res = {
    status,
    ok: status >= 200 && status < 300,
    json: async () => body,
  } as Response;
  const spy = vi.fn().mockResolvedValue(res);
  vi.stubGlobal("fetch", spy);
  return spy;
}

describe("fetchSessions", () => {
  it("returns the sessions array", async () => {
    mockFetch(200, { sessions: [{ session_id: "s1" }] });
    const sessions = await fetchSessions();
    expect(sessions).toHaveLength(1);
    expect(sessions[0].session_id).toBe("s1");
  });
});

describe("fetchEvents", () => {
  it("returns the body on 200", async () => {
    mockFetch(200, { events: [{ seq: "0".repeat(19) + "1" }], last_seq: "0".repeat(19) + "1" });
    const result = await fetchEvents("s1");
    expect(result?.events).toHaveLength(1);
  });

  it("returns null on 204 without parsing a body", async () => {
    // res.json() on a 204 throws in most runtimes -- the status must be
    // checked before any parse attempt.
    const res = {
      status: 204,
      ok: true,
      json: async () => {
        throw new Error("must not parse a 204 body");
      },
    } as unknown as Response;
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue(res));

    await expect(fetchEvents("s1")).resolves.toBeNull();
  });

  it("throws SessionGoneError on 404", async () => {
    mockFetch(404, { error: "no such session" });
    await expect(fetchEvents("ghost")).rejects.toBeInstanceOf(SessionGoneError);
  });

  it("throws a plain Error on 500 so callers can distinguish it from death", async () => {
    mockFetch(500, { error: "boom" });
    const err = await fetchEvents("s1").catch((e) => e);
    expect(err).toBeInstanceOf(Error);
    expect(err).not.toBeInstanceOf(SessionGoneError);
  });

  it("sends since as a query param when given", async () => {
    const spy = mockFetch(204);
    await fetchEvents("s1", "0".repeat(19) + "5");
    expect(spy.mock.calls[0][0]).toContain("since=" + "0".repeat(19) + "5");
  });

  it("omits since entirely on first load", async () => {
    const spy = mockFetch(204);
    await fetchEvents("s1");
    expect(spy.mock.calls[0][0]).not.toContain("since");
  });
});
