import { act, renderHook, waitFor } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { SessionGoneError } from "@/lib/api";
import { QUIET_THRESHOLD_MS, useSessionFeed } from "@/lib/useSessionFeed";

const seq = (n: number) => String(n).padStart(20, "0");

vi.mock("@/lib/api", async () => {
  const actual = await vi.importActual<typeof import("@/lib/api")>("@/lib/api");
  return { ...actual, fetchEvents: vi.fn() };
});

const { fetchEvents } = await import("@/lib/api");
const mockFetchEvents = vi.mocked(fetchEvents);

beforeEach(() => {
  vi.useFakeTimers({ shouldAdvanceTime: true });
  mockFetchEvents.mockReset();
});

afterEach(() => {
  vi.useRealTimers();
});

describe("useSessionFeed", () => {
  it("starts alive and appends events", async () => {
    mockFetchEvents.mockResolvedValue({
      events: [{ seq: seq(1), event_type: "log.line", raw: "x", attrs: {}, v: 1, session_id: "s1" }],
      last_seq: seq(1),
    });

    const { result } = renderHook(() => useSessionFeed("s1"));

    await waitFor(() => expect(result.current.events).toHaveLength(1));
    expect(result.current.state).toBe("alive");
  });

  it("advances the cursor so each poll asks for only what is new", async () => {
    mockFetchEvents.mockResolvedValueOnce({
      events: [{ seq: seq(5), event_type: "log.line", raw: "x", attrs: {}, v: 1, session_id: "s1" }],
      last_seq: seq(5),
    });
    mockFetchEvents.mockResolvedValue(null);

    renderHook(() => useSessionFeed("s1"));

    await waitFor(() => expect(mockFetchEvents).toHaveBeenCalledTimes(1));
    await act(async () => {
      await vi.advanceTimersByTimeAsync(2000);
    });

    expect(mockFetchEvents).toHaveBeenLastCalledWith("s1", seq(5));
  });

  it("goes downed after the quiet threshold, spending exactly one auto-revive", async () => {
    mockFetchEvents.mockResolvedValue(null);

    const { result } = renderHook(() => useSessionFeed("s1"));

    await act(async () => {
      await vi.advanceTimersByTimeAsync(QUIET_THRESHOLD_MS + 2000);
    });

    await waitFor(() => expect(result.current.state).toBe("downed"));
    // The auto-revive is spent silently before the prompt appears, so the
    // visible count stays 0 until the user clicks.
    expect(result.current.reviveCount).toBe(0);
  });

  it("returns to alive when a quiet stretch ends on its own", async () => {
    mockFetchEvents.mockResolvedValue(null);
    const { result } = renderHook(() => useSessionFeed("s1"));

    await act(async () => {
      await vi.advanceTimersByTimeAsync(10000);
    });

    mockFetchEvents.mockResolvedValue({
      events: [{ seq: seq(9), event_type: "log.line", raw: "x", attrs: {}, v: 1, session_id: "s1" }],
      last_seq: seq(9),
    });

    await act(async () => {
      await vi.advanceTimersByTimeAsync(2000);
    });

    await waitFor(() => expect(result.current.state).toBe("alive"));
  });

  it("manual revive from downed sends exactly one request", async () => {
    mockFetchEvents.mockResolvedValue(null);
    const { result } = renderHook(() => useSessionFeed("s1"));

    await act(async () => {
      await vi.advanceTimersByTimeAsync(QUIET_THRESHOLD_MS + 2000);
    });
    await waitFor(() => expect(result.current.state).toBe("downed"));

    mockFetchEvents.mockClear();
    await act(async () => {
      await result.current.revive();
    });

    expect(mockFetchEvents).toHaveBeenCalledTimes(1);
    expect(result.current.state).toBe("dead");
    expect(result.current.reviveCount).toBe(1);
  });

  it("stops polling once dead", async () => {
    mockFetchEvents.mockResolvedValue(null);
    const { result } = renderHook(() => useSessionFeed("s1"));

    await act(async () => {
      await vi.advanceTimersByTimeAsync(QUIET_THRESHOLD_MS + 2000);
    });
    await act(async () => {
      await result.current.revive();
    });
    await waitFor(() => expect(result.current.state).toBe("dead"));

    mockFetchEvents.mockClear();
    await act(async () => {
      await vi.advanceTimersByTimeAsync(10000);
    });

    // A dead session costs zero requests. That is the point of the mechanic.
    expect(mockFetchEvents).not.toHaveBeenCalled();
  });

  it("a network error does NOT kill the session", async () => {
    mockFetchEvents.mockRejectedValue(new Error("network down"));

    const { result } = renderHook(() => useSessionFeed("s1"));

    await waitFor(() => expect(result.current.state).toBe("error"));
    // Wifi hiccups are not session death: keep polling.
    mockFetchEvents.mockClear();
    await act(async () => {
      await vi.advanceTimersByTimeAsync(2000);
    });
    expect(mockFetchEvents).toHaveBeenCalled();
  });

  it("a 404 marks the session expired and stops polling", async () => {
    mockFetchEvents.mockRejectedValue(new SessionGoneError("s1"));

    const { result } = renderHook(() => useSessionFeed("s1"));

    await waitFor(() => expect(result.current.state).toBe("expired"));
    mockFetchEvents.mockClear();
    await act(async () => {
      await vi.advanceTimersByTimeAsync(5000);
    });
    expect(mockFetchEvents).not.toHaveBeenCalled();
  });

  it("does nothing without a session id", async () => {
    renderHook(() => useSessionFeed(null));
    await act(async () => {
      await vi.advanceTimersByTimeAsync(5000);
    });
    expect(mockFetchEvents).not.toHaveBeenCalled();
  });

  it("resets the feed when switching to a different session", async () => {
    mockFetchEvents.mockResolvedValue({
      events: [{ seq: seq(1), event_type: "log.line", raw: "x", attrs: {}, v: 1, session_id: "s1" }],
      last_seq: seq(1),
    });

    const { result, rerender } = renderHook(({ sessionId }) => useSessionFeed(sessionId), {
      initialProps: { sessionId: "s1" as string | null },
    });

    await waitFor(() => expect(result.current.events).toHaveLength(1));

    mockFetchEvents.mockClear();
    mockFetchEvents.mockResolvedValue(null);
    rerender({ sessionId: "s2" });

    expect(result.current.events).toHaveLength(0);
    expect(result.current.state).toBe("alive");

    // The cursor must reset too, not just the visible state. If it didn't,
    // this poll would ask s2 for events *after* s1's last_seq and silently
    // miss everything s2 has logged so far.
    await waitFor(() => expect(mockFetchEvents).toHaveBeenCalledWith("s2", undefined));
  });

  it("clears events when the session is deselected (set to null)", async () => {
    mockFetchEvents.mockResolvedValue({
      events: [{ seq: seq(1), event_type: "log.line", raw: "x", attrs: {}, v: 1, session_id: "s1" }],
      last_seq: seq(1),
    });

    const { result, rerender } = renderHook(({ sessionId }) => useSessionFeed(sessionId), {
      initialProps: { sessionId: "s1" as string | null },
    });

    await waitFor(() => expect(result.current.events).toHaveLength(1));

    mockFetchEvents.mockClear();
    rerender({ sessionId: null });

    expect(result.current.events).toHaveLength(0);
    expect(result.current.state).toBe("alive");

    // Deselecting must also stop polling -- there is no session to poll.
    await act(async () => {
      await vi.advanceTimersByTimeAsync(5000);
    });
    expect(mockFetchEvents).not.toHaveBeenCalled();
  });
});
