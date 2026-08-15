# M4 — Read API and dashboard design

**Status:** designed, not implemented.
**Refines:** [phase-1 design §6](2026-07-21-relic-phase1-design.md).
**Depends on:** M3's `relic-events` and `relic-sessions` tables, which this reads and never writes.

The goal is a dashboard that shows what is happening in-game while it happens, and tells the truth about what it cannot show when the data has aged out.

## What the dashboard is for

Not "match started". The point is **moments** — a relic reward landing, with its timestamp. M3 verified one against real data: `reward.relic`, `GyrePrimeSystemsBlueprint`, `game_time_s` 186.318, one line out of 6,332. The dashboard exists to make that one line look different from the other 6,331.

## Session states

Three states, all **inferred** — no new stored field:

| State | Session row | Events | Rendered as |
|---|---|---|---|
| Live | fresh `last_seen_at` | present | streaming feed, polling |
| Recent | ≤24h | present | full feed, static |
| Expired | 24h–7d | aged out (TTL) | summary card only |

The ambiguity worth naming: a session at its very first moment looks identical to an expired one — row present, no events. `last_seen_at` disambiguates. Fresh means live and just starting; stale means expired. `event_count` is shown in the expired card because it is genuinely informative ("4,812 events, expired") even when the events themselves are gone.

## The revive mechanic

Borrowed from the game's downed/dead mechanic, because the underlying problem is the same: the dashboard cannot distinguish "you stopped playing" from "you are in a menu and nothing is being logged". Rather than guess with a longer timeout, it asks.

```
                    ┌─────────────────────────────────┐
                    ▼                                 │
  idle ──select──► ALIVE ──30s quiet──► [auto-revive] ┤ found
                    ▲                        │        │
                    │                    nothing      │
                    │                        ▼        │
                    └──── found ────────  DOWNED ──────┘
                                             │ manual revive → 1 ping
                                         nothing
                                             ▼
                                           DEAD  (polling stopped)
```

**Polling interval is 2s while ALIVE** (per §6 of the phase-1 design), so the 30s quiet window is roughly 15 consecutive empty polls before a session is questioned. Both values are constants in one place, not scattered through components.

- **Auto-revive** spends one free ping before the prompt ever appears, so a loading screen or a slow batch self-heals silently.
- **Manual revives are unlimited.** A limited count would be thematically perfect and practically wrong — running out would mean refusing to check a session that might be active. The bleed-out count is displayed as flavor.
- No affinity cost. Nothing is being spent, and inventing a currency would be noise.
- **DEAD stops the interval entirely.** That is the actual saving: a finished session costs zero requests instead of polling forever while you have walked away.

**The quiet timer measures time since the last 200-with-events**, not since the last request. Measuring since the last request would let a burst of 204s reset it forever, and nothing would ever go downed.

## API contract

HTTP API (not REST API — cheaper, and no usage plans or API keys are needed). One Lambda, two routes.

```
GET /sessions
  200 → { "sessions": [ { session_id, started_at, last_seen_at, event_count }, … ] }
        newest first, capped at 20. Empty list is still 200 — a list endpoint
        returning nothing is a real answer.

GET /sessions/{id}/events?since=<padded_seq>
  200 → { "events": [ { seq, event_type, game_time_s, wall_time_utc, raw, attrs }, … ],
          "last_seq": "00000000000000005039" }
  204 → no body. The session exists; nothing past `since`.
  404 → unknown session id.
```

**Why 204 rather than `200 []`.** At the call site, `if (res.status === 204)` states the outcome; `if ((await res.json()).events.length === 0)` makes the reader reconstruct it. Code is read more than written. The usual objection — that 204 forbids a body, so `last_seq` cannot be returned — dissolves on inspection: when nothing is found the client's cursor has not moved, so it already holds the value.

**The 404/204 split is what makes "expired" work.** 404 means the session row itself aged out (>7d, gone entirely). 204 means the row is alive but its events aged out (>24h). Two distinguishable outcomes, no new field.

**`since` is exclusive and optional.** First load omits it and gets the session from the start; every later poll returns the `last_seq` it was given. This maps directly onto `KeyConditionExpression` with `seq > :since`.

**`last_seq` is the padded string, never an int.** The client passes it back verbatim, so padding is never reconstructed client-side. Padding is one of the invariants that fails silently, so the fewer places that know about it, the better.

**`GET /sessions` requires a Scan**, deliberately. A partition key cannot be enumerated. It is cheap because `relic-sessions` holds one row per session and the 20-cap bounds it. This is a known, accepted scan — not something for a later reader to "fix".

## Read Lambda (`infra/pipeline/lambda/api/`)

```
main.py       handler: route -> query -> response. Only module importing boto3.
queries.py    DynamoDB access. Table objects in, plain dicts out.
responses.py  status codes, JSON body, CORS headers. Pure.
```

Same split as the hot path, for the same reason: `queries.py` and `responses.py` are pure and testable without AWS; `main.py` stays thin enough to verify by reading.

**Routing:** two routes, one Lambda, dispatched on `event["requestContext"]["http"]["path"]` and `pathParameters`. Two functions would double the IAM and Terraform for no benefit at this scale.

**IAM:** a new role, read-only — `dynamodb:Query`, `GetItem`, `Scan` on the two table ARNs. The hot path's role stays write-only. Neither function can do the other's job.

**CORS:** `allow_origins = ["http://localhost:5173"]`, methods `["GET"]`. Phase 1 runs the dashboard locally, so a wildcard buys nothing. Note CORS is a browser-only control and is not what makes the API private — the API has no auth (§6 of the phase-1 design), so `curl` works regardless. The real mitigations are that it exists only while the pipeline is applied and serves a 24h rolling cache. A deployed origin gets added when one exists.

**`Decimal` is a real trap on the way out.** DynamoDB returns Numbers as `decimal.Decimal`, and `json.dumps` raises `TypeError` on it. `responses.py` must convert. This is the mirror image of the hot path's float bug — the same type boundary, failing in the opposite direction.

## Dashboard (`dashboard/`)

Vite 8, React 19.2, TypeScript 6, bun, CSS Modules, `src/` layout. No server: the dashboard is a static bundle that talks only to the HTTP API, which is why a SPA fits better than a server framework here.

```
src/
  components/
    SessionList/        sessions sidebar; live badge; selection
    SessionHeader/      id, duration, state chip (alive/downed/dead/expired)
    ReviveButton/       downed + dead only; bleed-out count
    EventFeed/          the scrolling list
    EventRow/           one event; reward.relic styled distinctly
    ExpiredCard/        summary when events aged out
  lib/
    api.ts              typed fetch wrappers; 204/404 handled here
    useSessionFeed.ts   the hook and its state machine
  types.ts              Session, RelicEvent, FeedState
  App.tsx               composes the layout
```

Every component is a directory with `Component.tsx`, `Component.module.css`, and an `index.tsx` re-export — the re-export is what makes `@/components/Foo` resolve. `lib/` and `types.ts` sit outside `components/` because they are not components.

**`EventRow` carries the visible payoff.** A `log.line` renders muted and compact; a `reward.relic` renders prominently with its item name and `game_time_s`. That contrast is the entire point of the parser made visible.

**All fetching is client-side**, against API Gateway directly. The revive mechanic is inherently client state — a gesture, a counter, a cursor — and routing it through a server would split that across a boundary for nothing. It also implies no server, which matches the project's cost discipline.

**The API URL comes from `VITE_RELIC_API_URL`.** Local dev reads `.env.local`; a future static build injects it at build time; a future Vercel deploy sets it in project settings. Same code either way. The value should be read from the SSM parameter the pipeline writes rather than copy-pasted, since the gateway URL changes on every apply.

**Deployment is out of scope.** Phase 1 runs `bun run dev` locally: the dashboard is only useful at this Mac while the game is running, and the API only exists while the pipeline is applied. Both are torn down together.

## Error handling

| Response | Meaning |
|---|---|
| 200 with events | alive — append, advance cursor, reset the quiet timer |
| 204 | nothing new — start or continue the quiet timer |
| 404 | session row gone entirely — terminal, not revivable |
| network error / 5xx | **not** death — show an error, keep retrying |

The last row is the one worth guarding. A failed request means the request failed, not that the session ended. Conflating them would show "dead" every time the wifi hiccups.

## Testing

Python, following the hot path: `queries.py` and `responses.py` are pure and tested against a dict-backed fake; `main.py` is tested for routing and status codes.

Cases that matter: `since` filtering is exclusive; an empty result yields 204 and not `200 []`; an unknown session yields 404; the 20-cap holds; a `Decimal` from DynamoDB serializes without raising.

TypeScript: **Vitest**, which is Vite's own test runner — it reads `vite.config.ts`, so the `@/*` alias is defined once and shared by the app and the tests. Its fake timers are what make the revive state machine testable.

`useSessionFeed` gets real tests with fake timers and a mocked fetch: 204 starts the quiet timer; 30s of 204s triggers exactly **one** auto-revive; a network error does **not** transition to dead; manual revive from dead sends exactly one request; dead stops the interval.

**Not covered:** real AWS, real browsers. The E2E proof is running against an applied pipeline during a live session — the same standard M3 was held to, and the same reason the hot path's float bug survived 35 green tests.

## Out of scope

- Deployment (S3, Vercel, containers) — local `bun dev` for phase 1.
- Auth — §6 of the phase-1 design defers it; revisit if ever hosted publicly.
- Historical browsing beyond the 24h/7d TTL windows — that is phase 2's Athena work.
- WebSockets — polling is sufficient for one user.
- Item path → display-name mapping (`GyrePrimeSystemsBlueprint` → "Gyre Prime Systems Blueprint"). The raw path renders until that mapping exists.
