import { render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";

import { EventRow } from "@/components/EventRow";
import type { RelicEvent } from "@/types";

const base: RelicEvent = {
  session_id: "s1",
  seq: "0".repeat(20),
  event_type: "log.line",
  raw: "1.0 Sys [Info]: something happened",
  attrs: {},
  v: 1,
  game_time_s: 1.0,
};

describe("EventRow", () => {
  it("renders a log line's raw text", () => {
    render(<EventRow event={base} />);
    expect(screen.getByText(/something happened/)).toBeDefined();
  });

  it("shows the item name prominently for a relic reward", () => {
    const reward: RelicEvent = {
      ...base,
      event_type: "reward.relic",
      game_time_s: 186.318,
      attrs: {
        player_id: "player0000",
        item_path: "/Lotus/StoreItems/Types/Recipes/Weapons/WeaponParts/GyrePrimeSystemsBlueprint",
        item_name: "GyrePrimeSystemsBlueprint",
      },
      raw: "186.318 Sys [Info]: VoidProjections: player0000 gets reward /Lotus/...",
    };

    render(<EventRow event={reward} />);

    // The whole point of the parser made visible: one line out of thousands
    // looks different because it matters.
    expect(screen.getByText("GyrePrimeSystemsBlueprint")).toBeDefined();
    expect(screen.getByText(/186\.318/)).toBeDefined();
  });

  it("renders without a game clock", () => {
    // Lines logged before the header is parsed carry no timestamp at all.
    const noClock: RelicEvent = { ...base };
    delete noClock.game_time_s;
    render(<EventRow event={noClock} />);
    expect(screen.getByText(/something happened/)).toBeDefined();
  });
});
