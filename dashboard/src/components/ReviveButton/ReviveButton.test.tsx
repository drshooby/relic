import { render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

import { ReviveButton } from "@/components/ReviveButton";

describe("ReviveButton", () => {
  it("renders nothing while the session is alive", () => {
    const { container } = render(
      <ReviveButton state="alive" reviveCount={0} onRevive={vi.fn()} />,
    );
    expect(container.firstChild).toBeNull();
  });

  it("prompts when downed", () => {
    render(<ReviveButton state="downed" reviveCount={0} onRevive={vi.fn()} />);
    expect(screen.getByRole("button")).toBeDefined();
  });

  it("still offers revive when dead", () => {
    // Unlimited revives: refusing to check a session that might be active
    // would be thematic but wrong.
    render(<ReviveButton state="dead" reviveCount={3} onRevive={vi.fn()} />);
    expect(screen.getByRole("button")).toBeDefined();
  });

  it("shows the bleed-out count once it is non-zero", () => {
    render(<ReviveButton state="dead" reviveCount={2} onRevive={vi.fn()} />);
    expect(screen.getByText(/2/)).toBeDefined();
  });

  it("calls onRevive when clicked", async () => {
    const onRevive = vi.fn();
    render(<ReviveButton state="downed" reviveCount={0} onRevive={onRevive} />);
    screen.getByRole("button").click();
    expect(onRevive).toHaveBeenCalledTimes(1);
  });
});
