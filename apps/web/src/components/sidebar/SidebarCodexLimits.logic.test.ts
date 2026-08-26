import { describe, expect, it } from "vitest";
import { ProviderDriverKind, ProviderInstanceId, type ServerProvider } from "@t3tools/contracts";

import {
  formatRateLimitResetsLabel,
  getCodexLimitsViews,
  getCodexLimitsViewsForEnvironments,
  labelForRateLimitWindow,
  remainingPercentFromUsed,
} from "./SidebarCodexLimits.logic";

const CODEX = ProviderDriverKind.make("codex");
const NOW = Date.parse("2026-08-26T12:00:00.000Z");

function makeCodexProvider(input: {
  instanceId: string;
  displayName?: string;
  email?: string;
  rateLimits?: ServerProvider["rateLimits"];
  enabled?: boolean;
  authStatus?: ServerProvider["auth"]["status"];
}): ServerProvider {
  return {
    instanceId: ProviderInstanceId.make(input.instanceId),
    driver: CODEX,
    displayName: input.displayName,
    enabled: input.enabled ?? true,
    installed: true,
    version: "0.145.0",
    status: "ready",
    auth: {
      status: input.authStatus ?? "authenticated",
      ...(input.email ? { email: input.email } : {}),
    },
    checkedAt: "2026-08-26T12:00:00.000Z",
    models: [],
    slashCommands: [],
    skills: [],
    ...(input.rateLimits ? { rateLimits: input.rateLimits } : {}),
  };
}

describe("SidebarCodexLimits.logic", () => {
  it("computes remaining percent from used", () => {
    expect(remainingPercentFromUsed(13)).toBe(87);
    expect(remainingPercentFromUsed(0)).toBe(100);
    expect(remainingPercentFromUsed(100)).toBe(0);
    expect(remainingPercentFromUsed(150)).toBe(0);
  });

  it("labels known windows", () => {
    expect(labelForRateLimitWindow({ windowDurationMins: 300 }, "fallback")).toBe("5 hour limit");
    expect(labelForRateLimitWindow({ windowDurationMins: 60 * 24 * 7 }, "fallback")).toBe(
      "Weekly limit",
    );
    expect(labelForRateLimitWindow({}, "5 hour limit")).toBe("5 hour limit");
  });

  it("formats resets labels", () => {
    const inThreeHours = Math.floor(NOW / 1000) + 3 * 60 * 60 + 12 * 60;
    expect(formatRateLimitResetsLabel(inThreeHours, NOW)).toBe("Resets in 3h 12m");

    const inTwoDays = Math.floor(NOW / 1000) + 2 * 24 * 60 * 60 + 5 * 60 * 60;
    expect(formatRateLimitResetsLabel(inTwoDays, NOW)).toBe("Resets in 2d 5h");

    const past = Math.floor(NOW / 1000) - 60;
    expect(formatRateLimitResetsLabel(past, NOW)).toBe("Reset");
  });

  it("builds views for authenticated Codex instances with limits", () => {
    const views = getCodexLimitsViews(
      [
        makeCodexProvider({
          instanceId: "codex",
          displayName: "Codex",
          email: "a@example.com",
          rateLimits: {
            primary: {
              usedPercent: 13,
              resetsAt: Math.floor(NOW / 1000) + 4 * 60 * 60,
              windowDurationMins: 300,
            },
            secondary: {
              usedPercent: 0,
              resetsAt: Math.floor(NOW / 1000) + 6 * 24 * 60 * 60,
              windowDurationMins: 60 * 24 * 7,
            },
          },
        }),
        makeCodexProvider({
          instanceId: "codex_2nd",
          displayName: "2nd",
          email: "b@example.com",
          rateLimits: {
            primary: {
              usedPercent: 40,
              resetsAt: Math.floor(NOW / 1000) + 60 * 60,
              windowDurationMins: 300,
            },
          },
        }),
        makeCodexProvider({
          instanceId: "codex_empty",
          displayName: "Empty",
        }),
      ],
      NOW,
    );

    expect(views).toHaveLength(2);
    expect(views[0]?.title).toContain("Codex");
    expect(views[0]?.title).toContain("a@example.com");
    expect(views[0]?.primary?.remainingPercent).toBe(87);
    expect(views[0]?.primary?.label).toBe("5 hour limit");
    expect(views[0]?.secondary?.remainingPercent).toBe(100);
    expect(views[1]?.title).toContain("2nd");
    expect(views[1]?.secondary).toBeNull();
  });

  it("hides unauthenticated or disabled providers", () => {
    const views = getCodexLimitsViews(
      [
        makeCodexProvider({
          instanceId: "codex",
          authStatus: "unauthenticated",
          rateLimits: { primary: { usedPercent: 10, windowDurationMins: 300 } },
        }),
        makeCodexProvider({
          instanceId: "codex_2nd",
          enabled: false,
          rateLimits: { primary: { usedPercent: 10, windowDurationMins: 300 } },
        }),
      ],
      NOW,
    );
    expect(views).toEqual([]);
  });

  it("aggregates Codex limits across Connect environments (not primary-only)", () => {
    const views = getCodexLimitsViewsForEnvironments(
      [
        {
          environmentId: "windows-local",
          environmentLabel: "This PC",
          providers: [],
        },
        {
          environmentId: "linux-remote",
          environmentLabel: "Linux",
          providers: [
            makeCodexProvider({
              instanceId: "codex",
              displayName: "Codex",
              email: "a@example.com",
              rateLimits: {
                primary: {
                  usedPercent: 9,
                  resetsAt: Math.floor(NOW / 1000) + 3600,
                  windowDurationMins: 300,
                },
              },
            }),
          ],
        },
      ],
      NOW,
    );

    expect(views).toHaveLength(1);
    expect(views[0]?.viewKey).toBe("linux-remote:codex");
    expect(views[0]?.title).toContain("Linux");
    expect(views[0]?.primary?.remainingPercent).toBe(91);
  });
});
