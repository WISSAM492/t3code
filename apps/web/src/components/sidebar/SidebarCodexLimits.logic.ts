import type { ServerProvider, ServerProviderRateLimitWindow } from "@t3tools/contracts";

export type CodexLimitWindowView = {
  readonly label: string;
  readonly remainingPercent: number;
  readonly usedPercent: number;
  readonly resetsLabel: string | null;
};

export type CodexLimitsView = {
  readonly instanceId: string;
  readonly title: string;
  readonly primary: CodexLimitWindowView | null;
  readonly secondary: CodexLimitWindowView | null;
};

function clampPercent(value: number): number {
  if (!Number.isFinite(value)) return 0;
  return Math.max(0, Math.min(100, Math.round(value)));
}

export function remainingPercentFromUsed(usedPercent: number): number {
  return clampPercent(100 - usedPercent);
}

/**
 * Format a Codex `resetsAt` unix timestamp (seconds, or ms if > 1e12).
 * Returns labels like "Resets in 3h" / "Resets in 2d", or "Reset" when past.
 */
export function formatRateLimitResetsLabel(
  resetsAt: number | undefined,
  nowMs: number = Date.now(),
): string | null {
  if (typeof resetsAt !== "number" || !Number.isFinite(resetsAt)) {
    return null;
  }
  const resetsAtMs = resetsAt > 1e12 ? resetsAt : resetsAt * 1000;
  const diffMs = resetsAtMs - nowMs;
  if (diffMs <= 0) return "Reset";

  const totalSeconds = Math.floor(diffMs / 1000);
  if (totalSeconds < 60) return "Resets in <1m";

  const totalMinutes = Math.floor(totalSeconds / 60);
  if (totalMinutes < 60) return `Resets in ${totalMinutes}m`;

  const totalHours = Math.floor(totalMinutes / 60);
  const remMinutes = totalMinutes % 60;
  if (totalHours < 24) {
    return remMinutes > 0 ? `Resets in ${totalHours}h ${remMinutes}m` : `Resets in ${totalHours}h`;
  }

  const days = Math.floor(totalHours / 24);
  const remHours = totalHours % 24;
  return remHours > 0 ? `Resets in ${days}d ${remHours}h` : `Resets in ${days}d`;
}

export function labelForRateLimitWindow(
  window: Pick<ServerProviderRateLimitWindow, "windowDurationMins">,
  fallback: string,
): string {
  const mins = window.windowDurationMins;
  if (typeof mins !== "number" || !Number.isFinite(mins) || mins <= 0) {
    return fallback;
  }
  if (mins === 300) return "5 hour limit";
  if (mins === 60 * 24 * 7) return "Weekly limit";
  if (mins % (60 * 24) === 0) {
    const days = mins / (60 * 24);
    return days === 1 ? "Daily limit" : `${days}-day limit`;
  }
  if (mins % 60 === 0) {
    const hours = mins / 60;
    return hours === 1 ? "1 hour limit" : `${hours} hour limit`;
  }
  return `${mins} min limit`;
}

function toWindowView(
  window: ServerProviderRateLimitWindow | undefined,
  fallbackLabel: string,
  nowMs: number,
): CodexLimitWindowView | null {
  if (!window || !Number.isFinite(window.usedPercent)) {
    return null;
  }
  const usedPercent = clampPercent(window.usedPercent);
  return {
    label: labelForRateLimitWindow(window, fallbackLabel),
    usedPercent,
    remainingPercent: remainingPercentFromUsed(usedPercent),
    resetsLabel: formatRateLimitResetsLabel(window.resetsAt, nowMs),
  };
}

function instanceTitle(provider: ServerProvider, includeEmail: boolean): string {
  const name = provider.displayName?.trim() || "Codex";
  const email = provider.auth.email?.trim();
  if (includeEmail && email) {
    return `${name} · ${email}`;
  }
  return name;
}

/**
 * Build sidebar views for every enabled, authenticated Codex instance that
 * reported rate limits. Returns empty when nothing should render.
 */
export function getCodexLimitsViews(
  providers: ReadonlyArray<ServerProvider>,
  nowMs: number = Date.now(),
): ReadonlyArray<CodexLimitsView> {
  const codexProviders = providers.filter(
    (provider) =>
      provider.driver === "codex" &&
      provider.enabled &&
      provider.installed &&
      provider.auth.status === "authenticated" &&
      provider.rateLimits !== undefined &&
      (provider.rateLimits.primary !== undefined || provider.rateLimits.secondary !== undefined),
  );

  const includeEmail = codexProviders.length > 1;

  return codexProviders.flatMap((provider) => {
    const primary = toWindowView(provider.rateLimits?.primary, "5 hour limit", nowMs);
    const secondary = toWindowView(provider.rateLimits?.secondary, "Weekly limit", nowMs);
    if (!primary && !secondary) {
      return [];
    }
    const view: CodexLimitsView = {
      instanceId: provider.instanceId,
      title: instanceTitle(provider, includeEmail),
      primary,
      secondary,
    };
    return [view];
  });
}
