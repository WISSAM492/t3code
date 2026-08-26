import type { ServerProvider, ServerProviderRateLimitWindow } from "@t3tools/contracts";

export type CodexLimitWindowView = {
  readonly label: string;
  readonly remainingPercent: number;
  readonly usedPercent: number;
  readonly resetsLabel: string | null;
};

export type CodexLimitsView = {
  /** Stable React key; includes environment id for Connect remotes. */
  readonly viewKey: string;
  readonly instanceId: string;
  readonly title: string;
  readonly primary: CodexLimitWindowView | null;
  readonly secondary: CodexLimitWindowView | null;
};

export type CodexLimitsEnvironmentInput = {
  readonly environmentId: string;
  readonly environmentLabel?: string | undefined;
  readonly providers: ReadonlyArray<ServerProvider>;
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

function instanceTitle(
  provider: ServerProvider,
  options: {
    readonly includeEmail: boolean;
    readonly environmentLabel?: string | undefined;
    readonly includeEnvironmentLabel: boolean;
  },
): string {
  const name = provider.displayName?.trim() || "Codex";
  const email = provider.auth.email?.trim();
  const parts = [name];
  if (options.includeEmail && email) {
    parts.push(email);
  }
  if (options.includeEnvironmentLabel && options.environmentLabel?.trim()) {
    parts.push(options.environmentLabel.trim());
  }
  return parts.join(" · ");
}

function isCodexLimitsProvider(provider: ServerProvider): boolean {
  return (
    provider.driver === "codex" &&
    provider.enabled &&
    provider.installed &&
    provider.auth.status === "authenticated" &&
    provider.rateLimits !== undefined &&
    (provider.rateLimits.primary !== undefined || provider.rateLimits.secondary !== undefined)
  );
}

/**
 * Build sidebar views for every enabled, authenticated Codex instance that
 * reported rate limits. Prefer {@link getCodexLimitsViewsForEnvironments} when
 * T3 Connect remotes are involved — primary-only misses remote Linux hosts.
 */
export function getCodexLimitsViews(
  providers: ReadonlyArray<ServerProvider>,
  nowMs: number = Date.now(),
): ReadonlyArray<CodexLimitsView> {
  return getCodexLimitsViewsForEnvironments([{ environmentId: "local", providers }], nowMs);
}

/**
 * Same as {@link getCodexLimitsViews}, but across every connected environment.
 * Windows Desktop + T3 Connect keeps Codex on the remote Linux environment while
 * `PrimaryConnectionTarget` stays local — aggregating all configs fixes that.
 */
export function getCodexLimitsViewsForEnvironments(
  environments: ReadonlyArray<CodexLimitsEnvironmentInput>,
  nowMs: number = Date.now(),
): ReadonlyArray<CodexLimitsView> {
  const candidates = environments.flatMap((environment) =>
    environment.providers.filter(isCodexLimitsProvider).map((provider) => ({
      environment,
      provider,
    })),
  );

  const includeEmail = candidates.length > 1;
  // Use the full environment list (not only those with limits) so a Windows
  // local + Linux Connect pair still labels the remote host clearly.
  const includeEnvironmentLabel = environments.length > 1;

  return candidates.flatMap(({ environment, provider }) => {
    const primary = toWindowView(provider.rateLimits?.primary, "5 hour limit", nowMs);
    const secondary = toWindowView(provider.rateLimits?.secondary, "Weekly limit", nowMs);
    if (!primary && !secondary) {
      return [];
    }
    const view: CodexLimitsView = {
      viewKey: `${environment.environmentId}:${provider.instanceId}`,
      instanceId: provider.instanceId,
      title: instanceTitle(provider, {
        includeEmail,
        includeEnvironmentLabel,
        environmentLabel: environment.environmentLabel,
      }),
      primary,
      secondary,
    };
    return [view];
  });
}
