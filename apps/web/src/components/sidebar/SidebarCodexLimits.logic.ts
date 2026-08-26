import type { ServerProvider, ServerProviderRateLimitWindow } from "@t3tools/contracts";

export type CodexLimitWindowView = {
  readonly label: string;
  readonly remainingPercent: number;
  readonly usedPercent: number;
  readonly resetsLabel: string | null;
  readonly isCritical: boolean;
};

export type CodexLimitsView = {
  /** Stable React key; includes environment id for Connect remotes. */
  readonly viewKey: string;
  readonly instanceId: string;
  /** Primary line: usually the account email. */
  readonly title: string;
  /** Optional secondary line (display name) when it adds information. */
  readonly subtitle: string | null;
  readonly accentColor: string | null;
  readonly primary: CodexLimitWindowView | null;
  readonly secondary: CodexLimitWindowView | null;
  /** Worst remaining across this account's windows. */
  readonly worstRemainingPercent: number;
  readonly isCritical: boolean;
  readonly isDepleted: boolean;
};

export type CodexLimitsEnvironmentInput = {
  readonly environmentId: string;
  readonly environmentLabel?: string | undefined;
  readonly providers: ReadonlyArray<ServerProvider>;
};

export type CodexLimitsSummary = {
  /** Usable remaining: min among windows that still have > 0 left. */
  readonly displayPercent: number | null;
  readonly hasCritical: boolean;
  readonly depletedAccountCount: number;
  readonly criticalAccountCount: number;
  readonly alertMessage: string | null;
};

const CRITICAL_REMAINING_PERCENT = 10;

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
  const remainingPercent = remainingPercentFromUsed(usedPercent);
  return {
    label: labelForRateLimitWindow(window, fallbackLabel),
    usedPercent,
    remainingPercent,
    resetsLabel: formatRateLimitResetsLabel(window.resetsAt, nowMs),
    isCritical: remainingPercent <= CRITICAL_REMAINING_PERCENT,
  };
}

function accountTitleParts(
  provider: ServerProvider,
  options: {
    readonly environmentLabel?: string | undefined;
    readonly includeEnvironmentLabel: boolean;
  },
): { readonly title: string; readonly subtitle: string | null } {
  const email = provider.auth.email?.trim() || null;
  const displayName = provider.displayName?.trim() || null;
  const environmentLabel = options.environmentLabel?.trim() || null;

  // Prefer email as the identity users recognize across "2nd"/"3rd" labels.
  const title = email ?? displayName ?? "Codex";
  const subtitleParts: string[] = [];
  if (email && displayName && displayName.toLowerCase() !== "codex") {
    subtitleParts.push(displayName);
  }
  if (options.includeEnvironmentLabel && environmentLabel) {
    subtitleParts.push(environmentLabel);
  }

  return {
    title,
    subtitle: subtitleParts.length > 0 ? subtitleParts.join(" · ") : null,
  };
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

function worstRemaining(
  primary: CodexLimitWindowView | null,
  secondary: CodexLimitWindowView | null,
): number {
  const values = [primary?.remainingPercent, secondary?.remainingPercent].filter(
    (value): value is number => typeof value === "number",
  );
  return values.length > 0 ? Math.min(...values) : 100;
}

/**
 * Sidebar button summary:
 * - percent = average of each account's **5 hour** (primary) remaining
 * - critical when any account is ≤ 10% (including 0%)
 */
export function summarizeCodexLimitsViews(
  views: ReadonlyArray<CodexLimitsView>,
): CodexLimitsSummary {
  const fiveHourPercents = views
    .map((view) => view.primary?.remainingPercent)
    .filter((value): value is number => typeof value === "number");

  const depletedAccountCount = views.filter((view) => view.isDepleted).length;
  const criticalAccountCount = views.filter((view) => view.isCritical).length;
  const displayPercent =
    fiveHourPercents.length > 0
      ? Math.round(
          fiveHourPercents.reduce((sum, value) => sum + value, 0) / fiveHourPercents.length,
        )
      : null;

  let alertMessage: string | null = null;
  if (depletedAccountCount > 0 && criticalAccountCount > depletedAccountCount) {
    alertMessage = `${depletedAccountCount} empty · ${criticalAccountCount - depletedAccountCount} low`;
  } else if (depletedAccountCount === 1) {
    alertMessage = "1 account has 0% remaining";
  } else if (depletedAccountCount > 1) {
    alertMessage = `${depletedAccountCount} accounts have 0% remaining`;
  } else if (criticalAccountCount === 1) {
    alertMessage = "1 account is below 10% remaining";
  } else if (criticalAccountCount > 1) {
    alertMessage = `${criticalAccountCount} accounts are below 10% remaining`;
  }

  return {
    displayPercent,
    hasCritical: criticalAccountCount > 0,
    depletedAccountCount,
    criticalAccountCount,
    alertMessage,
  };
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

  // Only label environments when limits actually come from more than one host.
  // A Windows local + Linux Connect pair with limits only on Linux should not
  // append the machine name to every row.
  const includeEnvironmentLabel =
    new Set(candidates.map((candidate) => candidate.environment.environmentId)).size > 1;

  return candidates.flatMap(({ environment, provider }) => {
    const primary = toWindowView(provider.rateLimits?.primary, "5 hour limit", nowMs);
    const secondary = toWindowView(provider.rateLimits?.secondary, "Weekly limit", nowMs);
    if (!primary && !secondary) {
      return [];
    }
    const remaining = worstRemaining(primary, secondary);
    const { title, subtitle } = accountTitleParts(provider, {
      includeEnvironmentLabel,
      environmentLabel: environment.environmentLabel,
    });
    const view: CodexLimitsView = {
      viewKey: `${environment.environmentId}:${provider.instanceId}`,
      instanceId: provider.instanceId,
      title,
      subtitle,
      accentColor: provider.accentColor?.trim() || null,
      primary,
      secondary,
      worstRemainingPercent: remaining,
      isCritical: remaining <= CRITICAL_REMAINING_PERCENT,
      isDepleted: remaining === 0,
    };
    return [view];
  });
}
