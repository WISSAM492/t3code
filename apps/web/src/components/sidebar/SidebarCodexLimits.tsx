import { useAtomValue } from "@effect/atom-react";
import { ChartNoAxesColumnIcon } from "lucide-react";
import { memo, useMemo } from "react";

import { useEnvironments } from "../../state/environments";
import { environmentServerConfigsAtom } from "../../state/server";
import { cn } from "../../lib/utils";
import { Button } from "../ui/button";
import { Popover, PopoverPopup, PopoverTrigger } from "../ui/popover";
import {
  getCodexLimitsViewsForEnvironments,
  summarizeCodexLimitsViews,
  type CodexLimitWindowView,
  type CodexLimitsView,
} from "./SidebarCodexLimits.logic";

function AccountMark({
  accentColor,
  isCritical,
}: {
  accentColor: string | null;
  isCritical: boolean;
}) {
  return (
    <span
      aria-hidden
      className={cn(
        "mt-0.5 size-2.5 shrink-0 rounded-full ring-1 ring-border/60",
        isCritical && !accentColor ? "bg-warning" : "bg-muted-foreground/50",
      )}
      style={accentColor ? { backgroundColor: accentColor } : undefined}
    />
  );
}

function LimitMeterRow({ window }: { window: CodexLimitWindowView }) {
  const usedForBar = Math.max(0, Math.min(100, window.usedPercent));

  return (
    <div className="flex flex-col gap-1.5">
      <div className="flex items-baseline justify-between gap-3">
        <div className="text-xs font-medium text-foreground">{window.label}</div>
        <div
          className={cn(
            "text-[11px] font-medium tabular-nums",
            window.isCritical ? "text-warning" : "text-foreground",
          )}
        >
          {window.remainingPercent}% left
        </div>
      </div>
      <div
        className="h-1.5 w-full overflow-hidden rounded-full bg-muted/60"
        role="progressbar"
        aria-valuemin={0}
        aria-valuemax={100}
        aria-valuenow={window.remainingPercent}
        aria-label={`${window.label} remaining`}
      >
        <div
          className={cn(
            "h-full rounded-full transition-[width,background-color] duration-500 ease-out motion-reduce:transition-none",
            window.isCritical ? "bg-warning" : "bg-foreground/70",
          )}
          style={{ width: `${100 - usedForBar}%` }}
        />
      </div>
      {window.resetsLabel ? (
        <div className="text-[11px] text-muted-foreground">{window.resetsLabel}</div>
      ) : null}
    </div>
  );
}

function CodexLimitsInstanceBlock({
  view,
  showTitle,
}: {
  view: CodexLimitsView;
  showTitle: boolean;
}) {
  return (
    <div
      className={cn("flex flex-col gap-3 rounded-md p-2 -mx-1", view.isCritical && "bg-warning/8")}
    >
      {showTitle ? (
        <div className="flex items-start gap-2 min-w-0">
          <AccountMark accentColor={view.accentColor} isCritical={view.isCritical} />
          <div className="min-w-0 flex-1">
            <div className="truncate text-xs font-medium text-foreground">{view.title}</div>
            {view.subtitle ? (
              <div className="truncate text-[11px] text-muted-foreground">{view.subtitle}</div>
            ) : null}
          </div>
          {view.isDepleted ? (
            <span className="shrink-0 text-[10px] font-medium uppercase tracking-wide text-warning">
              Empty
            </span>
          ) : view.isCritical ? (
            <span className="shrink-0 text-[10px] font-medium uppercase tracking-wide text-warning">
              Low
            </span>
          ) : null}
        </div>
      ) : null}
      {view.primary ? <LimitMeterRow window={view.primary} /> : null}
      {view.secondary ? <LimitMeterRow window={view.secondary} /> : null}
    </div>
  );
}

export const SidebarCodexLimits = memo(function SidebarCodexLimits() {
  const serverConfigs = useAtomValue(environmentServerConfigsAtom);
  const { environments } = useEnvironments();

  const views = useMemo(() => {
    const labelById = new Map(
      environments.map((environment) => [environment.environmentId, environment.label] as const),
    );
    return getCodexLimitsViewsForEnvironments(
      [...serverConfigs].map(([environmentId, config]) => ({
        environmentId,
        environmentLabel: labelById.get(environmentId),
        providers: config.providers,
      })),
    );
  }, [environments, serverConfigs]);

  const summary = useMemo(() => summarizeCodexLimitsViews(views), [views]);

  if (views.length === 0) {
    return null;
  }

  return (
    <Popover>
      <PopoverTrigger
        render={
          <Button
            variant="ghost"
            size="sm"
            className={cn(
              "mb-1 h-8 w-full justify-start gap-2 px-2 text-xs",
              summary.hasCritical
                ? "text-warning hover:text-warning"
                : "text-muted-foreground hover:text-foreground",
            )}
            aria-label={
              summary.alertMessage
                ? `Code usage limits. ${summary.alertMessage}`
                : "Code usage limits"
            }
          >
            <span className="relative shrink-0">
              <ChartNoAxesColumnIcon className="size-3.5 opacity-80" />
              {summary.hasCritical ? (
                <span
                  aria-hidden
                  className="absolute -right-0.5 -top-0.5 size-1.5 rounded-full bg-warning"
                />
              ) : null}
            </span>
            <span className="min-w-0 flex-1 truncate text-left">Limits</span>
            {summary.displayPercent !== null ? (
              <span className="tabular-nums text-[11px]">{summary.displayPercent}%</span>
            ) : null}
          </Button>
        }
      />
      <PopoverPopup
        side="top"
        align="start"
        sideOffset={8}
        className="w-72 max-w-none text-left whitespace-normal"
        viewportClassName="p-0"
      >
        <div className="flex flex-col gap-3 p-3">
          <div className="text-xs font-medium text-foreground">Code usage limits</div>
          {summary.alertMessage ? (
            <div className="rounded-md bg-warning/10 px-2 py-1.5 text-[11px] text-warning">
              {summary.alertMessage}
            </div>
          ) : null}
          {views.map((view) => (
            <CodexLimitsInstanceBlock key={view.viewKey} view={view} showTitle={views.length > 1} />
          ))}
        </div>
      </PopoverPopup>
    </Popover>
  );
});
