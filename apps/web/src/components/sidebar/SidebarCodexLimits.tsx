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
        "size-2 shrink-0 rounded-full ring-1 ring-border/60",
        isCritical && !accentColor ? "bg-warning" : "bg-muted-foreground/50",
      )}
      style={accentColor ? { backgroundColor: accentColor } : undefined}
    />
  );
}

function compactWindowLabel(label: string): string {
  if (label.startsWith("5 hour")) return "5 hour";
  if (label.startsWith("Weekly")) return "Weekly";
  return label.replace(/ limit$/i, "");
}

function LimitMeterRow({ window }: { window: CodexLimitWindowView }) {
  const usedForBar = Math.max(0, Math.min(100, window.usedPercent));

  return (
    <div
      className="grid grid-cols-[3.25rem_minmax(0,1fr)_2rem_minmax(4.5rem,auto)] items-center gap-x-1.5"
      title={[window.label, `${window.remainingPercent}% left`, window.resetsLabel]
        .filter(Boolean)
        .join(" · ")}
    >
      <div className="truncate text-[11px] text-muted-foreground">
        {compactWindowLabel(window.label)}
      </div>
      <div
        className="h-1 w-full overflow-hidden rounded-full bg-muted/60"
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
      <div
        className={cn(
          "text-right text-[11px] font-medium tabular-nums",
          window.isCritical ? "text-warning" : "text-foreground",
        )}
      >
        {window.remainingPercent}%
      </div>
      <div className="truncate text-right text-[10px] text-muted-foreground">
        {window.resetsLabel ?? "—"}
      </div>
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
      className={cn(
        "flex flex-col gap-1 rounded-md px-1.5 py-1",
        view.isCritical && "bg-warning/8",
      )}
    >
      {showTitle ? (
        <div className="flex min-w-0 items-center gap-1.5">
          <AccountMark accentColor={view.accentColor} isCritical={view.isCritical} />
          <div className="min-w-0 flex-1 truncate text-[11px] font-medium text-foreground">
            {view.title}
            {view.subtitle ? (
              <span className="font-normal text-muted-foreground"> · {view.subtitle}</span>
            ) : null}
          </div>
          {view.isDepleted ? (
            <span className="shrink-0 text-[9px] font-medium uppercase tracking-wide text-warning">
              Empty
            </span>
          ) : view.isCritical ? (
            <span className="shrink-0 text-[9px] font-medium uppercase tracking-wide text-warning">
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
        className="w-[19.5rem] max-w-none text-left whitespace-normal"
        viewportClassName="p-0"
      >
        <div className="flex flex-col gap-1 p-2">
          <div className="px-1 text-[11px] font-medium text-foreground">Code usage limits</div>
          {summary.alertMessage ? (
            <div className="rounded-md bg-warning/10 px-1.5 py-1 text-[10px] text-warning">
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
