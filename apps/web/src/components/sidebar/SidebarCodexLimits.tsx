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
  type CodexLimitWindowView,
  type CodexLimitsView,
} from "./SidebarCodexLimits.logic";

function LimitMeterRow({ window }: { window: CodexLimitWindowView }) {
  const usedForBar = Math.max(0, Math.min(100, window.usedPercent));
  const lowRemaining = window.remainingPercent <= 10;

  return (
    <div className="flex flex-col gap-1.5">
      <div className="flex items-baseline justify-between gap-3">
        <div className="text-xs font-medium text-foreground">{window.label}</div>
        <div
          className={cn(
            "text-[11px] font-medium tabular-nums",
            lowRemaining ? "text-warning" : "text-foreground",
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
            lowRemaining ? "bg-warning" : "bg-foreground/70",
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
    <div className="flex flex-col gap-3">
      {showTitle ? (
        <div className="truncate text-[11px] font-medium text-muted-foreground">{view.title}</div>
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

  if (views.length === 0) {
    return null;
  }

  const summaryRemaining = views
    .flatMap((view) => [view.primary?.remainingPercent, view.secondary?.remainingPercent])
    .filter((value): value is number => typeof value === "number");
  const lowestRemaining = summaryRemaining.length > 0 ? Math.min(...summaryRemaining) : null;

  return (
    <Popover>
      <PopoverTrigger
        render={
          <Button
            variant="ghost"
            size="sm"
            className="mb-1 h-8 w-full justify-start gap-2 px-2 text-xs text-muted-foreground hover:text-foreground"
            aria-label="Code usage limits"
          >
            <ChartNoAxesColumnIcon className="size-3.5 shrink-0 opacity-80" />
            <span className="min-w-0 flex-1 truncate text-left">Limits</span>
            {lowestRemaining !== null ? (
              <span
                className={cn(
                  "tabular-nums text-[11px]",
                  lowestRemaining <= 10 ? "text-warning" : "text-muted-foreground",
                )}
              >
                {lowestRemaining}%
              </span>
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
          {views.map((view) => (
            <CodexLimitsInstanceBlock key={view.viewKey} view={view} showTitle={views.length > 1} />
          ))}
          <a
            className="text-[11px] text-muted-foreground underline-offset-2 hover:text-foreground hover:underline"
            href="https://chatgpt.com/codex/settings/usage"
            target="_blank"
            rel="noreferrer"
          >
            View more usage stats
          </a>
        </div>
      </PopoverPopup>
    </Popover>
  );
});
