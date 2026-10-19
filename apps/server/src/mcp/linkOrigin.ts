import {
  linkOriginIsFenced,
  type OrchestrationV2LinkOrigin,
  type OrchestrationV2ThreadShell,
  OrchestratorMcpFailure,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";

import type { Caller } from "./threadAccess.ts";

export const callerLinkOrigin = (
  caller: Pick<Caller, "scope" | "caller">,
): OrchestrationV2LinkOrigin | undefined =>
  caller.caller?.linkOrigin ??
  (caller.scope.client?.linked === true
    ? {
        sessionId: caller.scope.client.sessionId,
        label: caller.scope.client.label,
        access: caller.scope.client.access,
      }
    : undefined);

const fencingOrigin = (caller: Pick<Caller, "scope" | "caller">) => {
  const origin = callerLinkOrigin(caller);
  return origin !== undefined && linkOriginIsFenced(origin) ? origin : undefined;
};

const denied = (origin: OrchestrationV2LinkOrigin, action: string) =>
  new OrchestratorMcpFailure({
    code: "capability_denied",
    message: `Work started from ${origin.label} through a linked environment cannot ${action}. Links paired with full access are exempt.`,
  });

export const assertLinkThreadTarget = (
  caller: Pick<Caller, "scope" | "caller">,
  target: Pick<OrchestrationV2ThreadShell, "linkOrigin">,
) => {
  const origin = fencingOrigin(caller);
  return origin === undefined || target.linkOrigin?.sessionId === origin.sessionId
    ? Effect.void
    : Effect.fail(denied(origin, "change threads outside its own link"));
};

export const assertUnfencedCaller = (caller: Pick<Caller, "scope" | "caller">, action: string) => {
  const origin = fencingOrigin(caller);
  return origin === undefined ? Effect.void : Effect.fail(denied(origin, action));
};
