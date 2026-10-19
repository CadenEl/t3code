import {
  type EnvironmentId,
  OrchestratorMcpFailure,
  type OrchestratorMcpEnvironmentLinksResult,
  OrchestratorMcpThreadWaitInput,
  OrchestratorMcpThreadWaitResult,
} from "@t3tools/contracts";
import * as Clock from "effect/Clock";
import * as Context from "effect/Context";
import * as Crypto from "effect/Crypto";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Hex from "effect/encoding/Hex";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import type * as Tool from "effect/ai/Tool";
import * as ServerEnvironment from "../environment/ServerEnvironment.ts";
import { assertUnfencedCaller } from "../mcp/linkOrigin.ts";
import * as McpInvocationContext from "../mcp/McpInvocationContext.ts";
import { assertLiveCaller, readCaller, unavailable } from "../mcp/threadAccess.ts";
import * as ThreadManagement from "../orchestration-v2/ThreadManagementService.ts";
import * as PeerLinks from "./PeerLinks.ts";
import * as PeerMcpClient from "./PeerMcpClient.ts";

type Scope = McpInvocationContext.McpInvocationScope;

export class PeerForwarding extends Context.Service<
  PeerForwarding,
  {
    readonly call: <T extends Tool.Any>(
      scope: Scope,
      tool: T,
      environmentId: EnvironmentId,
      params: Tool.Parameters<T>,
    ) => Effect.Effect<Tool.Success<T>, OrchestratorMcpFailure>;
    readonly links: (
      scope: Scope,
    ) => Effect.Effect<OrchestratorMcpEnvironmentLinksResult, OrchestratorMcpFailure>;
  }
>()("t3/peer/PeerForwarding") {}

const make = Effect.gen(function* () {
  const peers = yield* PeerMcpClient.PeerMcpClient;
  const links = yield* PeerLinks.PeerLinks;
  const crypto = yield* Crypto.Crypto;
  const threads = yield* ThreadManagement.ThreadManagementService;
  const environment = yield* ServerEnvironment.ServerEnvironment;

  const callerContext = (scope: Scope) =>
    Effect.gen(function* () {
      const descriptor = yield* environment.getDescriptor;
      if (scope.environmentId !== descriptor.environmentId)
        return yield* new OrchestratorMcpFailure({
          code: "capability_denied",
          message: "This credential belongs to another environment.",
        });
      const caller = yield* readCaller();
      yield* assertLiveCaller(caller);
      return caller;
    }).pipe(
      Effect.provideService(McpInvocationContext.McpInvocationContext, scope),
      Effect.provideService(ThreadManagement.ThreadManagementService, threads),
    );

  const forward = <A>(
    scope: Scope,
    tool: string,
    parameters: Schema.Top,
    success: Schema.Top,
    environmentId: EnvironmentId,
    params: unknown,
  ) =>
    Effect.gen(function* () {
      const caller = yield* callerContext(scope);
      // A third environment would see this server's credential, losing the original fence.
      yield* assertUnfencedCaller(caller, "use this environment's links");
      const { limits } = caller;
      const encoded = yield* Schema.encodeUnknownEffect(parameters as Schema.Encoder<unknown>)(
        params,
      ).pipe(Effect.mapError(unavailable));
      const { environmentId: _target, ...args } = encoded as Record<string, unknown>;
      if (Array.isArray(args.attachments) && args.attachments.length > 0)
        return yield* new OrchestratorMcpFailure({
          code: "invalid_request",
          message: "A launch in a linked environment cannot carry local attachment uploads.",
        });
      // The destination sees one paired session for all callers here.
      if (typeof args.clientRequestId === "string")
        args.clientRequestId = yield* crypto
          .digest(
            "SHA-256",
            new TextEncoder().encode(
              // @effect-diagnostics-next-line preferSchemaOverJson:off -- Hash known strings without replacing lone UTF-16 surrogates.
              JSON.stringify([scope.requestNamespace, args.clientRequestId]),
            ),
          )
          .pipe(
            Effect.map((digest) => `fwd:${Hex.encode(digest)}`),
            Effect.orDie,
          );
      return yield* peers.call({
        environmentId,
        tool,
        arguments: args,
        limits,
        success: success as Schema.Decoder<A>,
      });
    });

  const waitForThread = (
    scope: Scope,
    environmentId: EnvironmentId,
    input: OrchestratorMcpThreadWaitInput,
  ) =>
    Effect.gen(function* () {
      const budget = Math.min(3_600_000, Math.max(1, input.timeoutMs ?? 600_000));
      const deadline = (yield* Clock.currentTimeMillis) + budget;
      let pinnedRun = input.runId;
      let last: OrchestratorMcpThreadWaitResult | undefined;
      const expired = () =>
        last === undefined
          ? Effect.fail(
              new OrchestratorMcpFailure({
                code: "orchestration_error",
                message: "The linked environment did not answer before the wait deadline.",
              }),
            )
          : Effect.succeed({ ...last, timedOut: true });
      while (true) {
        const remaining = deadline - (yield* Clock.currentTimeMillis);
        if (remaining <= 0) return yield* expired();
        // Reserve time for probes and transport before the absolute deadline.
        const headroom = Math.min(3_000, Math.ceil(remaining / 10));
        const result = yield* forward<OrchestratorMcpThreadWaitResult>(
          scope,
          "t3_thread_wait",
          OrchestratorMcpThreadWaitInput,
          OrchestratorMcpThreadWaitResult,
          environmentId,
          {
            threadId: input.threadId,
            runId: pinnedRun,
            timeoutMs: Math.max(1, Math.min(50_000, remaining - headroom)),
          },
        ).pipe(Effect.timeoutOption(remaining));
        if (Option.isNone(result)) return yield* expired();
        last = result.value;
        if (pinnedRun === undefined && last.runId !== null) pinnedRun = last.runId;
        if (!last.timedOut || deadline - (yield* Clock.currentTimeMillis) <= headroom) return last;
        // A peer may answer a timeout immediately. Yield until time advances
        // rather than spin indefinitely while spending no deadline budget.
        yield* Effect.sleep(1);
      }
    });

  const call: PeerForwarding["Service"]["call"] = (scope, tool, environmentId, params) =>
    tool.name === "t3_thread_wait"
      ? (waitForThread(
          scope,
          environmentId,
          params as OrchestratorMcpThreadWaitInput,
        ) as Effect.Effect<Tool.Success<typeof tool>, OrchestratorMcpFailure>)
      : forward(scope, tool.name, tool.parametersSchema, tool.successSchema, environmentId, params);

  const listLinks: PeerForwarding["Service"]["links"] = (scope) =>
    Effect.gen(function* () {
      yield* callerContext(scope);
      const listed = yield* links.list.pipe(Effect.mapError(unavailable));
      return {
        environmentId: scope.environmentId,
        links: listed.map((link) => ({
          environmentId: link.environmentId,
          label: link.label,
          status: link.status,
          access: link.access,
          expiresAt: DateTime.formatIso(link.expiresAt),
          lastError: link.lastError,
        })),
      };
    });
  return PeerForwarding.of({ call, links: listLinks });
});

export const layer = Layer.effect(PeerForwarding, make);
