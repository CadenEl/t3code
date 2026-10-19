import {
  type EnvironmentId,
  type OrchestrationV2LinkOrigin,
  OrchestratorMcpEnvironmentTarget,
  OrchestratorMcpFailure,
  type ProviderInteractionMode,
  type RuntimeMode,
  type ThreadId,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Predicate from "effect/Predicate";
import * as PeerForwarding from "../peer/PeerForwarding.ts";
import * as Ref from "effect/Ref";
import * as Struct from "effect/Struct";
import type * as Layer from "effect/Layer";
import type * as Scope from "effect/Scope";
import type { Tool, Toolkit } from "effect/ai";

import {
  DispatchModeLimit,
  type DispatchModeRefusal,
} from "../orchestration-v2/DispatchModeLimit.ts";
import { assertLinkThreadTarget, assertUnfencedCaller, callerLinkOrigin } from "./linkOrigin.ts";
import * as McpInvocationContext from "./McpInvocationContext.ts";
import { resolveInteractionMode, resolveRuntimeMode } from "./OrchestratorMcpService.ts";
import {
  assertFullAccess,
  assertLiveCaller,
  assertTargetWithinLimits,
  type Caller,
  loadCaller,
  readCaller,
  unavailable,
} from "./threadAccess.ts";

/**
 * Their private constructors only stop the type checker; this also stops
 * `Reflect.construct` and friends at runtime.
 */
const builtHere: unique symbol = Symbol("t3/mcp/McpToolAccess/builtHere");

const refuseOutsideConstruction = (token: symbol) => {
  if (token !== builtHere) {
    throw new TypeError("Only McpToolAccess builds MCP tool declarations and handler layers.");
  }
};

let declare: <P, A, E, R>(
  handle: (params: P) => Effect.Effect<A, E, R>,
  remote?: RemoteCheck<P>,
) => Declaration<(params: P) => Effect.Effect<A, E, R>>;

type RemoteCheck<P> = (
  params: P,
) => Effect.Effect<void, OrchestratorMcpFailure, Effect.Services<typeof writingCaller>>;
let remoteCheckOf: <Handler>(declaration: Declaration<Handler>) => RemoteCheck<never> | undefined;

type CheckedHandlerOf<D> = D extends Declaration<infer Handler> ? Handler : never;

interface CheckedHandler extends Struct.Lambda {
  <Handler>(declaration: Declaration<Handler>): Handler;
  readonly "~lambda.out": CheckedHandlerOf<this["~lambda.in"]>;
}
let checkedHandler: CheckedHandler;

let handlersLayer: <Tools extends Record<string, Tool.Any>, EX, RX>(
  layer: Layer.Layer<Tool.HandlersFor<Tools>, EX, RX>,
) => HandlersLayer<Tools, EX, RX>;

/**
 * Effect's own registration functions accept any handler, so the
 * `t3code/no-raw-mcp-registration` lint rule keeps Effect's `McpServer` inside
 * McpHttpServer.
 */
export class Declaration<out Handler> {
  readonly #handle: Handler;
  readonly #remote: RemoteCheck<never> | undefined;
  private constructor(
    token: typeof builtHere,
    handle: Handler,
    remote: RemoteCheck<never> | undefined,
  ) {
    refuseOutsideConstruction(token);
    this.#handle = handle;
    this.#remote = remote;
  }
  static {
    declare = (handle, remote) => new Declaration(builtHere, handle, remote);
    remoteCheckOf = (declaration) => declaration.#remote;
    checkedHandler = Struct.lambda<CheckedHandler>((declaration) => declaration.#handle);
  }
}

const refuseReadOnlyClient = McpInvocationContext.McpInvocationContext.pipe(
  Effect.flatMap((scope) =>
    scope.client?.access === "read-only"
      ? Effect.fail(
          new OrchestratorMcpFailure({
            code: "capability_denied",
            message:
              "This tool changes the environment, and this MCP client was approved for read-only access.",
          }),
        )
      : Effect.void,
  ),
);

const writingCaller = refuseReadOnlyClient.pipe(
  Effect.andThen(loadCaller()),
  Effect.tap(assertLiveCaller),
);

const orchestratingCaller = refuseReadOnlyClient.pipe(
  Effect.andThen(readCaller()),
  Effect.tap(assertLiveCaller),
);

const requireThreadCaller = McpInvocationContext.McpInvocationContext.pipe(
  Effect.flatMap((scope) => McpInvocationContext.requireThreadScope(scope, "This tool")),
);

export const reads = <P, A, E, R>(handle: (params: P) => Effect.Effect<A, E, R>) =>
  declare(
    (params: P) => handle(params),
    () => Effect.void,
  );

export const readsAsCaller = <P, A, E, R>(handle: (params: P) => Effect.Effect<A, E, R>) =>
  declare((params: P) => requireThreadCaller.pipe(Effect.flatMap(() => handle(params))));

export const actsAsCaller = <P, A, E, R>(handle: (params: P) => Effect.Effect<A, E, R>) =>
  declare((params: P) =>
    requireThreadCaller.pipe(
      Effect.flatMap(() => writingCaller),
      Effect.flatMap(() => handle(params)),
    ),
  );

export const writes = <P, A, E, R>(
  handle: (params: P) => Effect.Effect<A, E, R>,
  options?: { readonly linkedCallers?: "allowed" },
) =>
  declare(
    (params: P) =>
      orchestratingCaller.pipe(
        Effect.tap((caller) =>
          options?.linkedCallers === "allowed"
            ? Effect.void
            : assertUnfencedCaller(caller, "change environment resources"),
        ),
        Effect.flatMap(() => handle(params)),
      ),
    () => orchestratingCaller.pipe(Effect.asVoid),
  );

/**
 * Unlike `writes` it does not need thread control; the handler checks its
 * capability, as the capability toolkits do.
 */
export const writesWithCapability = <P, A, E, R>(handle: (params: P) => Effect.Effect<A, E, R>) =>
  declare((params: P) =>
    writingCaller.pipe(
      Effect.tap((caller) => assertUnfencedCaller(caller, "change environment resources")),
      Effect.flatMap(() => handle(params)),
    ),
  );

export const writesThreads = <P, A, E, R>(
  threads: (params: P) => ReadonlyArray<ThreadId | undefined>,
  handle: (params: P) => Effect.Effect<A, E, R>,
) =>
  declare(
    (params: P) =>
      Effect.gen(function* () {
        const caller = yield* writingCaller;
        for (const requestedId of threads(params)) {
          const threadId = requestedId ?? caller.caller?.id;
          if (threadId === undefined) continue;
          const target =
            threadId === caller.caller?.id
              ? caller.caller
              : yield* caller.threads.getThreadShell(threadId).pipe(Effect.mapError(unavailable));
          if (target !== null && target.deletedAt === null) {
            yield* assertTargetWithinLimits(caller.limits, target);
            yield* assertLinkThreadTarget(caller, target);
          }
        }
        // The target's user can raise its modes after the check above; the
        // orchestrator checks again under the thread's lock and records its
        // refusal here, since handlers wrap dispatch errors their own way.
        const refused = yield* Ref.make<DispatchModeRefusal | undefined>(undefined);
        return yield* handle(params).pipe(
          Effect.provideService(DispatchModeLimit, { ...caller.limits, refused }),
          Effect.catch((error) =>
            Effect.flatMap(Ref.get(refused), (refusal) =>
              Effect.fail<E | OrchestratorMcpFailure>(
                refusal === undefined ? error : escalationDenied(refusal),
              ),
            ),
          ),
        );
      }),
    () => writingCaller.pipe(Effect.asVoid),
  );

const escalationDenied = (refusal: DispatchModeRefusal) =>
  new OrchestratorMcpFailure({
    code:
      refusal.mode === "runtime"
        ? "runtime_mode_escalation_denied"
        : "interaction_mode_escalation_denied",
    message: `Thread ${refusal.threadId} now runs in ${refusal.runtimeMode}/${refusal.interactionMode} mode, above this caller's. Its user changed it while this call ran.`,
  });

export interface StartedModes {
  readonly runtimeMode: RuntimeMode;
  readonly interactionMode: ProviderInteractionMode;
  readonly linkOrigin: OrchestrationV2LinkOrigin | undefined;
}

export const startsThreads = <P, A, E, R>(
  modes: (params: P) => {
    readonly runtimeMode?: RuntimeMode | undefined;
    readonly interactionMode?: ProviderInteractionMode | undefined;
  },
  handle: (params: P, modes: StartedModes) => Effect.Effect<A, E, R>,
  link: "stamped" | { readonly refused: string },
) =>
  declare(
    (params: P) =>
      Effect.gen(function* () {
        const caller = yield* writingCaller;
        const { limits } = caller;
        if (link !== "stamped") yield* assertUnfencedCaller(caller, link.refused);
        const requested = modes(params);
        const started: StartedModes = {
          linkOrigin: callerLinkOrigin(caller),
          runtimeMode: yield* resolveRuntimeMode(limits.runtimeMode, requested.runtimeMode),
          interactionMode: yield* resolveInteractionMode(
            limits.interactionMode,
            requested.interactionMode,
          ),
        };
        return yield* handle(params, started);
      }),
    (params: P) =>
      Effect.gen(function* () {
        const { limits } = yield* writingCaller;
        const requested = modes(params);
        yield* resolveRuntimeMode(limits.runtimeMode, requested.runtimeMode);
        yield* resolveInteractionMode(limits.interactionMode, requested.interactionMode);
      }),
  );

const fullAccessRequired =
  "Changing projects or environment settings needs a live full-access/default calling thread or a full-access client.";

/**
 * `check` re-checks the caller wherever the handler waits before writing, such
 * as for a lock, since the caller's modes can change meanwhile.
 */
export const writesEnvironment = <P, A, E, R>(
  handle: (
    params: P,
    check: Effect.Effect<Caller, OrchestratorMcpFailure, CheckServices>,
  ) => Effect.Effect<A, E, R>,
  options?: { readonly linkedCallers?: "allowed" },
) => {
  const check = orchestratingCaller.pipe(
    Effect.tap((caller) =>
      options?.linkedCallers === "allowed"
        ? Effect.void
        : assertUnfencedCaller(caller, "change projects or environment settings"),
    ),
    Effect.tap((caller) => assertFullAccess(caller, fullAccessRequired)),
  );
  return declare(
    (params: P) => check.pipe(Effect.flatMap(() => handle(params, check))),
    () =>
      orchestratingCaller.pipe(
        Effect.tap((caller) => assertFullAccess(caller, fullAccessRequired)),
        Effect.asVoid,
      ),
  );
};

type CheckServices = Effect.Services<typeof orchestratingCaller>;

type Declarations<Handlers> = {
  readonly [Name in keyof Handlers]: Declaration<Handlers[Name]> & NotCallable;
};

type NotCallable = { readonly call?: never } & { readonly apply?: never };

export type Handlers<Tools extends Record<string, Tool.Any>> = Declarations<
  Toolkit.HandlersFrom<Tools>
>;

export class HandlersLayer<Tools extends Record<string, Tool.Any>, EX = never, RX = never> {
  readonly #layer: Layer.Layer<Tool.HandlersFor<Tools>, EX, RX>;
  private constructor(
    token: typeof builtHere,
    layer: Layer.Layer<Tool.HandlersFor<Tools>, EX, RX>,
  ) {
    refuseOutsideConstruction(token);
    this.#layer = layer;
  }
  static {
    handlersLayer = (layer) => new HandlersLayer(builtHere, layer);
  }
  static layer<Tools extends Record<string, Tool.Any>, EX, RX>(
    handlers: HandlersLayer<Tools, EX, RX>,
  ) {
    return handlers.#layer;
  }
}

const checkedHandlers = <Handlers>(declarations: Declarations<Handlers>): Handlers =>
  Struct.map(declarations, checkedHandler);

/**
 * A tool runs in a linked environment when its parameters take this target
 * as `environmentId`. Other tools may name an environment for their own
 * reasons, such as the one to link or to move a thread to.
 */
const takesLinkedEnvironment = (tool: Tool.Any) => {
  const parameters: unknown = tool.parametersSchema;
  return (
    Predicate.hasProperty(parameters, "fields") &&
    Predicate.hasProperty(parameters.fields, "environmentId") &&
    parameters.fields.environmentId === OrchestratorMcpEnvironmentTarget
  );
};

const linksUnavailable = new OrchestratorMcpFailure({
  code: "orchestration_error",
  message: "This environment cannot reach linked environments.",
});

/**
 * Every tool that takes a linked environment runs there the same way: its
 * declaration checks the caller here, and the same call goes to the other
 * environment, which checks the rest. The handler here never sees
 * `environmentId`, so it cannot leak into what it writes.
 */
const routed = <Tools extends Record<string, Tool.Any>>(
  toolkit: Toolkit.Toolkit<Tools>,
  declarations: Handlers<Tools>,
  checked: Toolkit.HandlersFrom<Tools>,
) =>
  Effect.gen(function* () {
    // Optional so toolkits that never leave this environment build without it.
    const forwarding = yield* Effect.serviceOption(PeerForwarding.PeerForwarding);
    const handlers: Record<string, unknown> = { ...checked };
    for (const [name, declaration] of Object.entries<Declaration<unknown>>(declarations)) {
      const tool = toolkit.tools[name];
      if (tool === undefined || !takesLinkedEnvironment(tool)) continue;
      const remote = remoteCheckOf(declaration);
      if (remote === undefined)
        return yield* Effect.die(
          `${name} takes a linked environment, but it acts as its calling thread, which is here.`,
        );
      const here = handlers[name] as (
        params: object,
      ) => Effect.Effect<unknown, Tool.Failure<Tools[keyof Tools]>>;
      handlers[name] = ({ environmentId, ...params }: { readonly environmentId?: EnvironmentId }) =>
        Effect.gen(function* () {
          const scope = yield* McpInvocationContext.McpInvocationContext;
          const target = environmentId === scope.environmentId ? undefined : environmentId;
          if (target === undefined) return yield* here(params);
          yield* (remote as RemoteCheck<typeof params>)(params);
          if (Option.isNone(forwarding)) return yield* linksUnavailable;
          return yield* forwarding.value.call(scope, tool, target, {
            ...params,
            environmentId,
          });
        });
    }
    return handlers as Toolkit.HandlersFrom<Tools>;
  });

export const toLayer = <Tools extends Record<string, Tool.Any>, EX = never, RX = never>(
  toolkit: Toolkit.Toolkit<Tools>,
  build: Handlers<Tools> | Effect.Effect<Handlers<Tools>, EX, RX>,
): HandlersLayer<Tools, EX, Exclude<RX, Scope.Scope>> => {
  // Checked as soon as they are given, so a forged declaration fails here.
  const withDeclarations = (declarations: Handlers<Tools>) =>
    routed(toolkit, declarations, checkedHandlers(declarations));
  return handlersLayer(
    toolkit.toLayer(
      Effect.isEffect(build) ? Effect.flatMap(build, withDeclarations) : withDeclarations(build),
    ),
  );
};
