import { expect, it } from "@effect/vitest";
import { NodeHttpServer } from "@effect/platform-node";
import * as NodeServices from "@effect/platform-node/NodeServices";
import {
  ChatAttachmentId,
  AuthSessionId,
  CommandId,
  EnvironmentHttpApi,
  EnvironmentId,
  OrchestratorMcpFailure,
  MessageId,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  RunId,
  ThreadId,
  type ExecutionEnvironmentDescriptor,
  type OrchestratorMcpThreadWaitResult,
} from "@t3tools/contracts";
import { createPendingAttachmentId, resolveAttachmentPath } from "../attachmentStore.ts";
import * as Clock from "effect/Clock";
import * as Context from "effect/Context";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as FileSystem from "effect/FileSystem";
import * as ChildProcessSpawner from "effect/process/ChildProcessSpawner";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Ref from "effect/Ref";
import * as Queue from "effect/Queue";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import * as TestClock from "effect/testing/TestClock";
import { McpSchema, McpServer } from "effect/ai";
import type * as Tool from "effect/ai/Tool";
import * as FetchHttpClient from "effect/http/FetchHttpClient";
import * as HttpRouter from "effect/http/HttpRouter";
import * as HttpServer from "effect/http/HttpServer";
import * as HttpServerRequest from "effect/http/HttpServerRequest";
import * as HttpServerResponse from "effect/http/HttpServerResponse";
import * as HttpApi from "effect/http-api/HttpApi";
import * as HttpApiBuilder from "effect/http-api/HttpApiBuilder";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import * as EnvironmentAuth from "../auth/EnvironmentAuth.ts";
import * as AuthHttp from "../auth/http.ts";
import * as McpOAuth from "../auth/McpOAuth.ts";
import * as McpOAuthHttp from "../auth/mcpOAuthHttp.ts";
import * as ServerSecretStore from "../auth/ServerSecretStore.ts";
import * as ServerConfig from "../config.ts";
import * as ServerEnvironment from "../environment/ServerEnvironment.ts";
import * as ServerHttp from "../http.ts";
import * as McpHttpServer from "../mcp/McpHttpServer.ts";
import * as McpInvocationContext from "../mcp/McpInvocationContext.ts";
import * as McpSessionRegistryTestkit from "../mcp/McpSessionRegistry.testkit.ts";
import * as OrchestratorMcpService from "../mcp/OrchestratorMcpService.ts";
import * as ThreadMetadataMcpService from "../mcp/ThreadMetadataMcpService.ts";
import * as ProjectHandlers from "../mcp/toolkits/project/handlers.ts";
import { ProjectToolkit } from "../mcp/toolkits/project/tools.ts";
import { OrchestratorToolkit } from "../mcp/toolkits/orchestrator/tools.ts";
import { ThreadToolkit } from "../mcp/toolkits/thread/tools.ts";
import { EnvironmentToolkit } from "../mcp/toolkits/environment/tools.ts";
import * as CommandReceiptStore from "../orchestration-v2/CommandReceiptStore.ts";
import * as ProviderAdapterRegistry from "../orchestration-v2/ProviderAdapterRegistry.ts";
import { CodexProviderCapabilitiesV2 } from "../orchestration-v2/Adapters/CodexAdapterV2.ts";
import * as ProviderReplayHarness from "../orchestration-v2/testkit/ProviderReplayHarness.ts";
import * as RuntimeLayer from "../orchestration-v2/runtimeLayer.ts";
import * as ThreadCommandExecutor from "../orchestration-v2/ThreadCommandExecutor.ts";
import * as ThreadLaunch from "../orchestration-v2/ThreadLaunchService.ts";
import * as ThreadManagement from "../orchestration-v2/ThreadManagementService.ts";
import * as ThreadSearch from "../orchestration-v2/ThreadSearch.ts";
import * as Sqlite from "../persistence/Sqlite.ts";
import * as ProjectService from "../project/ProjectService.ts";
import * as ProjectEnrichment from "../project/ProjectEnrichmentService.ts";
import * as RepositoryIdentityResolver from "../project/RepositoryIdentityResolver.ts";
import * as ProjectFaviconResolver from "../project/ProjectFaviconResolver.ts";
import * as ManagedProjectFolders from "../project/ManagedProjectFolders.ts";
import * as ProjectCloneTracker from "../project/ProjectCloneTracker.ts";
import * as WorktreeSetupTracker from "../project/WorktreeSetupTracker.ts";
import * as ProjectSetupScriptRunner from "../project/ProjectSetupScriptRunner.ts";
import * as ProviderRegistryMock from "../provider/testUtils/providerRegistryMock.ts";
import * as GitWorkflow from "../git/GitWorkflowService.ts";
import * as GitVcsDriver from "../vcs/GitVcsDriver.ts";
import * as Repositories from "../sourceControl/SourceControlRepositoryService.ts";
import * as SourceControlProviderRegistry from "../sourceControl/SourceControlProviderRegistry.ts";
import * as ScheduledTasks from "../scheduledTasks/ScheduledTaskService.ts";
import * as Scheduler from "../scheduling/Scheduler.ts";
import * as AttachmentUpload from "../assets/AttachmentUpload.ts";
import { AttachmentToolkit } from "../mcp/toolkits/attachment/tools.ts";
import * as SecretRequests from "../secrets/SecretRequests.ts";
import * as ServerSettings from "../serverSettings.ts";
import * as TerminalManager from "../terminal/Manager.ts";
import * as TextGeneration from "../textGeneration/TextGeneration.ts";
import * as WorkspacePaths from "../workspace/WorkspacePaths.ts";
import * as PeerLinks from "./PeerLinks.ts";
import * as PeerMcpClient from "./PeerMcpClient.ts";
import * as PeerForwarding from "./PeerForwarding.ts";

class PeerApi extends HttpApi.make("environment")
  .add(EnvironmentHttpApi.groups.metadata)
  .add(EnvironmentHttpApi.groups.mcpOAuth) {}
const destinationId = EnvironmentId.make("destination");
const sourceId = EnvironmentId.make("source");
const modelSelection = { instanceId: ProviderInstanceId.make("codex"), model: "gpt-5.1-codex" };
const descriptor = (environmentId: EnvironmentId): ExecutionEnvironmentDescriptor => ({
  environmentId,
  label: environmentId,
  platform: { os: "linux", arch: "x64" },
  serverVersion: "0.0.0-test",
  // Only the fixture advertises compatibility with the future receiving fence.
  capabilities: { repositoryIdentity: true, mcpModeLimitHeader: true, linkFence: true },
});
const environmentLayer = (value: Effect.Effect<ExecutionEnvironmentDescriptor>) =>
  Layer.succeed(ServerEnvironment.ServerEnvironment, {
    getDescriptor: value,
    getEnvironmentId: value.pipe(Effect.map((d) => d.environmentId)),
  });
const decodeFailure = Schema.decodeUnknownSync(Schema.fromJsonString(OrchestratorMcpFailure));
const client = McpSchema.McpServerClient.of({
  clientId: 1,
  clientCapabilities: {},
  clientInfo: { name: "test", version: "1" },
  protocolVersion: "2025-06-18",
  initializePayload: {
    protocolVersion: "2025-06-18",
    capabilities: {},
    clientInfo: { name: "test", version: "1" },
  },
  getClient: Effect.die("unused"),
});
const scope = (
  name = "A",
  access: "full-access" | "auto" | "read-only" = "full-access",
): McpInvocationContext.McpInvocationScope => ({
  environmentId: sourceId,
  requestNamespace: `client:${name}`,
  thread: undefined,
  client: { sessionId: name, label: name, access },
  capabilities: new Set(["orchestration"]),
  issuedAt: 1,
});
const registration = Layer.mergeAll(
  McpHttpServer.layerOrchestratorToolkit,
  McpHttpServer.layerThreadToolkit,
  McpHttpServer.layerEnvironmentToolkit,
  McpHttpServer.layerAttachmentToolkit,
  McpHttpServer.toolkitRegistration(ProjectToolkit, ProjectHandlers.layer),
);

const makeEnvironment = Effect.fnUntraced(function* (id: EnvironmentId) {
  const fs = yield* FileSystem.FileSystem;
  const root = yield* fs.makeTempDirectoryScoped({ prefix: "t3-forwarding-" });
  const described = yield* Ref.make(descriptor(id));
  const database = Sqlite.layerMemory;
  const config = ServerConfig.layerTest(root, { prefix: "t3-forwarding-state-" });
  const registry = ProviderAdapterRegistry.layerFromAdapters([
    {
      instanceId: modelSelection.instanceId,
      driver: ProviderDriverKind.make("codex"),
      getCapabilities: () => Effect.succeed(CodexProviderCapabilitiesV2),
      planSelectionTransition: () => Effect.succeed({ type: "apply_on_next_turn" as const }),
      openSession: () => Effect.die("provider execution is disabled"),
    } as Parameters<typeof ProviderAdapterRegistry.layerFromAdapters>[0][number],
  ]);
  // Persist accepted runs/outbox work, but do not execute external provider turns.
  const orchestrator = ProviderReplayHarness.layerWithRegistry({ name: `peer-${id}` }, registry, {
    databaseLayer: database,
    runEffectWorker: false,
  });
  const settings = ServerSettings.layerTest();
  const receipts = CommandReceiptStore.layer.pipe(Layer.provide(database));
  const threads = ThreadManagement.layer.pipe(
    Layer.provide(Layer.mergeAll(orchestrator, settings, receipts)),
  );
  const projects = RuntimeLayer.layerProjectService.pipe(
    Layer.provideMerge(ProjectEnrichment.layer),
    Layer.provide(
      Layer.mergeAll(
        WorkspacePaths.layer,
        Layer.succeed(RepositoryIdentityResolver.RepositoryIdentityResolver, {
          resolve: () => Effect.succeed(null),
        }),
        Layer.succeed(ProjectFaviconResolver.ProjectFaviconResolver, {
          resolvePath: () => Effect.succeed(null),
        }),
        ThreadCommandExecutor.layer,
      ),
    ),
    Layer.provide(settings),
    Layer.provide(database),
    Layer.provide(config),
    Layer.provide(NodeServices.layer),
  );
  const external = Layer.mergeAll(
    WorktreeSetupTracker.layer,
    ProviderRegistryMock.layer([
      {
        instanceId: modelSelection.instanceId,
        driver: ProviderDriverKind.make("codex"),
        displayName: `${id} Codex`,
        enabled: true,
        installed: true,
        version: "test",
        status: "ready",
        auth: { status: "authenticated" },
        checkedAt: "2026-09-13T00:00:00.000Z",
        models: [
          {
            slug: modelSelection.model,
            name: modelSelection.model,
            isCustom: false,
            capabilities: null,
          },
          { slug: `${id}-custom`, name: `${id} custom`, isCustom: true, capabilities: null },
        ],
        slashCommands: [],
        skills: [],
      },
    ]),
    Layer.mock(ProjectCloneTracker.ProjectCloneTracker)({ get: () => Effect.succeed(null) }),
    Layer.mock(TerminalManager.TerminalManager)({ close: () => Effect.void }),
    Layer.mock(GitWorkflow.GitWorkflowService)({ hasCommit: () => Effect.succeed(true) }),
    Layer.succeed(ProjectSetupScriptRunner.ProjectSetupScriptRunner, {
      runForThread: () => Effect.succeed({ status: "no-script" as const }),
    }),
    Layer.mock(TextGeneration.TextGeneration)({}),
    Layer.mock(ManagedProjectFolders.ManagedProjectFolders)({
      namedProjectsRoot: `${root}/projects`,
      folderForThread: () => Effect.succeed(Option.none()),
    }),
    Layer.mock(GitVcsDriver.GitVcsDriver)({
      execute: (input) =>
        fs.makeDirectory(`${input.cwd}/${input.args.at(-1)}`, { recursive: true }).pipe(
          Effect.andThen(
            fs.writeFileString(
              `${input.cwd}/${input.args.at(-1)}/cloned.txt`,
              "controlled git clone",
            ),
          ),
          Effect.as({
            stdout: "",
            stderr: "",
            exitCode: ChildProcessSpawner.ExitCode(0),
            stdoutTruncated: false,
            stderrTruncated: false,
          }),
          Effect.orDie,
        ),
    }),
    Layer.mock(SourceControlProviderRegistry.SourceControlProviderRegistry)({
      resolveLink: () => undefined,
    }),
    Layer.mock(SecretRequests.SecretRequests)({}),
    Layer.mock(ThreadSearch.ThreadSearch)({}),
  );
  const base = Layer.mergeAll(
    threads,
    projects,
    registry,
    settings,
    receipts,
    external,
    IdAllocator.layer,
    ThreadCommandExecutor.layer,
  );
  const launch = ThreadLaunch.layer.pipe(Layer.provide(base));
  const schedules = ScheduledTasks.layer.pipe(
    Layer.provide(Layer.mergeAll(launch, base, Scheduler.layer)),
    Layer.provide(database),
  );
  const business = Layer.mergeAll(
    OrchestratorMcpService.layer.pipe(
      Layer.provide(schedules),
      Layer.provide(launch),
      Layer.provide(base),
    ),
    ThreadMetadataMcpService.layer.pipe(Layer.provide(threads)),
    Repositories.layer.pipe(Layer.provide(external)),
    launch,
    schedules,
    base,
    McpOAuth.layerMcpClientAuthenticator.pipe(Layer.provideMerge(EnvironmentAuth.layer)),
  ).pipe(
    Layer.provideMerge(ServerSecretStore.layer),
    Layer.provideMerge(ServerEnvironment.layerIdentity),
    Layer.provideMerge(database),
    Layer.provideMerge(environmentLayer(Ref.get(described))),
    Layer.provideMerge(config),
    Layer.provideMerge(NodeServices.layer),
    Layer.fresh,
  );
  const context = yield* Layer.build(business);
  const projectService = Context.get(context, ProjectService.ProjectService);
  yield* projectService.create({
    commandId: CommandId.make(`${id}:project`),
    projectId: ProjectId.make(`${id}:project`),
    workspaceRoot: root,
    title: `${id} project`,
  });
  const project = yield* projectService.update({
    commandId: CommandId.make(`${id}:defaults`),
    projectId: ProjectId.make(`${id}:project`),
    defaultModelSelection: modelSelection,
  });
  const threadService = Context.get(context, ThreadManagement.ThreadManagementService);
  const seed = Effect.fnUntraced(function* (
    name: string,
    runtimeMode: "auto" | "full-access" = "full-access",
    interactionMode: "default" | "plan" = "default",
  ) {
    const threadId = ThreadId.make(`${id}:${name}`);
    yield* threadService.dispatch({
      type: "thread.create",
      commandId: CommandId.make(`${threadId}:create`),
      threadId,
      projectId: project.id,
      title: name,
      modelSelection,
      runtimeMode,
      interactionMode,
      branch: null,
      worktreePath: root,
      createdBy: "user",
      creationSource: "web",
    });
    return threadId;
  });
  const caller = Effect.fnUntraced(function* (
    name: string,
    runtimeMode: "auto" | "full-access" = "full-access",
    interactionMode: "default" | "plan" = "default",
  ) {
    const threadId = yield* seed(name, runtimeMode, interactionMode);
    yield* threadService.dispatch({
      type: "message.dispatch",
      commandId: CommandId.make(`${threadId}:start`),
      threadId,
      messageId: MessageId.make(`${threadId}:start`),
      text: "source stays here",
      attachments: [],
      modelSelection,
      dispatchMode: { type: "start_immediately" },
      createdBy: "user",
      creationSource: "web",
    });
    return {
      ...scope(name),
      environmentId: id,
      client: undefined,
      thread: {
        threadId,
        providerInstanceId: modelSelection.instanceId,
        providerSessionId: `live:${threadId}`,
      },
    } satisfies McpInvocationContext.McpInvocationScope;
  });
  return {
    caller,
    context,
    root,
    described,
    project,
    projects: projectService,
    threads: threadService,
    seed,
    auth: Context.get(context, EnvironmentAuth.EnvironmentAuth),
    sql: Context.get(context, SqlClient.SqlClient),
  };
});

type RequestRecord = {
  method: string | undefined;
  session: string | undefined;
  limit: string | undefined;
  body: unknown;
};
const serveDestination = Effect.fnUntraced(function* (
  destination: Effect.Success<ReturnType<typeof makeEnvironment>>,
) {
  const requests = yield* Ref.make<ReadonlyArray<RequestRecord>>([]);
  const bearers = yield* Ref.make(0);
  const probes = yield* Ref.make(0);
  const faults = yield* Ref.make<
    ReadonlyArray<"404" | "malformed" | "rpc-error" | { redirect: string }>
  >([]);
  const middleware = HttpRouter.middleware(
    (effect) =>
      Effect.gen(function* () {
        const request = yield* HttpServerRequest.HttpServerRequest;
        if (request.headers.authorization !== undefined) yield* Ref.update(bearers, (n) => n + 1);
        if (request.url.startsWith("/.well-known/t3/environment"))
          yield* Ref.update(probes, (n) => n + 1);
        if (request.url !== "/mcp") return yield* effect;
        const body = yield* request.json;
        const method = (body as { method?: string }).method;
        yield* Ref.update(requests, (seen) => [
          ...seen,
          {
            method,
            body,
            session: request.headers["mcp-session-id"],
            limit: request.headers["t3-mode-limit"],
          },
        ]);
        if (method === "tools/call") {
          const fault = yield* Ref.modify(faults, (items) => [items[0], items.slice(1)]);
          if (typeof fault === "object")
            return HttpServerResponse.redirect(fault.redirect, { status: 307 });
          if (fault === "404") return HttpServerResponse.empty({ status: 404 });
          if (fault === "malformed")
            return HttpServerResponse.text("private upstream secret", {
              contentType: "application/json",
            });
          if (fault === "rpc-error")
            return HttpServerResponse.jsonUnsafe({
              jsonrpc: "2.0",
              id: (body as { id: string | number }).id,
              error: { code: -32603, message: "private upstream secret" },
            });
        }
        return yield* effect;
      }),
    { global: true },
  );
  const routes = Layer.mergeAll(
    registration.pipe(
      Layer.provideMerge(McpHttpServer.layerMcpTransport),
      Layer.provide(McpSessionRegistryTestkit.layer),
    ),
    HttpApiBuilder.layer(PeerApi).pipe(
      Layer.provide(McpOAuthHttp.layer.pipe(Layer.provide(McpOAuth.layer))),
      Layer.provide(ServerHttp.layerServerEnvironmentHttpApi),
      Layer.provide(AuthHttp.layerAuthenticatedAuth),
    ),
  ).pipe(Layer.provide(middleware), Layer.provide(Layer.succeedContext(destination.context)));
  yield* HttpRouter.serve(routes, { disableListenLog: true, disableLogger: true }).pipe(
    Layer.build,
  );
  const address = (yield* HttpServer.HttpServer).address;
  if (address._tag === "UnixPathAddress") return yield* Effect.die("TCP expected");
  return { url: `http://127.0.0.1:${address.port}`, requests, faults, bearers, probes };
});

const paired = Effect.gen(function* () {
  const destination = yield* makeEnvironment(destinationId);
  const http = yield* serveDestination(destination);
  const source = yield* makeEnvironment(sourceId);
  const linksContext = yield* PeerLinks.layer.pipe(
    Layer.provide(FetchHttpClient.layer),
    Layer.provide(Layer.succeedContext(source.context)),
    Layer.build,
  );
  const links = Context.get(linksContext, PeerLinks.PeerLinks);
  const pair = Effect.fnUntraced(function* (
    access: "full-access" | "auto" | "read-only" = "full-access",
  ) {
    return yield* links.link({
      url: http.url,
      access,
      pairingCode: (yield* destination.auth.issuePairingCredential()).credential,
    });
  });
  yield* pair();
  const forwarding = PeerForwarding.layer.pipe(
    Layer.provide(PeerMcpClient.layer.pipe(Layer.provide(FetchHttpClient.layer))),
    Layer.provide(Layer.succeedContext(linksContext)),
    Layer.provide(Layer.succeedContext(source.context)),
  );
  const registered = yield* registration.pipe(
    Layer.provideMerge(McpServer.McpServer.layer),
    Layer.provideMerge(forwarding),
    Layer.provide(Layer.succeedContext(source.context)),
    Layer.build,
  );
  const server = Context.get(registered, McpServer.McpServer);
  const invoke = <T extends Tool.Any>(tool: T, args: Record<string, unknown>, caller = scope()) =>
    server
      .callTool({
        name: tool.name,
        arguments: args,
      })
      .pipe(
        Effect.provideService(McpInvocationContext.McpInvocationContext, caller),
        Effect.provideService(McpSchema.McpServerClient, client),
        Effect.map((result) =>
          result.isError === true
            ? {
                ...result,
                structuredContent: decodeFailure(
                  result.content
                    .flatMap((part) => (part.type === "text" ? [part.text] : []))
                    .join("\n"),
                ),
              }
            : result,
        ),
      );
  const call = <T extends Tool.Any>(tool: T, args: Tool.Parameters<T>, caller = scope()) =>
    invoke(tool, args as Record<string, unknown>, caller).pipe(
      Effect.flatMap((result) => {
        expect(result.isError, JSON.stringify(result)).not.toBe(true);
        // oxlint-disable-next-line t3code/no-inline-schema-compile -- Each actual tool supplies its result schema.
        return Schema.decodeUnknownEffect(tool.successSchema as Schema.Decoder<Tool.Success<T>>)(
          result.structuredContent,
        );
      }),
    );
  return { source, destination, http, links, pair, call, invoke };
});

it.effect("fences linked work by its immutable session while preserving local exceptions", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const f = yield* paired;
      const fs = yield* FileSystem.FileSystem;
      const userThread = yield* f.source.seed("user-auto", "auto");
      const broadThread = yield* f.source.seed("user-full");
      const remoteThread = yield* f.destination.seed("third-hop");
      const authenticate = Effect.fnUntraced(function* (
        label: string,
        access: "auto" | "full-access",
      ) {
        const issued = yield* f.source.auth.issueMcpClientSession({
          label,
          access,
          linked: true,
          client: { deviceType: "bot" },
        });
        const authenticated = yield* Context.get(
          f.source.context,
          McpHttpServer.McpClientAuthenticator,
        ).authenticate(
          HttpServerRequest.fromWeb(
            new Request("http://localhost/mcp", {
              headers: { authorization: `Bearer ${issued.token}` },
            }),
          ),
        );
        if (authenticated === undefined)
          return yield* Effect.die("Signed linked session was rejected");
        return authenticated;
      });
      const auto = yield* authenticate("Laptop", "auto");
      const other = yield* authenticate("Other laptop", "auto");
      const full = yield* authenticate("Trusted desktop", "full-access");
      const launch = (caller: McpInvocationContext.McpInvocationScope, title: string) =>
        f.call(
          ProjectToolkit.tools.t3_thread_launch,
          {
            projectId: f.source.project.id,
            title,
            message: "Start work",
            clientRequestId: title,
          },
          caller,
        );
      const own = yield* launch(auto, "own");
      const sibling = yield* launch(auto, "same-session-sibling");
      const foreign = yield* launch(other, "foreign");
      const trusted = yield* launch(full, "trusted");
      const asThread = (threadId: ThreadId): McpInvocationContext.McpInvocationScope => ({
        ...scope(),
        client: undefined,
        requestNamespace: `provider:${threadId}`,
        thread: {
          threadId,
          providerInstanceId: modelSelection.instanceId,
          providerSessionId: `live:${threadId}`,
        },
      });
      const ownAgent = asThread(own.threadId);
      const trustedAgent = asThread(trusted.threadId);
      const denied = { isError: true, structuredContent: { code: "capability_denied" } };
      const send = (threadId: ThreadId, key: string) => ({
        threadId,
        message: key,
        clientRequestId: key,
      });
      yield* f.call(
        OrchestratorToolkit.tools.t3_thread_send,
        send(sibling.threadId, "same-session"),
        ownAgent,
      );
      const userBefore = yield* f.source.threads.getThreadProjection(userThread);
      const foreignBefore = yield* f.source.threads.getThreadProjection(foreign.threadId);
      const ownOrigin = { sessionId: auto.client!.sessionId, label: "Laptop", access: "auto" };
      expect
        .soft((yield* f.source.threads.getThreadProjection(own.threadId)).thread)
        .toMatchObject({ linkOrigin: ownOrigin });
      // Both omitted and explicit own environment IDs use the same fence.
      for (const caller of [auto, ownAgent]) {
        yield* f.call(
          OrchestratorToolkit.tools.t3_thread_read,
          { environmentId: sourceId, threadId: userThread },
          caller,
        );
        yield* f.call(
          OrchestratorToolkit.tools.t3_thread_send,
          send(own.threadId, `${caller.requestNamespace}:own`),
          caller,
        );
        for (const threadId of [userThread, foreign.threadId]) {
          expect
            .soft(
              yield* f.invoke(
                OrchestratorToolkit.tools.t3_thread_send,
                {
                  ...send(threadId, `${caller.requestNamespace}:forbidden:${threadId}`),
                  environmentId: sourceId,
                },
                caller,
              ),
            )
            .toMatchObject(denied);
        }
      }
      const queued = yield* f.call(
        OrchestratorToolkit.tools.t3_thread_send,
        {
          ...send(own.threadId, "queue"),
          mode: "queue",
        },
        ownAgent,
      );
      yield* f.call(
        ThreadToolkit.tools.t3_queue_edit,
        { threadId: own.threadId, queuedRunId: queued.runId, text: "edited" },
        ownAgent,
      );
      expect(
        (yield* f.source.threads.getThreadProjection(own.threadId)).messages.find(
          (message) => message.id === queued.messageId,
        )?.text,
      ).toBe("edited");
      expect
        .soft(
          yield* f.invoke(
            OrchestratorToolkit.tools.t3_thread_update,
            {
              threadId: userThread,
              action: "rename",
              title: "Forbidden rename",
            },
            ownAgent,
          ),
        )
        .toMatchObject(denied);
      expect.soft(yield* f.source.threads.getThreadProjection(userThread)).toEqual(userBefore);
      expect
        .soft(yield* f.source.threads.getThreadProjection(foreign.threadId))
        .toEqual(foreignBefore);

      yield* f.source.threads.dispatch({
        type: "thread.runtime-mode.set",
        commandId: CommandId.make("upgrade-linked"),
        threadId: own.threadId,
        runtimeMode: "full-access",
      });
      expect
        .soft(
          yield* f.invoke(
            OrchestratorToolkit.tools.t3_thread_send,
            send(userThread, "upgraded-still-fenced"),
            ownAgent,
          ),
        )
        .toMatchObject(denied);
      const projectBefore = yield* f.source.projects.getById(f.source.project.id);
      expect
        .soft(
          yield* f.invoke(
            ProjectToolkit.tools.t3_project_update,
            { projectId: f.source.project.id, title: "Forbidden project edit" },
            ownAgent,
          ),
        )
        .toMatchObject(denied);
      expect.soft(yield* f.source.projects.getById(f.source.project.id)).toEqual(projectBefore);
      const schedule = {
        projectId: f.source.project.id,
        prompt: "Later",
        schedule: { type: "interval" as const, everyMs: 3600000 },
        enabled: false,
      };
      const scheduled = yield* f.call(OrchestratorToolkit.tools.schedule_task, schedule);
      const schedules = Context.get(f.source.context, ScheduledTasks.ScheduledTaskService);
      const schedulesBefore = yield* schedules.list();
      expect
        .soft(
          yield* f.invoke(
            OrchestratorToolkit.tools.schedule_task,
            { ...schedule, clientRequestId: "forbidden-schedule" },
            ownAgent,
          ),
        )
        .toMatchObject(denied);
      expect
        .soft(
          yield* f.invoke(
            OrchestratorToolkit.tools.delete_scheduled_task,
            { scheduledTaskId: scheduled.scheduledTaskId },
            ownAgent,
          ),
        )
        .toMatchObject(denied);
      expect.soft(yield* schedules.list()).toEqual(schedulesBefore);

      const cloneArgs = {
        remoteUrl: "https://example.com/team/repo.git",
        destinationPath: `${f.source.root}/linked-clone`,
      };
      expect(yield* f.invoke(ProjectToolkit.tools.t3_project_clone, cloneArgs, auto)).toMatchObject(
        denied,
      );
      expect(
        yield* f.invoke(
          ProjectToolkit.tools.t3_project_create,
          { title: "No", workspaceRoot: `${f.source.root}/no`, createWorkspaceRootIfMissing: true },
          auto,
        ),
      ).toMatchObject(denied);
      const cloned = yield* f.call(ProjectToolkit.tools.t3_project_clone, cloneArgs, ownAgent);
      const registered = yield* f.call(
        ProjectToolkit.tools.t3_project_create,
        { title: "Allowed new project", workspaceRoot: cloned.cwd },
        ownAgent,
      );
      expect(yield* fs.readFileString(`${cloned.cwd}/cloned.txt`)).toBe("controlled git clone");
      expect(Option.getOrThrow(yield* f.source.projects.getById(registered.id)).workspaceRoot).toBe(
        cloned.cwd,
      );

      const upload = {
        type: "file" as const,
        name: "note.txt",
        mimeType: "text/plain",
        sizeBytes: 4,
      };
      const stage = Effect.fnUntraced(function* () {
        const prepared = yield* f.call(
          AttachmentToolkit.tools.t3_attachment_prepare_upload,
          { upload },
          ownAgent,
        );
        const claims = yield* AttachmentUpload.validateAttachmentUploadToken(
          prepared.relativeUrl.split("/").at(-1)!,
        ).pipe(Effect.provide(f.source.context));
        if (claims === null) return yield* Effect.die("Upload token did not validate");
        expect(
          yield* AttachmentUpload.storeAttachmentUpload(
            claims,
            new Uint8Array([116, 101, 115, 116]),
          ).pipe(Effect.provide(f.source.context)),
        ).toEqual({ ok: true });
        return { ...upload, id: ChatAttachmentId.make(prepared.attachmentId) };
      });
      const attachment = yield* stage();
      const config = Context.get(f.source.context, ServerConfig.ServerConfig);
      const pendingPath = resolveAttachmentPath({
        attachmentsDir: config.attachmentsDir,
        attachment,
      })!;
      expect(
        yield* f.invoke(
          AttachmentToolkit.tools.t3_attachment_discard,
          { attachmentId: attachment.id },
          other,
        ),
      ).toMatchObject({ isError: true, structuredContent: { code: "invalid_request" } });
      expect
        .soft(
          yield* f.invoke(
            AttachmentToolkit.tools.t3_thread_send_attachments,
            { threadId: userThread, attachments: [attachment] },
            ownAgent,
          ),
        )
        .toMatchObject(denied);
      expect.soft(yield* fs.exists(pendingPath)).toBe(true);
      // A distinct pending upload keeps the successful claim independent of the refused send.
      const owned = yield* stage();
      const sent = yield* f.call(
        AttachmentToolkit.tools.t3_thread_send_attachments,
        { threadId: own.threadId, attachments: [owned] },
        ownAgent,
      );
      const stored = (yield* f.source.threads.getThreadProjection(own.threadId)).messages.find(
        (message) => message.id === sent.messageId,
      )!.attachments[0]!;
      expect(
        yield* fs.readFileString(
          resolveAttachmentPath({ attachmentsDir: config.attachmentsDir, attachment: stored })!,
        ),
      ).toBe("test");
      expect(
        yield* f.invoke(
          AttachmentToolkit.tools.t3_thread_send_attachments,
          { threadId: foreign.threadId, attachments: [stored] },
          other,
        ),
      ).toMatchObject({ isError: true, structuredContent: { code: "invalid_request" } });
      yield* f.call(
        AttachmentToolkit.tools.t3_attachment_discard,
        { attachmentId: attachment.id },
        ownAgent,
      );
      expect(yield* fs.exists(pendingPath)).toBe(false);

      for (const caller of [auto, ownAgent]) {
        yield* Ref.set(f.http.requests, []);
        yield* Ref.set(f.http.bearers, 0);
        expect
          .soft(
            yield* f.invoke(
              OrchestratorToolkit.tools.t3_thread_read,
              { environmentId: destinationId, threadId: remoteThread },
              caller,
            ),
          )
          .toMatchObject(denied);
        expect.soft(yield* Ref.get(f.http.requests)).toEqual([]);
        expect.soft(yield* Ref.get(f.http.bearers)).toBe(0);
      }
      // Old persisted origins have no access member and remain fenced in a full-access thread.
      yield* f.source.sql`UPDATE orchestration_v2_projection_threads
      SET payload_json = json_remove(payload_json, '$.linkOrigin.access') WHERE thread_id = ${own.threadId}`;
      expect
        .soft(
          yield* f.invoke(
            OrchestratorToolkit.tools.t3_thread_send,
            send(userThread, "legacy-still-fenced"),
            ownAgent,
          ),
        )
        .toMatchObject(denied);
      yield* Ref.set(f.http.bearers, 0);
      expect
        .soft(
          yield* f.invoke(
            OrchestratorToolkit.tools.t3_thread_read,
            { environmentId: destinationId, threadId: remoteThread },
            ownAgent,
          ),
        )
        .toMatchObject(denied);
      expect.soft(yield* Ref.get(f.http.bearers)).toBe(0);
      for (const caller of [full, trustedAgent]) {
        yield* f.call(
          OrchestratorToolkit.tools.t3_thread_send,
          send(userThread, `${caller.requestNamespace}:exempt`),
          caller,
        );
        yield* f.call(
          OrchestratorToolkit.tools.t3_thread_read,
          { environmentId: destinationId, threadId: remoteThread },
          caller,
        );
      }
      yield* f.source.threads.dispatch({
        type: "thread.runtime-mode.set",
        commandId: CommandId.make("narrow-trusted"),
        threadId: trusted.threadId,
        runtimeMode: "auto",
      });
      expect(
        yield* f.invoke(
          OrchestratorToolkit.tools.t3_thread_send,
          send(broadThread, "trusted-mode-ceiling"),
          trustedAgent,
        ),
      ).toMatchObject({
        isError: true,
        structuredContent: { code: "runtime_mode_escalation_denied" },
      });
      expect(
        yield* f.invoke(
          OrchestratorToolkit.tools.t3_thread_send,
          { ...send(remoteThread, "trusted-downstream-ceiling"), environmentId: destinationId },
          trustedAgent,
        ),
      ).toMatchObject({
        isError: true,
        structuredContent: { code: "runtime_mode_escalation_denied" },
      });
      expect
        .soft((yield* f.source.threads.getThreadProjection(trusted.threadId)).thread)
        .toMatchObject({
          linkOrigin: {
            sessionId: full.client!.sessionId,
            label: "Trusted desktop",
            access: "full-access",
          },
        });
    }),
  ).pipe(Effect.provide(Layer.mergeAll(NodeHttpServer.layerTest, NodeServices.layer))),
);

it.effect(
  "routes registered tools over paired HTTP with durable caller namespaces and destination effects",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const f = yield* paired;
        const target = yield* f.destination.seed("target");
        const liveCaller = yield* f.source.caller("local");
        const otherCaller = yield* f.source.caller("other");
        const local = liveCaller.thread.threadId;
        const before = yield* f.source.threads.getThreadProjection(local);
        const links = yield* f.call(EnvironmentToolkit.tools.t3_environment_links, {}, liveCaller);
        expect(links).toMatchObject({
          environmentId: sourceId,
          links: [{ environmentId: destinationId, status: "reachable" }],
        });
        const localCapabilities = yield* f.call(
          OrchestratorToolkit.tools.orchestrator_capabilities,
          {},
        );
        expect(localCapabilities.providers).toMatchObject([
          {
            models: expect.arrayContaining([{ id: "source-custom", label: "source custom" }]),
            canRunChildTask: true,
          },
        ]);
        const remoteCapabilities = yield* f
          .invoke(
            OrchestratorToolkit.tools.orchestrator_capabilities,
            { environmentId: destinationId },
            liveCaller,
          )
          .pipe(Effect.result);
        const capabilityRequests = (yield* Ref.get(f.http.requests)).filter(
          (request) =>
            request.method === "tools/call" &&
            (request.body as { params: { name: string } }).params.name ===
              "orchestrator_capabilities",
        );
        const listed = yield* f.call(OrchestratorToolkit.tools.t3_thread_list, {
          environmentId: destinationId,
          projectId: f.destination.project.id,
        });
        expect(JSON.stringify(listed)).toContain(target);
        const read = yield* f.call(
          OrchestratorToolkit.tools.t3_thread_read,
          { environmentId: destinationId, threadId: target },
          liveCaller,
        );
        expect(JSON.stringify(read)).toContain(target);
        const args = {
          environmentId: destinationId,
          threadId: target,
          message: "remote message",
          clientRequestId: "same",
        };
        const a = yield* f.call(OrchestratorToolkit.tools.t3_thread_send, args, liveCaller);
        expect(yield* f.call(OrchestratorToolkit.tools.t3_thread_send, args, liveCaller)).toEqual(
          a,
        );
        const b = yield* f.call(OrchestratorToolkit.tools.t3_thread_send, args, otherCaller);
        expect(b.messageId).not.toBe(a.messageId);
        const projection = yield* f.destination.threads.getThreadProjection(target);
        expect(projection.messages.map((m) => m.id)).toEqual([a.messageId, b.messageId]);
        expect(projection.runs).toHaveLength(2);
        yield* f.call(OrchestratorToolkit.tools.t3_thread_update, {
          environmentId: destinationId,
          threadId: target,
          action: "rename",
          title: "Renamed remotely",
        });
        const queued = yield* f.call(OrchestratorToolkit.tools.t3_thread_send, {
          ...args,
          clientRequestId: "queued",
          message: "before edit",
          mode: "queue",
        });
        yield* f.call(ThreadToolkit.tools.t3_queue_edit, {
          environmentId: destinationId,
          threadId: target,
          queuedRunId: queued.runId,
          text: "after edit",
        });
        expect(
          yield* f.call(ThreadToolkit.tools.t3_queue_list, {
            environmentId: destinationId,
            threadId: target,
          }),
        ).toMatchObject({
          items: expect.arrayContaining([
            { queuedRunId: queued.runId, text: "after edit", truncated: false },
          ]),
        });
        const edited = yield* f.destination.threads.getThreadProjection(target);
        expect(edited.thread.title).toBe("Renamed remotely");
        expect(edited.messages.find((m) => m.id === queued.messageId)?.text).toBe("after edit");

        const launchArgs = {
          environmentId: destinationId,
          projectId: f.destination.project.id,
          title: "remote launch",
          message: "only once",
          clientRequestId: "launch",
        };
        const launched = yield* f.call(
          ProjectToolkit.tools.t3_thread_launch,
          launchArgs,
          scope("A", "auto"),
        );
        const replay = yield* f.call(
          ProjectToolkit.tools.t3_thread_launch,
          launchArgs,
          scope("A", "auto"),
        );
        expect(replay.threadId).toBe(launched.threadId);
        expect(replay.runId).toBe(launched.runId);
        const otherLaunch = yield* f.call(
          ProjectToolkit.tools.t3_thread_launch,
          launchArgs,
          scope("B", "auto"),
        );
        expect(otherLaunch.threadId).not.toBe(launched.threadId);
        const signedSession = (yield* f.destination.auth.listSessions())[0]!;
        for (const launch of [launched, otherLaunch]) {
          const saved = yield* f.destination.threads.getThreadProjection(launch.threadId);
          expect(saved.messages).toHaveLength(1);
          expect(saved.runs).toHaveLength(1);
          expect(saved.thread).toMatchObject({
            projectId: f.destination.project.id,
            modelSelection,
            runtimeMode: "auto",
            interactionMode: "default",
          });
          expect.soft(saved.thread).toMatchObject({
            linkOrigin: {
              sessionId: signedSession.sessionId,
              label: "T3 Code · source",
              access: "full-access",
            },
          });
        }
        expect(yield* f.source.threads.getThreadProjection(local)).toEqual(before);

        yield* Ref.set(f.http.requests, []);
        yield* Ref.set(f.http.bearers, 0);
        yield* f.call(OrchestratorToolkit.tools.t3_thread_read, { threadId: local });
        yield* f.call(OrchestratorToolkit.tools.t3_thread_read, {
          environmentId: sourceId,
          threadId: local,
        });
        expect(yield* Ref.get(f.http.requests)).toEqual([]);
        expect(yield* Ref.get(f.http.bearers)).toBe(0);
        expect(
          yield* f.invoke(OrchestratorToolkit.tools.t3_thread_read, {
            environmentId: EnvironmentId.make("unknown"),
            threadId: target,
          }),
        ).toMatchObject({ isError: true, structuredContent: { code: "invalid_request" } });
        expect(
          yield* f.invoke(
            OrchestratorToolkit.tools.t3_thread_send,
            {
              environmentId: sourceId,
              threadId: local,
              message: "local escalation",
            },
            scope("auto", "auto"),
          ),
        ).toMatchObject({
          isError: true,
          structuredContent: { code: "runtime_mode_escalation_denied" },
        });
        const destinationIds = (yield* f.destination.threads.listProjectThreads({
          projectId: f.destination.project.id,
          includeSubagents: false,
        })).map((thread) => thread.id);
        const deniedSend = { ...args, clientRequestId: "denied" };
        for (const caller of [
          scope("reader", "read-only"),
          { ...scope(), capabilities: new Set<never>() },
          { ...scope(), environmentId: EnvironmentId.make("wrong-source") },
        ]) {
          expect(
            yield* f.invoke(OrchestratorToolkit.tools.t3_thread_send, deniedSend, caller),
          ).toMatchObject({ isError: true, structuredContent: { code: "capability_denied" } });
        }
        const planCaller = yield* f.source.caller("plan", "full-access", "plan");
        expect(
          yield* f.invoke(
            ProjectToolkit.tools.t3_thread_launch,
            { ...launchArgs, interactionMode: "default" },
            planCaller,
          ),
        ).toMatchObject({
          isError: true,
          structuredContent: { code: "interaction_mode_escalation_denied" },
        });
        const fs = yield* FileSystem.FileSystem;
        const config = Context.get(f.source.context, ServerConfig.ServerConfig);
        const attachment = {
          type: "image" as const,
          id: ChatAttachmentId.make(createPendingAttachmentId()),
          name: "local.png",
          mimeType: "image/png",
          sizeBytes: 4,
        };
        const pendingPath = resolveAttachmentPath({
          attachmentsDir: config.attachmentsDir,
          attachment,
        })!;
        yield* fs.makeDirectory(config.attachmentsDir, { recursive: true });
        yield* fs.writeFile(pendingPath, new Uint8Array([1, 2, 3, 4]));
        expect(
          yield* f.invoke(ProjectToolkit.tools.t3_thread_launch, {
            ...launchArgs,
            clientRequestId: "attachment",
            attachments: [attachment],
          }),
        ).toMatchObject({
          isError: true,
          structuredContent: {
            code: "invalid_request",
            message: expect.stringContaining("local attachment"),
          },
        });
        expect(Array.from(yield* fs.readFile(pendingPath))).toEqual([1, 2, 3, 4]);
        expect(
          (yield* f.destination.threads.listProjectThreads({
            projectId: f.destination.project.id,
            includeSubagents: false,
          })).map((thread) => thread.id),
        ).toEqual(destinationIds);
        expect(yield* Ref.get(f.http.requests)).toEqual([]);
        expect(yield* Ref.get(f.http.bearers)).toBe(0);

        // These refusals must reach the destination: the source cannot know its target modes.
        expect(
          yield* f.invoke(OrchestratorToolkit.tools.t3_thread_send, deniedSend, planCaller),
        ).toMatchObject({
          isError: true,
          structuredContent: { code: "interaction_mode_escalation_denied" },
        });
        expect(
          yield* f.invoke(
            OrchestratorToolkit.tools.t3_thread_send,
            deniedSend,
            scope("auto", "auto"),
          ),
        ).toMatchObject({
          isError: true,
          structuredContent: { code: "runtime_mode_escalation_denied" },
        });
        expect(
          (yield* Ref.get(f.http.requests))
            .filter((r) => r.method === "tools/call")
            .map((r) => r.limit),
        ).toEqual(["full-access/plan", "auto/default"]);
        yield* f.pair("auto");
        const narrowLaunch = yield* f.invoke(
          ProjectToolkit.tools.t3_thread_launch,
          { ...launchArgs, clientRequestId: "narrow-link" },
          scope(),
        );
        expect(yield* f.invoke(OrchestratorToolkit.tools.t3_thread_send, deniedSend)).toMatchObject(
          { isError: true, structuredContent: { code: "runtime_mode_escalation_denied" } },
        );
        expect(yield* f.destination.threads.getThreadProjection(target)).toEqual(edited);
        expect.soft(remoteCapabilities, JSON.stringify(remoteCapabilities)).toMatchObject({
          _tag: "Success",
          success: {
            structuredContent: {
              providers: [
                {
                  providerInstanceId: modelSelection.instanceId,
                  displayName: "destination Codex",
                  models: expect.arrayContaining([
                    { id: "destination-custom", label: "destination custom" },
                  ]),
                  canRunChildTask: true,
                },
              ],
            },
          },
        });

        expect.soft(capabilityRequests).toHaveLength(1);

        expect(narrowLaunch.isError, JSON.stringify(narrowLaunch)).not.toBe(true);
        // oxlint-disable-next-line t3code/no-inline-schema-compile -- Decode the real launch response before reading its persisted thread.
        const narrowLaunched = yield* Schema.decodeUnknownEffect(
          ProjectToolkit.tools.t3_thread_launch.successSchema,
        )(narrowLaunch.structuredContent);
        const narrowSaved = yield* f.destination.threads.getThreadProjection(
          narrowLaunched.threadId,
        );
        expect(narrowSaved.thread).toMatchObject({
          runtimeMode: "auto",
          interactionMode: "default",
        });
        expect.soft(narrowSaved.thread).toMatchObject({
          linkOrigin: { label: "T3 Code · source", access: "auto" },
        });

        const unicodeThreads: Array<ThreadId> = [];
        const threadsBeforeUnicode = yield* f.destination.threads.listProjectThreads({
          projectId: f.destination.project.id,
          includeSubagents: false,
        });
        // JSON strings preserve lone UTF-16 surrogates; replacing them with U+FFFD aliases keys.
        for (const [caller, clientRequestId] of [
          [liveCaller, "\ud800"],
          [otherCaller, "\ud800"],
          [liveCaller, "\udc00"],
          [liveCaller, "\ud801"],
          [liveCaller, "\ufffd"],
        ] as const) {
          const input = {
            environmentId: destinationId,
            projectId: f.destination.project.id,
            title: "Unicode retry key",
            clientRequestId,
          };
          const first = yield* f.call(ProjectToolkit.tools.t3_thread_launch, input, caller);
          const saved = yield* f.destination.threads.getThreadProjection(first.threadId);
          expect(yield* f.call(ProjectToolkit.tools.t3_thread_launch, input, caller)).toEqual(
            first,
          );
          expect(yield* f.destination.threads.getThreadProjection(first.threadId)).toEqual(saved);
          expect(saved.messages).toEqual([]);
          expect(saved.runs).toEqual([]);
          expect(unicodeThreads).not.toContain(first.threadId);
          unicodeThreads.push(first.threadId);
        }
        expect(
          yield* f.destination.threads.listProjectThreads({
            projectId: f.destination.project.id,
            includeSubagents: false,
          }),
        ).toHaveLength(threadsBeforeUnicode.length + unicodeThreads.length);
      }),
    ).pipe(Effect.provide(Layer.mergeAll(NodeHttpServer.layerTest, NodeServices.layer))),
);

it.effect(
  "bounds forwarded waits by elapsed time, pins the first run, and interrupts pending requests",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const requests = yield* Queue.unbounded<{
          args: Record<string, unknown>;
          reply: Deferred.Deferred<unknown>;
          finalized: Deferred.Deferred<void>;
        }>();
        const calls = yield* Ref.make(0);
        const peers = PeerMcpClient.PeerMcpClient.of({
          call: (input) =>
            Effect.gen(function* () {
              const reply = yield* Deferred.make<unknown>();
              const finalized = yield* Deferred.make<void>();
              yield* Ref.update(calls, (n) => n + 1);
              yield* Queue.offer(requests, { args: input.arguments, reply, finalized });
              return yield* Deferred.await(reply).pipe(
                Effect.flatMap(Schema.decodeUnknownEffect(input.success)),
                Effect.orDie,
                Effect.ensuring(Deferred.succeed(finalized, undefined)),
              );
            }),
        });
        const context = yield* PeerForwarding.layer.pipe(
          Layer.provide(
            Layer.mergeAll(
              Layer.succeed(PeerMcpClient.PeerMcpClient, peers),
              Layer.mock(PeerLinks.PeerLinks)({}),
              Layer.mock(ThreadManagement.ThreadManagementService)({}),
              environmentLayer(Effect.succeed(descriptor(sourceId))),
              NodeServices.layer,
            ),
          ),
          Layer.build,
        );
        const forwarding = Context.get(context, PeerForwarding.PeerForwarding);
        const threadId = ThreadId.make("waiting");
        const runId = RunId.make("first-run");
        const response: OrchestratorMcpThreadWaitResult = {
          threadId,
          runId,
          status: "running",
          timedOut: true,
        };
        const wait = (timeoutMs: number) =>
          forwarding.call(scope(), OrchestratorToolkit.tools.t3_thread_wait, destinationId, {
            threadId,
            timeoutMs,
          });
        const startedAt = yield* Clock.currentTimeMillis;
        const waiting = yield* wait(120_000).pipe(Effect.forkScoped);
        const first = yield* Queue.take(requests);
        expect(first.args.threadId).toBe(threadId);
        expect(Number(first.args.timeoutMs)).toBeGreaterThan(0);
        expect(Number(first.args.timeoutMs)).toBeLessThanOrEqual(50_000);
        yield* TestClock.adjust(60_000);
        yield* Deferred.succeed(first.reply, response);
        yield* TestClock.adjust(1);
        const second = yield* Queue.take(requests);
        expect(second.args.runId).toBe(runId);
        expect(Number(second.args.timeoutMs)).toBeGreaterThan(0);
        expect(Number(second.args.timeoutMs)).toBeLessThanOrEqual(50_000);
        yield* TestClock.adjust(55_000);
        yield* Deferred.succeed(second.reply, { ...response, runId: RunId.make("successor") });
        yield* TestClock.adjust(1);
        const third = yield* Queue.take(requests);
        expect(third.args.runId).toBe(runId);
        const finalRemaining = 120_000 - ((yield* Clock.currentTimeMillis) - startedAt);
        expect(finalRemaining).toBe(4_998);
        const finalTimeout = Number(third.args.timeoutMs);
        // The destination starts its timer after probe/init/transit, not at the local deadline.
        const overhead = 100;
        yield* TestClock.adjust(finalTimeout + overhead);
        yield* Deferred.succeed(third.reply, { ...response, status: "completed", timedOut: false });
        const finalAnswer = yield* Fiber.join(waiting);
        yield* Deferred.await(third.finalized);

        const quick = yield* wait(10).pipe(Effect.forkScoped);
        const quickFirst = yield* Queue.take(requests);
        yield* Deferred.succeed(quickFirst.reply, response);
        yield* Deferred.await(quickFirst.finalized);
        yield* TestClock.adjust(1);
        const quickSecond = yield* Queue.take(requests);
        expect(quickSecond.args.runId).toBe(runId);
        expect(Number(quickSecond.args.timeoutMs)).toBeGreaterThan(0);
        expect(Number(quickSecond.args.timeoutMs)).toBeLessThanOrEqual(9);
        yield* TestClock.adjust(9);
        expect(yield* Fiber.join(quick)).toEqual(response);
        yield* Deferred.await(quickSecond.finalized);

        const noResponse = yield* wait(20).pipe(Effect.result, Effect.forkScoped);
        const pending = yield* Queue.take(requests);
        yield* TestClock.adjust(20);
        expect(yield* Fiber.join(noResponse)).toMatchObject({
          _tag: "Failure",
          failure: { code: "orchestration_error" },
        });
        yield* Deferred.await(pending.finalized);

        const cancelled = yield* wait(120_000).pipe(Effect.forkScoped);
        const cancelling = yield* Queue.take(requests);
        const count = yield* Ref.get(calls);
        yield* Fiber.interrupt(cancelled);
        yield* Deferred.await(cancelling.finalized);
        yield* TestClock.adjust(120_000);
        expect(yield* Ref.get(calls)).toBe(count);
        expect(yield* Queue.size(requests)).toBe(0);

        const short = yield* wait(10_000).pipe(Effect.result, Effect.forkScoped);
        const shortRequest = yield* Queue.take(requests);
        const shortTimeout = Number(shortRequest.args.timeoutMs);
        yield* TestClock.adjust(shortTimeout + overhead);
        yield* Deferred.succeed(shortRequest.reply, response);
        yield* Deferred.await(shortRequest.finalized);
        yield* TestClock.adjust(Math.max(0, 10_000 - shortTimeout - overhead));
        const shortAnswer = yield* Fiber.join(short);
        // Continuing after a fresh timeout is allowed, but any last request must honor the deadline.
        for (const remaining of yield* Queue.clear(requests)) {
          yield* Deferred.await(remaining.finalized);
        }
        expect
          .soft(shortAnswer, JSON.stringify(shortAnswer))
          .toMatchObject({ _tag: "Success", success: response });
        expect
          .soft(finalAnswer, JSON.stringify(finalAnswer))
          .toMatchObject({ runId, status: "completed", timedOut: false });
        for (const [requested, remaining] of [
          [shortTimeout, 10_000],
          [finalTimeout, finalRemaining],
        ] as const) {
          expect.soft(requested).toBeLessThanOrEqual(50_000);
          expect.soft(requested).toBeLessThan(remaining);
          expect.soft(requested).toBeGreaterThan(0);
          expect.soft(remaining - requested).toBeLessThanOrEqual(5_000);
        }
      }),
    ),
);

it.effect(
  "renews MCP sessions once, revalidates repaired credentials, and persists safe best-effort outcomes",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const f = yield* paired;
        const target = yield* f.destination.seed("protocol");
        const outcome = () => f.source.sql<{
          last_reached_at: string | null;
          last_error: string | null;
        }>`
      SELECT last_reached_at, last_error FROM peer_environment_links WHERE environment_id = ${destinationId}`;
        const pairedAt = (yield* outcome())[0]!.last_reached_at;
        yield* TestClock.adjust(1);
        const read = { environmentId: destinationId, threadId: target };
        yield* Ref.set(f.http.requests, []);
        yield* Ref.set(f.http.bearers, 0);
        yield* f.call(OrchestratorToolkit.tools.t3_thread_read, read);
        yield* f.call(OrchestratorToolkit.tools.t3_thread_read, read, scope("narrow", "auto"));
        let requests = yield* Ref.get(f.http.requests);
        expect(requests.filter((r) => r.method === "initialize")).toHaveLength(1);
        const reused = requests.filter((r) => r.method === "tools/call");
        expect(reused.map((r) => r.limit)).toEqual(["full-access/default", "auto/default"]);
        expect(reused[1]?.session).toBe(reused[0]?.session);
        const reachedAt = (yield* outcome())[0]!.last_reached_at;
        expect(reachedAt).not.toBe(pairedAt);
        yield* TestClock.adjust(1);
        expect(
          yield* f.invoke(
            OrchestratorToolkit.tools.t3_thread_send,
            {
              ...read,
              message: "must refuse",
              clientRequestId: "narrow",
            },
            scope("narrow", "auto"),
          ),
        ).toMatchObject({
          isError: true,
          structuredContent: { code: "runtime_mode_escalation_denied" },
        });
        expect((yield* f.destination.threads.getThreadProjection(target)).messages).toEqual([]);
        const refused = (yield* outcome())[0]!;
        expect(refused.last_reached_at).toBe(reachedAt);
        expect(refused.last_error).toBe("Peer call failed (runtime_mode_escalation_denied).");

        const receiverContext = yield* Layer.build(Layer.fresh(NodeHttpServer.layerTest));
        const receiver = Context.get(receiverContext, HttpServer.HttpServer);
        const received = yield* Ref.make<
          ReadonlyArray<{ method: string; body: string; session: string | undefined }>
        >([]);
        yield* receiver.serve(
          Effect.gen(function* () {
            const request = yield* HttpServerRequest.HttpServerRequest;
            const body = yield* request.text;
            yield* Ref.update(received, (seen) => [
              ...seen,
              { method: request.method, body, session: request.headers["mcp-session-id"] },
            ]);
            return HttpServerResponse.empty({ status: 502 });
          }),
        );
        if (receiver.address._tag === "UnixPathAddress") return yield* Effect.die("TCP expected");
        const beforeRedirect = yield* f.destination.threads.getThreadProjection(target);
        expect(requests.map((request) => request.method)).toContain("notifications/initialized");
        yield* Ref.set(f.http.requests, []);
        yield* Ref.set(f.http.probes, 0);
        // This native listener is reachable, but its plain-HTTP origin is not an allowed peer.
        yield* Ref.set(f.http.faults, [
          { redirect: `http://0.0.0.0:${receiver.address.port}/untrusted` },
        ]);
        expect(
          yield* f.invoke(OrchestratorToolkit.tools.t3_thread_send, {
            ...read,
            message: "synthetic body must stay at the paired origin",
            clientRequestId: "redirected-write",
          }),
        ).toMatchObject({ isError: true, structuredContent: { code: "orchestration_error" } });
        expect(yield* Ref.get(f.http.probes)).toBe(1);
        expect((yield* Ref.get(f.http.requests)).map((request) => request.method)).toEqual([
          "tools/call",
        ]);
        expect(yield* f.destination.threads.getThreadProjection(target)).toEqual(beforeRedirect);
        expect
          .soft(yield* Ref.get(received), "a tool POST must not follow a peer redirect")
          .toEqual([]);
        yield* f.call(OrchestratorToolkit.tools.t3_thread_read, read);
        expect((yield* outcome())[0]?.last_error).toBeNull();

        yield* Ref.set(f.http.requests, []);
        yield* Ref.set(f.http.bearers, 0);
        yield* Ref.set(f.http.faults, ["404"]);
        const send = { ...read, message: "renew once", clientRequestId: "renew" };
        const accepted = yield* f.call(OrchestratorToolkit.tools.t3_thread_send, send);
        requests = yield* Ref.get(f.http.requests);
        expect(requests.filter((r) => r.method === "initialize")).toHaveLength(1);
        const retried = requests.filter((r) => r.method === "tools/call");
        expect(retried).toHaveLength(2);
        expect(retried[0]?.body).toEqual(retried[1]?.body);
        expect(retried[0]?.session).not.toBe(retried[1]?.session);
        expect(yield* f.call(OrchestratorToolkit.tools.t3_thread_send, send)).toEqual(accepted);
        expect((yield* f.destination.threads.getThreadProjection(target)).messages).toHaveLength(1);
        expect((yield* outcome())[0]?.last_error).toBeNull();

        yield* Ref.set(f.http.requests, []);
        yield* Ref.set(f.http.bearers, 0);
        yield* Ref.set(f.http.faults, ["404", "404"]);
        expect(
          yield* f.invoke(OrchestratorToolkit.tools.t3_thread_send, {
            ...send,
            clientRequestId: "two404",
          }),
        ).toMatchObject({ isError: true, structuredContent: { code: "orchestration_error" } });
        requests = yield* Ref.get(f.http.requests);
        expect(requests.filter((r) => r.method === "initialize")).toHaveLength(1);
        expect(requests.filter((r) => r.method === "tools/call")).toHaveLength(2);
        expect((yield* f.destination.threads.getThreadProjection(target)).messages).toHaveLength(1);
        for (const fault of ["malformed", "rpc-error"] as const) {
          yield* Ref.set(f.http.faults, [fault]);
          const failed = yield* f.invoke(OrchestratorToolkit.tools.t3_thread_read, read);
          expect(failed).toMatchObject({
            isError: true,
            structuredContent: { code: "orchestration_error" },
          });
          expect(JSON.stringify(failed)).not.toContain("private upstream secret");
          expect((yield* outcome())[0]?.last_error).toBe("Peer call failed (orchestration_error).");
        }
        yield* f.call(OrchestratorToolkit.tools.t3_thread_read, read);
        expect((yield* outcome())[0]?.last_error).toBeNull();

        // A real SQLite trigger fails only diagnostic updates, after destination acceptance.
        yield* f.source
          .sql`CREATE TRIGGER fail_peer_outcome BEFORE UPDATE OF last_reached_at, last_error
      ON peer_environment_links BEGIN SELECT RAISE(FAIL, 'private outcome storage failure'); END`;
        const durable = yield* f.call(OrchestratorToolkit.tools.t3_thread_send, {
          ...send,
          message: "accepted despite diagnostic failure",
          clientRequestId: "outcome-fails",
        });
        yield* f.source.sql`DROP TRIGGER fail_peer_outcome`;
        const persisted = yield* f.destination.threads.getThreadProjection(target);
        expect(persisted.messages.map((m) => m.id)).toContain(durable.messageId);
        expect(persisted.runs).toHaveLength(2);

        const sessions = yield* f.destination.auth.listSessions();
        expect(sessions).toHaveLength(1);
        yield* f.destination.auth.revokeSession(AuthSessionId.make(sessions[0]!.sessionId));
        expect(
          yield* f.invoke(OrchestratorToolkit.tools.t3_thread_send, {
            ...send,
            clientRequestId: "revoked",
          }),
        ).toMatchObject({
          isError: true,
          structuredContent: {
            code: "capability_denied",
            message: expect.stringMatching(/link.*again/i),
          },
        });
        yield* f.pair("read-only");
        yield* Ref.set(f.http.requests, []);
        yield* Ref.set(f.http.bearers, 0);
        yield* f.call(OrchestratorToolkit.tools.t3_thread_read, read);
        expect(
          (yield* Ref.get(f.http.requests)).filter((r) => r.method === "initialize"),
        ).toHaveLength(1);
        expect(
          yield* f.invoke(OrchestratorToolkit.tools.t3_thread_send, {
            ...send,
            clientRequestId: "repaired-narrow",
          }),
        ).toMatchObject({ isError: true, structuredContent: { code: "capability_denied" } });
        expect(yield* f.destination.threads.getThreadProjection(target)).toEqual(persisted);
        for (const changed of [
          {
            ...descriptor(destinationId),
            capabilities: { repositoryIdentity: true, linkFence: true },
          },
          {
            ...descriptor(destinationId),
            capabilities: { repositoryIdentity: true, mcpModeLimitHeader: true },
          },
          descriptor(EnvironmentId.make("different-peer")),
        ]) {
          yield* Ref.set(f.destination.described, changed);
          yield* Ref.set(f.http.probes, 0);
          yield* Ref.set(f.http.requests, []);
          yield* Ref.set(f.http.bearers, 0);
          expect((yield* f.invoke(OrchestratorToolkit.tools.t3_thread_read, read)).isError).toBe(
            true,
          );
          expect(yield* Ref.get(f.http.probes)).toBe(1);
          expect(yield* Ref.get(f.http.requests)).toEqual([]);
          expect(yield* Ref.get(f.http.bearers)).toBe(0);
        }
        yield* Ref.set(f.destination.described, descriptor(destinationId));
        yield* f.call(OrchestratorToolkit.tools.t3_thread_read, read);
        expect((yield* outcome())[0]?.last_error).toBeNull();
        const reusedIds = reused.map((request) => (request.body as { id: string | number }).id);
        expect(reusedIds).not.toContain(0);
        expect(
          new Set(reusedIds).size,
          "sequential calls in one MCP session need distinct IDs",
        ).toBe(2);
      }),
    ).pipe(Effect.provide(Layer.mergeAll(NodeHttpServer.layerTest, NodeServices.layer))),
);

it.effect(
  "clones and registers projects only with full-access default authority at both ends",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const f = yield* paired;
        const fs = yield* FileSystem.FileSystem;
        const sourceBefore = yield* f.source.projects.snapshot;
        const remoteUrl = "https://example.com/team/repository.git";
        const clone = yield* f.call(ProjectToolkit.tools.t3_project_clone, {
          environmentId: destinationId,
          destinationPath: `${f.destination.root}/clone`,
          remoteUrl,
        });
        expect(yield* fs.readFileString(`${clone.cwd}/cloned.txt`)).toBe("controlled git clone");
        const project = yield* f.call(ProjectToolkit.tools.t3_project_create, {
          environmentId: destinationId,
          workspaceRoot: clone.cwd,
          title: "Cloned project",
        });
        yield* f.call(ProjectToolkit.tools.t3_project_update, {
          environmentId: destinationId,
          projectId: project.id,
          defaultModelSelection: modelSelection,
        });
        expect(
          yield* f.call(ProjectToolkit.tools.t3_project_read, {
            environmentId: destinationId,
            projectId: project.id,
          }),
        ).toMatchObject({
          id: project.id,
          workspaceRoot: clone.cwd,
          defaultModelSelection: modelSelection,
        });
        expect(
          Option.getOrThrow(yield* f.destination.projects.getById(project.id)).workspaceRoot,
        ).toBe(clone.cwd);
        expect(yield* f.source.projects.snapshot).toEqual(sourceBefore);
        const projectCount = (yield* f.destination.projects.snapshot).projects.length;
        const forbidden = `${f.destination.root}/forbidden`;
        const createArgs = {
          environmentId: destinationId,
          workspaceRoot: forbidden,
          title: "Forbidden",
          createWorkspaceRootIfMissing: true,
        };
        const cloneArgs = { environmentId: destinationId, destinationPath: forbidden, remoteUrl };
        yield* Ref.set(f.http.requests, []);
        yield* Ref.set(f.http.bearers, 0);
        const plan = {
          ...scope(),
          client: {
            ...scope().client!,
            narrowedTo: { runtimeMode: "full-access" as const, interactionMode: "plan" as const },
          },
        };
        for (const caller of [scope("auto", "auto"), plan]) {
          expect(
            yield* f.invoke(ProjectToolkit.tools.t3_project_create, createArgs, caller),
          ).toMatchObject({ isError: true, structuredContent: { code: "capability_denied" } });
          expect(
            yield* f.invoke(ProjectToolkit.tools.t3_project_clone, cloneArgs, caller),
          ).toMatchObject({ isError: true, structuredContent: { code: "capability_denied" } });
        }
        expect(yield* Ref.get(f.http.requests)).toEqual([]);
        expect(yield* Ref.get(f.http.bearers)).toBe(0);
        expect(yield* fs.exists(forbidden)).toBe(false);
        yield* f.pair("auto");
        yield* Ref.set(f.http.requests, []);
        yield* Ref.set(f.http.bearers, 0);
        expect(yield* f.invoke(ProjectToolkit.tools.t3_project_create, createArgs)).toMatchObject({
          isError: true,
          structuredContent: { code: "capability_denied" },
        });
        expect(yield* f.invoke(ProjectToolkit.tools.t3_project_clone, cloneArgs)).toMatchObject({
          isError: true,
          structuredContent: { code: "capability_denied" },
        });
        expect(
          (yield* Ref.get(f.http.requests)).filter((r) => r.method === "tools/call"),
        ).toHaveLength(2);
        expect(yield* fs.exists(forbidden)).toBe(false);
        expect((yield* f.destination.projects.snapshot).projects).toHaveLength(projectCount);
      }),
    ).pipe(Effect.provide(Layer.mergeAll(NodeHttpServer.layerTest, NodeServices.layer))),
);
