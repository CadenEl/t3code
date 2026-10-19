import * as NodeUtil from "node:util";
import { NodeHttpServer } from "@effect/platform-node";
import * as NodeServices from "@effect/platform-node/NodeServices";
import {
  AuthSessionId,
  EnvironmentHttpApi,
  EnvironmentId,
  type ExecutionEnvironmentDescriptor,
} from "@t3tools/contracts";
import { expect, it } from "@effect/vitest";
import * as Context from "effect/Context";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Logger from "effect/Logger";
import * as Option from "effect/Option";
import * as Ref from "effect/Ref";
import * as References from "effect/References";
import * as SqlClient from "effect/sql/SqlClient";
import * as TestClock from "effect/testing/TestClock";
import * as FetchHttpClient from "effect/http/FetchHttpClient";
import * as HttpRouter from "effect/http/HttpRouter";
import * as HttpServer from "effect/http/HttpServer";
import * as HttpServerRequest from "effect/http/HttpServerRequest";
import * as HttpServerResponse from "effect/http/HttpServerResponse";
import * as HttpApi from "effect/http-api/HttpApi";
import * as HttpApiBuilder from "effect/http-api/HttpApiBuilder";
import * as HttpApiClient from "effect/http-api/HttpApiClient";

import * as EnvironmentAuth from "../auth/EnvironmentAuth.ts";
import * as AuthHttp from "../auth/http.ts";
import * as McpOAuth from "../auth/McpOAuth.ts";
import * as McpOAuthHttp from "../auth/mcpOAuthHttp.ts";
import * as ServerSecretStore from "../auth/ServerSecretStore.ts";
import * as ServerConfig from "../config.ts";
import * as ServerEnvironment from "../environment/ServerEnvironment.ts";
import * as ServerHttp from "../http.ts";
import * as McpHttpServer from "../mcp/McpHttpServer.ts";
import * as Sqlite from "../persistence/Sqlite.ts";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import { runMigrations } from "../persistence/Migrations.ts";
import * as PeerLinks from "./PeerLinks.ts";

class PeerTestApi extends HttpApi.make("environment")
  .add(EnvironmentHttpApi.groups.metadata)
  .add(EnvironmentHttpApi.groups.mcpOAuth) {}

// The remote fixture represents a compatible peer; this layer must not advertise linkFence itself.
const box: ExecutionEnvironmentDescriptor = {
  environmentId: EnvironmentId.make("box"),
  label: "Box",
  platform: { os: "linux", arch: "x64" },
  serverVersion: "0.0.0-test",
  capabilities: { repositoryIdentity: true, mcpModeLimitHeader: true, linkFence: true },
};
const laptop = {
  ...box,
  environmentId: EnvironmentId.make("laptop"),
  label: "Laptop",
  capabilities: { ...box.capabilities, linkFence: false },
};
const environmentLayer = (descriptor: Effect.Effect<ExecutionEnvironmentDescriptor>) =>
  Layer.succeed(ServerEnvironment.ServerEnvironment, {
    getEnvironmentId: descriptor.pipe(Effect.map((value) => value.environmentId)),
    getDescriptor: descriptor,
  });

const servePeer = Effect.gen(function* () {
  const descriptor = yield* Ref.make(box);
  const beforeDescriptor = yield* Ref.make(Effect.void);
  const redirect = yield* Ref.make<
    { path: string; location: string; status: 307 | 308 } | undefined
  >(undefined);
  const describe = Effect.gen(function* () {
    const gate = yield* Ref.getAndSet(beforeDescriptor, Effect.void);
    yield* gate;
    return yield* Ref.get(descriptor);
  });
  const requests = yield* Ref.make<
    ReadonlyArray<{ url: string; authorization: string | undefined }>
  >([]);
  const context = yield* McpOAuth.layerMcpClientAuthenticator.pipe(
    Layer.provideMerge(EnvironmentAuth.layer.pipe(Layer.provide(Sqlite.layerMemory))),
    Layer.provideMerge(ServerSecretStore.layer),
    Layer.provideMerge(ServerEnvironment.layerIdentity),
    Layer.provide(environmentLayer(describe)),
    Layer.provideMerge(ServerConfig.layerTest(process.cwd(), { prefix: "t3-peer-link-peer-" })),
    Layer.provideMerge(NodeServices.layer),
    Layer.fresh,
    Layer.build,
  );
  const recordRequests = HttpRouter.middleware(
    (effect) =>
      Effect.gen(function* () {
        const request = yield* HttpServerRequest.HttpServerRequest;
        yield* Ref.update(requests, (seen) => [
          ...seen,
          { url: request.url, authorization: request.headers.authorization },
        ]);
        const destination = yield* Ref.get(redirect);
        if (destination?.path === request.url) {
          yield* request.text;
          return HttpServerResponse.redirect(destination.location, { status: destination.status });
        }
        return yield* effect;
      }),
    { global: true },
  );
  yield* HttpRouter.serve(
    HttpApiBuilder.layer(PeerTestApi).pipe(
      Layer.provide(McpOAuthHttp.layer.pipe(Layer.provide(McpOAuth.layer))),
      Layer.provide(ServerHttp.layerServerEnvironmentHttpApi),
      Layer.provide(AuthHttp.layerAuthenticatedAuth),
      Layer.provide(recordRequests),
      Layer.provide(environmentLayer(describe)),
      Layer.provide(Layer.succeedContext(context)),
    ),
    { disableListenLog: true, disableLogger: true },
  ).pipe(Layer.build);
  const address = (yield* HttpServer.HttpServer).address;
  if (address._tag === "UnixPathAddress") return yield* Effect.die("expected TCP address");
  const url = `http://127.0.0.1:${address.port}`;
  const authenticator = Context.get(context, McpHttpServer.McpClientAuthenticator);
  return {
    url,
    descriptor,
    beforeDescriptor,
    redirect,
    requests,
    auth: Context.get(context, EnvironmentAuth.EnvironmentAuth),
    authenticate: (token: string) =>
      authenticator.authenticate(
        HttpServerRequest.fromWeb(
          new Request(`${url}/mcp`, { headers: { authorization: `Bearer ${token}` } }),
        ),
      ),
  };
});

const makeLaptop = (throughMigration?: number) =>
  Effect.gen(function* () {
    const database =
      throughMigration === undefined
        ? Sqlite.layerMemory
        : Layer.effectDiscard(runMigrations({ toMigrationInclusive: throughMigration })).pipe(
            Layer.provideMerge(NodeSqliteClient.layer({ filename: ":memory:" })),
          );
    const context = yield* Layer.mergeAll(
      database,
      ServerSecretStore.layer,
      FetchHttpClient.layer,
      environmentLayer(Effect.succeed(laptop)),
    ).pipe(
      Layer.provide(ServerConfig.layerTest(process.cwd(), { prefix: "t3-peer-link-local-" })),
      Layer.provideMerge(NodeServices.layer),
      Layer.fresh,
      Layer.build,
    );
    const secrets = Context.get(context, ServerSecretStore.ServerSecretStore);
    const reopen = (store = secrets) =>
      PeerLinks.layer.pipe(
        Layer.provide(
          Layer.succeedContext(Context.add(context, ServerSecretStore.ServerSecretStore, store)),
        ),
        Layer.fresh,
        Layer.build,
        Effect.map((services) => Context.get(services, PeerLinks.PeerLinks)),
      );
    return {
      links: yield* reopen(),
      reopen,
      sql: Context.get(context, SqlClient.SqlClient),
      secrets,
    };
  });

it.effect(
  "pairs through OAuth, persists a redacted link, and forgets its credential on unlink",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const peer = yield* servePeer;
        const alternateServer = yield* Layer.build(Layer.fresh(NodeHttpServer.layerTest));
        const alternate = yield* servePeer.pipe(Effect.provide(alternateServer));
        const local = yield* makeLaptop();
        expect(
          yield* local.links
            .link({ url: peer.url, pairingCode: "wrong", access: "auto" })
            .pipe(Effect.flip),
        ).toMatchObject({ reason: "pairing_rejected" });
        expect(yield* peer.auth.listSessions()).toEqual([]);
        const linked = yield* local.links.link({
          url: peer.url,
          alternateUrls: [alternate.url],
          pairingCode: (yield* peer.auth.issuePairingCredential()).credential,
          access: "auto",
        });
        const resolved = yield* local.links.resolve(box.environmentId);
        const invocation = yield* peer.authenticate(resolved.token);
        expect(invocation?.client).toMatchObject({
          label: "T3 Code · Laptop",
          access: "auto",
          linked: true,
        });
        expect((yield* peer.auth.listSessions()).map((session) => session.sessionId)).toEqual([
          invocation!.client!.sessionId,
        ]);
        const rows = yield* local.sql<{
          secret_name: string;
        }>`SELECT * FROM peer_environment_links`;
        expect(rows).toHaveLength(1);
        const secretName = rows[0]!.secret_name;
        const storedToken = yield* local.secrets.get(secretName);
        expect(Option.map(storedToken, (bytes) => new TextDecoder().decode(bytes))).toEqual(
          Option.some(resolved.token),
        );
        const reopened = yield* local.reopen();
        expect((yield* reopened.resolve(box.environmentId)).token).toBe(resolved.token);
        const listed = yield* reopened.list;
        expect(listed).toMatchObject([
          { environmentId: box.environmentId, access: "auto", status: "reachable" },
        ]);
        const stored = yield* reopened.get(box.environmentId);
        expect(stored).toEqual(Option.some(linked));
        const publicState = JSON.stringify({ linked, listed, stored, rows });
        expect(publicState).not.toContain(resolved.token);
        expect(JSON.stringify({ linked, listed, stored })).not.toContain(secretName);

        // An ordinary OAuth registration must not acquire the peer marker.
        const client = yield* HttpApiClient.make(EnvironmentHttpApi, { baseUrl: peer.url }).pipe(
          Effect.provide(FetchHttpClient.layer),
        );
        const redirect = "http://localhost/callback";
        const registered = yield* client.mcpOAuth.register({
          payload: { client_name: "Claude Code", redirect_uris: [redirect] },
        });
        const decided = yield* client.mcpOAuth.decision({
          payload: {
            authorization: {
              response_type: "code",
              client_id: registered.client_id,
              redirect_uri: redirect,
              code_challenge: "DeB41nTVkPwpbbYecrnqtVq7VXLezustdHAK4SWt13c",
              code_challenge_method: "S256",
              resource: `${peer.url}/mcp`,
            },
            decision: {
              _tag: "pairing-code",
              access: "read-only",
              code: (yield* peer.auth.issuePairingCredential()).credential,
            },
          },
        });
        const ordinary = yield* client.mcpOAuth.token({
          payload: {
            grant_type: "authorization_code",
            client_id: registered.client_id,
            redirect_uri: redirect,
            code: new URL(decided.redirectTo).searchParams.get("code")!,
            code_verifier: "a".repeat(43) + "-verifier-for-tests",
            resource: `${peer.url}/mcp`,
          },
        });
        const ordinaryInvocation = yield* peer.authenticate(ordinary.access_token);
        expect(ordinaryInvocation?.client).toMatchObject({
          label: "Claude Code",
          access: "read-only",
        });
        expect(ordinaryInvocation?.client).not.toHaveProperty("linked", true);

        yield* Ref.set(peer.requests, []);
        const fallbackResults = yield* Effect.forEach(
          [
            { repositoryIdentity: true, linkFence: true },
            { repositoryIdentity: true, mcpModeLimitHeader: true },
          ],
          (capabilities) =>
            Effect.gen(function* () {
              yield* Ref.set(peer.descriptor, { ...box, capabilities });
              return yield* reopened.resolve(box.environmentId).pipe(Effect.result);
            }),
        );
        yield* Ref.set(alternate.descriptor, {
          ...box,
          capabilities: { repositoryIdentity: true, linkFence: true },
        });
        expect(yield* reopened.resolve(box.environmentId).pipe(Effect.flip)).toMatchObject({
          reason: "incompatible",
        });
        for (const requests of [peer.requests, alternate.requests]) {
          expect(
            (yield* Ref.get(requests)).every(
              (request) =>
                request.url.startsWith("/.well-known/t3/environment") &&
                request.authorization === undefined,
            ),
          ).toBe(true);
        }

        expect(yield* reopened.unlink(box.environmentId)).toBe(true);
        expect(yield* reopened.list).toEqual([]);
        expect(yield* reopened.get(box.environmentId)).toEqual(Option.none());
        expect(yield* local.secrets.get(secretName)).toEqual(Option.none());
        expect(yield* reopened.resolve(box.environmentId).pipe(Effect.flip)).toMatchObject({
          reason: "unknown_link",
        });
        expect(yield* reopened.unlink(box.environmentId)).toBe(false);
        expect(fallbackResults).toMatchObject([
          { _tag: "Success", success: { url: alternate.url, token: resolved.token } },
          { _tag: "Success", success: { url: alternate.url, token: resolved.token } },
        ]);
        expect(yield* Ref.get(alternate.requests)).not.toHaveLength(0);
      }),
    ).pipe(Effect.provide(NodeHttpServer.layerTest)),
);

it.effect("refuses unsafe peers and repairs a revoked link", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const peer = yield* servePeer;
      const local = yield* makeLaptop();
      const receiverContext = yield* Layer.build(Layer.fresh(NodeHttpServer.layerTest));
      const receiver = Context.get(receiverContext, HttpServer.HttpServer);
      const received = yield* Ref.make<
        ReadonlyArray<{ method: string; url: string; body: string }>
      >([]);
      yield* receiver.serve(
        Effect.gen(function* () {
          const request = yield* HttpServerRequest.HttpServerRequest;
          const body = yield* request.text;
          yield* Ref.update(received, (requests) => [
            ...requests,
            { method: request.method, url: request.url, body },
          ]);
          return HttpServerResponse.empty({ status: 502 });
        }),
      );
      if (receiver.address._tag === "UnixPathAddress") {
        return yield* Effect.die("expected TCP address");
      }
      // Reaches this native listener without DNS, but is not an allowed plain-HTTP peer origin.
      const receiverUrl = `http://0.0.0.0:${receiver.address.port}`;
      expect(
        yield* local.links
          .link({ url: receiverUrl, pairingCode: "synthetic-pairing-code", access: "auto" })
          .pipe(Effect.flip),
      ).toMatchObject({ _tag: "PeerLinkError", reason: "unreachable" });
      expect(yield* Ref.get(received)).toEqual([]);
      for (const [phase, status] of [
        ["decision", 307],
        ["token", 308],
      ] as const) {
        yield* Ref.set(peer.requests, []);
        yield* Ref.set(received, []);
        yield* Ref.set(peer.redirect, {
          path: `/oauth/mcp/${phase}`,
          location: `${receiverUrl}/${phase}`,
          status,
        });
        const result = yield* local.links
          .link({
            url: peer.url,
            pairingCode: (yield* peer.auth.issuePairingCredential()).credential,
            access: "auto",
          })
          .pipe(Effect.result);
        expect(result).toMatchObject({
          _tag: "Failure",
          failure: { _tag: "PeerLinkError", reason: "unreachable" },
        });
        expect((yield* Ref.get(peer.requests)).map((request) => request.url)).toEqual([
          "/.well-known/t3/environment",
          "/oauth/mcp/register",
          "/oauth/mcp/decision",
          ...(phase === "token" ? ["/oauth/mcp/token"] : []),
        ]);
        expect(yield* local.links.list).toEqual([]);
        expect(yield* local.sql`SELECT * FROM peer_environment_links`).toEqual([]);
        expect
          .soft(yield* Ref.get(received), `${status} ${phase} must not forward its body`)
          .toEqual([]);
      }
      yield* Ref.set(peer.redirect, undefined);
      yield* Ref.set(peer.requests, []);
      const pairing = yield* peer.auth.issuePairingCredential();
      const incompatible = [
        { ...box, capabilities: { repositoryIdentity: true, linkFence: true } },
        { ...box, capabilities: { repositoryIdentity: true, mcpModeLimitHeader: true } },
      ];
      for (const descriptor of incompatible) {
        yield* Ref.set(peer.descriptor, descriptor);
        expect(
          yield* local.links
            .link({ url: peer.url, pairingCode: pairing.credential, access: "auto" })
            .pipe(Effect.flip),
        ).toMatchObject({ reason: "incompatible" });
      }
      expect(yield* peer.auth.listSessions()).toEqual([]);
      expect(yield* local.links.list).toEqual([]);
      expect(
        (yield* Ref.get(peer.requests)).every((request) =>
          request.url.startsWith("/.well-known/t3/environment"),
        ),
      ).toBe(true);
      yield* Ref.set(peer.descriptor, box);
      yield* local.links.link({ url: peer.url, pairingCode: pairing.credential, access: "auto" });
      const original = yield* local.links.resolve(box.environmentId);
      const [row] = yield* local.sql<{
        secret_name: string;
      }>`SELECT secret_name FROM peer_environment_links`;
      yield* Ref.set(peer.requests, []);
      for (const descriptor of incompatible) {
        yield* Ref.set(peer.descriptor, descriptor);
        expect(yield* local.links.resolve(box.environmentId).pipe(Effect.flip)).toMatchObject({
          reason: "incompatible",
        });
      }
      yield* Ref.set(peer.descriptor, {
        ...box,
        environmentId: EnvironmentId.make("someone-else"),
      });
      expect(yield* local.links.resolve(box.environmentId).pipe(Effect.flip)).toMatchObject({
        reason: "unreachable",
      });
      expect((yield* local.links.list).map((link) => link.status)).toEqual(["unreachable"]);
      expect(
        (yield* Ref.get(peer.requests)).every((request) => request.authorization === undefined),
      ).toBe(true);
      yield* Ref.set(peer.descriptor, box);
      const oldInvocation = yield* peer.authenticate(original.token);
      yield* peer.auth.revokeSession(AuthSessionId.make(oldInvocation!.client!.sessionId));
      expect(yield* peer.authenticate(original.token)).toBeUndefined();
      const probeStarted = yield* Deferred.make<void>();
      const resumeProbe = yield* Deferred.make<void>();
      yield* Ref.set(
        peer.beforeDescriptor,
        Deferred.succeed(probeStarted, undefined).pipe(Effect.andThen(Deferred.await(resumeProbe))),
      );
      const resolving = yield* local.links
        .resolve(box.environmentId)
        .pipe(Effect.result, Effect.forkScoped({ startImmediately: true }));
      yield* Deferred.await(probeStarted);
      yield* local.links.link({
        url: peer.url,
        pairingCode: (yield* peer.auth.issuePairingCredential()).credential,
        access: "full-access",
      });
      yield* Deferred.succeed(resumeProbe, undefined);
      const interruptedResolution = yield* Fiber.join(resolving);
      const renewed = yield* local.links.resolve(box.environmentId);
      expect(renewed.token).not.toBe(original.token);
      expect(renewed.link.access).toBe("full-access");
      expect((yield* peer.authenticate(renewed.token))?.client).toMatchObject({
        access: "full-access",
        linked: true,
      });
      expect((yield* local.links.list).map((link) => link.access)).toEqual(["full-access"]);
      expect(yield* local.secrets.get(row!.secret_name)).toEqual(Option.none());
      yield* TestClock.adjust("31 days");
      expect((yield* local.links.list).map((link) => link.status)).toEqual(["expired"]);
      expect(yield* local.links.resolve(box.environmentId).pipe(Effect.flip)).toMatchObject({
        reason: "expired",
      });
      expect(interruptedResolution).toMatchObject({
        _tag: "Failure",
        failure: {
          reason: "unreachable",
          message: expect.stringMatching(
            /changed.*(?:retry|try again)|(?:retry|try again).*changed/i,
          ),
        },
      });

      const legacy = yield* makeLaptop(65);
      const old = yield* peer.auth.issueMcpClientSession({
        label: "Legacy laptop",
        access: "auto",
        client: { deviceType: "bot" },
      });
      yield* legacy.secrets.set("legacy-peer", new TextEncoder().encode(old.token));
      yield* legacy.sql`INSERT INTO peer_environment_links
        (environment_id, secret_name, label, urls_json, access, linked_at, expires_at)
        VALUES (${box.environmentId}, 'legacy-peer', 'Box', ${JSON.stringify([peer.url])},
          'auto', '2026-01-01T00:00:00.000Z', '2099-01-01T00:00:00.000Z')`;
      const migrated = yield* runMigrations().pipe(
        Effect.provideService(SqlClient.SqlClient, legacy.sql),
      );
      expect.soft(migrated).toEqual([[66, "PeerLinkFence"]]);
      expect
        .soft(
          yield* legacy.sql`SELECT migration_id, name FROM effect_sql_migrations
        WHERE migration_id >= 64 ORDER BY migration_id`,
        )
        .toEqual([
          { migration_id: 64, name: "CanonicalizeThreadPullRequestLinks" },
          { migration_id: 65, name: "PeerLinks" },
          { migration_id: 66, name: "PeerLinkFence" },
        ]);
      expect
        .soft((yield* legacy.sql`SELECT * FROM peer_environment_links`)[0])
        .toMatchObject({ fenced: 0 });
      let secretReads = 0;
      const guarded = yield* legacy.reopen({
        ...legacy.secrets,
        get: (name) =>
          Effect.sync(() => {
            secretReads += 1;
          }).pipe(Effect.andThen(legacy.secrets.get(name))),
      });
      yield* Ref.set(peer.requests, []);
      const refused = yield* guarded.resolve(box.environmentId).pipe(Effect.result);
      expect.soft(refused).toMatchObject({
        _tag: "Failure",
        failure: { reason: "incompatible", message: expect.stringMatching(/link.*again/i) },
      });
      expect.soft(secretReads).toBe(0);
      expect
        .soft(
          (yield* Ref.get(peer.requests)).every((request) => request.authorization === undefined),
        )
        .toBe(true);
      yield* guarded.link({
        url: peer.url,
        access: "auto",
        pairingCode: (yield* peer.auth.issuePairingCredential()).credential,
      });
      const repaired = yield* guarded.resolve(box.environmentId);
      expect((yield* peer.authenticate(repaired.token))?.client).toMatchObject({
        linked: true,
        access: "auto",
      });
      expect
        .soft((yield* legacy.sql`SELECT * FROM peer_environment_links`)[0])
        .toMatchObject({ fenced: 1 });
      expect(yield* legacy.secrets.get("legacy-peer")).toEqual(Option.none());
    }),
  ).pipe(Effect.provide(NodeHttpServer.layerTest)),
);

it.effect("keeps a renewed link usable when removing its old credential fails", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const peer = yield* servePeer;
      const local = yield* makeLaptop();
      yield* local.links.link({
        url: peer.url,
        pairingCode: (yield* peer.auth.issuePairingCredential()).credential,
        access: "auto",
      });
      const original = yield* local.links.resolve(box.environmentId);
      const [row] = yield* local.sql<{
        secret_name: string;
      }>`SELECT secret_name FROM peer_environment_links`;
      const links = yield* local.reopen({
        ...local.secrets,
        remove: (name) =>
          name === row!.secret_name
            ? Effect.fail(
                new ServerSecretStore.SecretStoreRemoveError({
                  resource: `secret ${name}`,
                  cause: new Error(`private cleanup failure: ${original.token}`),
                }),
              )
            : local.secrets.remove(name),
      });
      const warnings: string[] = [];
      const logger = Logger.make(({ logLevel, message, cause, fiber }) => {
        if (logLevel === "Warn") {
          warnings.push(
            NodeUtil.inspect(
              { message, cause, annotations: fiber.getRef(References.CurrentLogAnnotations) },
              { depth: null },
            ),
          );
        }
      });
      const pairing = yield* peer.auth.issuePairingCredential();
      const result = yield* links
        .link({
          url: peer.url,
          pairingCode: pairing.credential,
          access: "full-access",
        })
        .pipe(Effect.result, Effect.provide(Logger.layer([logger], { mergeWithExisting: false })));
      const renewed = yield* local.links.resolve(box.environmentId);
      expect(renewed.token).not.toBe(original.token);
      expect(renewed.link.access).toBe("full-access");
      expect((yield* peer.authenticate(renewed.token))?.client).toMatchObject({
        access: "full-access",
        linked: true,
      });
      expect(yield* local.secrets.get(row!.secret_name)).toEqual(
        Option.some(new TextEncoder().encode(original.token)),
      );
      expect.soft(result).toMatchObject({ _tag: "Success", success: renewed.link });
      expect(warnings).toHaveLength(1);
      expect(warnings.join("\n")).not.toContain(original.token);
      expect(warnings.join("\n")).not.toContain(renewed.token);
      expect(warnings.join("\n")).not.toContain(pairing.credential);
      expect(warnings.join("\n")).not.toContain(row!.secret_name);
      expect(warnings.join("\n")).not.toContain("private cleanup failure");
    }),
  ).pipe(Effect.provide(NodeHttpServer.layerTest)),
);
