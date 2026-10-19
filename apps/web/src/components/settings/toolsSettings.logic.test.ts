import { describe, expect, it } from "vite-plus/test";
import {
  mcpServerDraftFrom,
  mcpServerFromDraft,
  mcpServerForTarget,
  mcpServerTransportForTarget,
  parseMcpServerJson,
} from "./toolsSettings.logic";

describe("MCP server editor", () => {
  it("saves a pasted command with quoted arguments and secret variables", () => {
    const args = ["--dir", "/work/My Projects", 'say "hi"', ""];
    const draft = parseMcpServerJson(
      JSON.stringify({
        mcpServers: {
          Docs: { command: "node", args, env: { API_TOKEN: "new-token" } },
        },
      }),
    );
    expect(draft).not.toBeNull();
    if (draft === null) throw new Error("Expected a server draft");
    expect(mcpServerFromDraft(draft, new Set())).toEqual({
      ok: true,
      name: "docs",
      transport: {
        type: "stdio",
        command: "node",
        args,
        env: [{ name: "API_TOKEN", value: "new-token", sensitive: true }],
      },
    });
    expect(mcpServerFromDraft(draft, new Set(["docs"]))).toMatchObject({
      ok: false,
      field: "name",
    });
    expect(
      mcpServerFromDraft(
        {
          ...draft,
          env: [...draft.env, { ...draft.env[0]!, id: "duplicate", name: " API_TOKEN " }],
        },
        new Set(),
      ),
    ).toMatchObject({
      ok: false,
      field: "variables",
      message: "Duplicate environment variable name: API_TOKEN.",
    });
  });

  it("preserves an untouched secret and its original identity through an editor rename", () => {
    const server = {
      enabled: true,
      transport: {
        type: "http" as const,
        url: "https://example.com/mcp",
        headers: [{ name: "Authorization", value: "", sensitive: true, valueRedacted: true }],
      },
    };
    const draft = mcpServerDraftFrom("docs", server);
    expect(mcpServerFromDraft(draft, new Set(), "docs")).toEqual({
      ok: true,
      name: "docs",
      transport: server.transport,
    });
    const renamed = {
      ...draft,
      name: "docs-work",
      headers: draft.headers.map((header) => ({ ...header, name: "X-Api-Key" })),
    };
    expect(mcpServerFromDraft(renamed, new Set(), "docs")).toEqual({
      ok: true,
      name: "docs-work",
      transport: {
        ...server.transport,
        headers: [
          {
            name: "X-Api-Key",
            value: "",
            sensitive: true,
            valueRedacted: true,
            storedAs: { server: "docs", variable: "Authorization" },
          },
        ],
      },
    });
    expect(
      mcpServerFromDraft(
        {
          ...draft,
          headers: [
            ...draft.headers,
            { ...draft.headers[0]!, id: "duplicate", name: " authorization " },
          ],
        },
        new Set(),
        "docs",
      ),
    ).toMatchObject({
      ok: false,
      field: "variables",
      message: "Duplicate header name: authorization.",
    });
    const replaced = {
      ...renamed,
      headers: renamed.headers.map((header) => ({
        ...header,
        value: "replacement-token",
        stored: false,
      })),
    };
    const result = mcpServerFromDraft(replaced, new Set(), "docs");
    expect(result.ok && result.transport.type === "http" && result.transport.headers).toEqual([
      { name: "X-Api-Key", value: "replacement-token", sensitive: true },
    ]);
  });
});

it("preserves different destination headers in a URL-only bulk edit", () => {
  const initial = {
    type: "http" as const,
    url: "https://example.com/mcp",
    headers: [{ name: "Authorization", value: "", sensitive: true, valueRedacted: true }],
  };
  const local = {
    ...initial,
    headers: [{ name: "X-Api-Key", value: "", sensitive: true, valueRedacted: true }],
  };
  const edited = { ...initial, url: "https://changed.example/mcp" };
  expect(mcpServerTransportForTarget(edited, initial, initial)).toEqual(edited);
  expect(mcpServerTransportForTarget(edited, initial, local)).toEqual({
    ...local,
    url: edited.url,
  });
  expect(mcpServerTransportForTarget(edited, initial, local, "docs")).toEqual({
    ...local,
    url: edited.url,
    headers: [{ ...local.headers[0], storedAs: { server: "docs", variable: "X-Api-Key" } }],
  });

  for (const enabled of [true, false]) {
    const destination = { enabled, transport: local };
    for (const renamedFrom of [undefined, "docs"]) {
      expect(
        mcpServerForTarget(
          { enabled: !enabled, transport: edited },
          initial,
          destination,
          renamedFrom,
        ).enabled,
      ).toBe(enabled);
    }
  }
  expect(
    mcpServerForTarget({ enabled: false, transport: edited }, undefined, undefined).enabled,
  ).toBe(false);
  expect(
    mcpServerForTarget({ enabled: true, transport: edited }, undefined, undefined).enabled,
  ).toBe(true);

  const sameHeader = {
    ...initial,
    headers: [{ name: "Authorization", value: "local-token", sensitive: false }],
  };
  expect(mcpServerTransportForTarget(edited, initial, sameHeader)).toEqual({
    ...sameHeader,
    url: edited.url,
  });

  expect(() =>
    mcpServerTransportForTarget(edited, initial, {
      type: "stdio",
      command: "native-tool",
      args: [],
      env: [{ name: "API_KEY", value: "", sensitive: true, valueRedacted: true }],
    }),
  ).toThrow("each environment");

  const replaced = {
    ...edited,
    headers: [{ name: "Authorization", value: "replacement", sensitive: true }],
  };
  expect(() => mcpServerTransportForTarget(replaced, initial, local)).toThrow("each environment");
  expect(mcpServerTransportForTarget(replaced, initial, initial)).toEqual(replaced);
  expect(mcpServerTransportForTarget({ ...edited, headers: [] }, initial, initial)).toEqual({
    ...edited,
    headers: [],
  });
  const renamed = {
    ...edited,
    headers: [
      {
        ...initial.headers[0]!,
        name: "New-Authorization",
        storedAs: { server: "docs", variable: "Authorization" },
      },
    ],
  };
  expect(mcpServerTransportForTarget(renamed, initial, initial)).toEqual(renamed);
  const mixedInitial = {
    ...initial,
    headers: [
      { name: "Authorization", value: "public-value", sensitive: false },
      { name: "X-Region", value: "east", sensitive: false },
    ],
  };
  const mixedTarget = {
    ...initial,
    headers: [
      { name: "Authorization", value: "", sensitive: true, valueRedacted: true },
      { name: "X-Region", value: "west", sensitive: false },
    ],
  };
  const mixedEdit = {
    ...mixedInitial,
    headers: [mixedInitial.headers[0]!, { ...mixedInitial.headers[1]!, value: "north" }],
  };
  expect(mcpServerTransportForTarget(mixedEdit, mixedInitial, mixedTarget, "docs")).toEqual({
    ...mixedEdit,
    headers: [
      {
        name: "Authorization",
        value: "",
        sensitive: true,
        valueRedacted: true,
        storedAs: { server: "docs", variable: "Authorization" },
      },
      { name: "X-Region", value: "north", sensitive: false },
    ],
  });
  const plainDraft = mcpServerDraftFrom("docs", { enabled: true, transport: mixedInitial });
  const plainRename = mcpServerFromDraft(
    {
      ...plainDraft,
      headers: plainDraft.headers.map((variable, index) =>
        index === 0 ? { ...variable, name: "X-Account" } : variable,
      ),
    },
    new Set(),
    "docs",
  );
  if (!plainRename.ok) throw new Error("Expected a valid plain variable rename");
  expect(
    mcpServerForTarget({ enabled: true, transport: plainRename.transport }, mixedInitial, {
      enabled: false,
      transport: mixedInitial,
    }).transport,
  ).toEqual(plainRename.transport);
  const differentPlainTarget = {
    ...mixedInitial,
    headers: mixedInitial.headers.map((variable, index) =>
      index === 0 ? { ...variable, value: "destination-value" } : variable,
    ),
  };
  expect(() =>
    mcpServerForTarget({ enabled: true, transport: plainRename.transport }, mixedInitial, {
      enabled: false,
      transport: differentPlainTarget,
    }),
  ).toThrow("each environment");
  expect(() =>
    mcpServerForTarget({ enabled: true, transport: plainRename.transport }, mixedInitial, {
      enabled: false,
      transport: mixedTarget,
    }),
  ).toThrow("each environment");
});
