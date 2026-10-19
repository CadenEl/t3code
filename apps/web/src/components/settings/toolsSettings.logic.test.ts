import { describe, expect, it } from "vite-plus/test";
import {
  mcpServerDraftFrom,
  mcpServerFromDraft,
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

it("preserves each target's credential when a shared server edit leaves it untouched", () => {
  const initial = {
    type: "http" as const,
    url: "https://example.com/mcp",
    headers: [{ name: "Authorization", value: "", sensitive: true, valueRedacted: true }],
  };
  const local = {
    ...initial,
    headers: [{ name: "Authorization", value: "local-token", sensitive: false }],
  };
  expect(mcpServerTransportForTarget(initial, initial, local)).toEqual(local);
  const replaced = {
    ...initial,
    headers: [{ name: "Authorization", value: "replacement", sensitive: true }],
  };
  expect(mcpServerTransportForTarget(replaced, initial, local)).toEqual(replaced);
});
