import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";

import { ForwardCompatibleArray, TrimmedNonEmptyString } from "./baseSchemas.ts";

/**
 * Lowercase letters, digits, `-` and `_`, starting with a letter: the subset
 * every provider accepts as a server key and tool prefix (Claude names tools
 * `mcp__<server>__<tool>`, OpenCode `<server>_<tool>`).
 */
const MCP_SERVER_NAME_PATTERN = /^[a-z][a-z0-9_-]*$/;
const MCP_SERVER_NAME_MAX_CHARS = 48;
const RESERVED_MCP_SERVER_NAMES: ReadonlySet<string> = new Set(["t3-code"]);

export const McpServerName = TrimmedNonEmptyString.check(
  Schema.isMaxLength(MCP_SERVER_NAME_MAX_CHARS),
  Schema.isPattern(MCP_SERVER_NAME_PATTERN),
  Schema.makeFilter((name: string) =>
    RESERVED_MCP_SERVER_NAMES.has(name) ? `"${name}" is reserved for T3 Code.` : true,
  ),
);
export type McpServerName = typeof McpServerName.Type;

/**
 * An environment variable or HTTP header. A `sensitive` value is moved into
 * the environment's secret store on save; settings and clients then see an
 * empty value with `valueRedacted` set, and sending that back keeps it.
 * A client that renamed the server or the variable sends `storedAs`, the
 * names the secret was saved under, so the save moves it instead of losing it.
 */
export const McpServerVariable = Schema.Struct({
  name: TrimmedNonEmptyString,
  value: Schema.String.pipe(Schema.withDecodingDefault(Effect.succeed(""))),
  sensitive: Schema.Boolean.pipe(Schema.withDecodingDefault(Effect.succeed(false))),
  valueRedacted: Schema.optionalKey(Schema.Boolean),
  storedAs: Schema.optionalKey(
    Schema.Struct({ server: TrimmedNonEmptyString, variable: TrimmedNonEmptyString }),
  ),
});
export type McpServerVariable = typeof McpServerVariable.Type;

export const McpStdioTransport = Schema.Struct({
  type: Schema.Literal("stdio"),
  command: TrimmedNonEmptyString,
  args: Schema.Array(Schema.String).pipe(Schema.withDecodingDefault(Effect.succeed([]))),
  env: Schema.Array(McpServerVariable).pipe(Schema.withDecodingDefault(Effect.succeed([]))),
});
export type McpStdioTransport = typeof McpStdioTransport.Type;

export const McpHttpTransport = Schema.Struct({
  type: Schema.Literal("http"),
  url: TrimmedNonEmptyString,
  headers: Schema.Array(McpServerVariable).pipe(Schema.withDecodingDefault(Effect.succeed([]))),
});
export type McpHttpTransport = typeof McpHttpTransport.Type;

export const McpServerTransport = Schema.Union([McpStdioTransport, McpHttpTransport]);
export type McpServerTransport = typeof McpServerTransport.Type;

export const McpServerConfig = Schema.Struct({
  enabled: Schema.Boolean.pipe(Schema.withDecodingDefault(Effect.succeed(true))),
  transport: McpServerTransport,
});
export type McpServerConfig = typeof McpServerConfig.Type;

export const McpServerProjectOverride = Schema.Struct({
  enabled: Schema.Boolean.pipe(Schema.withDecodingDefault(Effect.succeed(true))),
  transport: Schema.optionalKey(McpServerTransport),
});
export type McpServerProjectOverride = typeof McpServerProjectOverride.Type;

export const McpServers = Schema.Record(McpServerName, McpServerConfig);
export type McpServers = typeof McpServers.Type;

export const McpServerProjectOverrides = Schema.Record(McpServerName, McpServerProjectOverride);
export type McpServerProjectOverrides = typeof McpServerProjectOverrides.Type;

export const DisabledSkills = ForwardCompatibleArray(TrimmedNonEmptyString);
export type DisabledSkills = typeof DisabledSkills.Type;

export const DisabledSkillsProjectOverride = Schema.Record(TrimmedNonEmptyString, Schema.Boolean);
export type DisabledSkillsProjectOverride = typeof DisabledSkillsProjectOverride.Type;

export interface ResolvedMcpServer {
  readonly name: McpServerName;
  readonly transport: McpServerTransport;
}

export function mcpServerVariableRecord(
  variables: ReadonlyArray<McpServerVariable>,
): Record<string, string> {
  const record: Record<string, string> = {};
  for (const variable of variables) {
    if (variable.valueRedacted === true && variable.value.length === 0) continue;
    record[variable.name] = variable.value;
  }
  return record;
}

export const isValidMcpServerName: (name: unknown) => name is McpServerName =
  Schema.is(McpServerName);

export function mcpServerTransportSummary(transport: McpServerTransport): string {
  return transport.type === "stdio"
    ? [transport.command, ...transport.args].join(" ")
    : transport.url;
}
