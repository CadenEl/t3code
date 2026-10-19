import {
  type DeviceHostConfig,
  EasDeviceHostConfig,
  type EasDeviceHostConfig as EasDeviceHostConfigType,
} from "@t3tools/contracts";
import * as Schema from "effect/Schema";

export const easDeviceHosts = (hosts: ReadonlyArray<DeviceHostConfig>) =>
  hosts.filter((host): host is EasDeviceHostConfigType => host.kind === "eas");

export const parseEasDeviceHostDraft = Schema.decodeUnknownOption(EasDeviceHostConfig);

export const newEasDeviceHost = (id: string): EasDeviceHostConfigType => ({
  kind: "eas",
  id,
  label: "EAS Simulator",
  accessToken: "",
  maxSessionMinutes: 30,
  idleMinutes: 10,
  maxConcurrentSessions: 2,
});

/** Adds, replaces, or removes one EAS host; hosts of other kinds keep their place. */
export function updateEasDeviceHosts(
  hosts: ReadonlyArray<DeviceHostConfig>,
  host: EasDeviceHostConfigType,
  remove: boolean,
): ReadonlyArray<DeviceHostConfig> {
  if (remove) return hosts.filter((candidate) => candidate.id !== host.id);
  return hosts.some((candidate) => candidate.id === host.id)
    ? hosts.map((candidate) => (candidate.id === host.id ? host : candidate))
    : [...hosts, host];
}

export function easSessionLimits(host: EasDeviceHostConfigType) {
  const sessions =
    host.maxConcurrentSessions === 1 ? "1 session" : `${host.maxConcurrentSessions} sessions`;
  return `Up to ${sessions} at once, each stopped after ${host.maxSessionMinutes} min or ${host.idleMinutes} idle min`;
}
