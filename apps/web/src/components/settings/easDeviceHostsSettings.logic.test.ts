import { describe, expect, it } from "vite-plus/test";
import * as Option from "effect/Option";

import {
  easSessionLimits,
  newEasDeviceHost,
  parseEasDeviceHostDraft,
  updateEasDeviceHosts,
} from "./easDeviceHostsSettings.logic";

const ssh = { kind: "ssh" as const, id: "mac", label: "Mac mini", target: "julius@mac" };

describe("EAS device host settings", () => {
  it("adds, edits, and removes an EAS host without touching SSH hosts", () => {
    const eas = newEasDeviceHost("eas-1");
    const added = updateEasDeviceHosts([ssh], eas, false);
    expect(added).toEqual([ssh, eas]);
    const edited = { ...eas, label: "Cloud iPhone", maxConcurrentSessions: 1 };
    expect(updateEasDeviceHosts(added, edited, false)).toEqual([ssh, edited]);
    expect(updateEasDeviceHosts(added, eas, true)).toEqual([ssh]);
  });

  it("rejects limits EAS does not allow", () => {
    expect(Option.isSome(parseEasDeviceHostDraft(newEasDeviceHost("eas")))).toBe(true);
    expect(
      Option.isNone(
        parseEasDeviceHostDraft({ ...newEasDeviceHost("eas"), maxSessionMinutes: 600 }),
      ),
    ).toBe(true);
    expect(Option.isNone(parseEasDeviceHostDraft({ ...newEasDeviceHost("eas"), label: " " }))).toBe(
      true,
    );
  });

  it("describes a host's bounds in one line", () => {
    expect(easSessionLimits({ ...newEasDeviceHost("eas"), maxConcurrentSessions: 1 })).toBe(
      "Up to 1 session at once, each stopped after 30 min or 10 idle min",
    );
  });
});
