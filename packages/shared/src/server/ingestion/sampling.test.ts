import { describe, expect, it } from "vitest";

import {
  computeTraceSamplingDecision,
  getTraceSamplingDecisionKey,
  resolveTraceSamplingDecision,
  TRACE_SAMPLING_DECISION_TTL_SECONDS,
} from "./sampling";

class FakeRedis {
  readonly values = new Map<string, string>();
  readonly setCalls: Array<{
    key: string;
    value: string;
    args: (string | number)[];
  }> = [];
  readonly getCalls: string[] = [];

  setError: Error | null = null;
  getError: Error | null = null;

  constructor(initial: Record<string, "in" | "out"> = {}) {
    Object.entries(initial).forEach(([key, value]) => {
      this.values.set(key, value);
    });
  }

  async set(
    key: string,
    value: string,
    ...args: (string | number)[]
  ): Promise<"OK" | null> {
    this.setCalls.push({ key, value, args });
    if (this.setError) throw this.setError;

    // Minimal NX semantics.
    if (args.includes("NX") && this.values.has(key)) return null;
    this.values.set(key, value);
    return "OK";
  }

  async get(key: string): Promise<string | null> {
    this.getCalls.push(key);
    if (this.getError) throw this.getError;
    return this.values.get(key) ?? null;
  }
}

function createDeterministicRandom(seed: number) {
  let state = Math.abs(Math.trunc(seed)) % 4294967296;
  return () => {
    state = (state * 1664525 + 1013904223) % 4294967296;
    return state / 4294967296;
  };
}

describe("computeTraceSamplingDecision", () => {
  it("uses the fixed project and trace bucket", () => {
    const decision = computeTraceSamplingDecision({
      projectId: "p1",
      traceId: "trace-1",
      sampleRate: 0.2,
    });

    expect(decision.bucket).toBe(487692088);
    expect(decision.keep).toBe(true);
    expect(decision.reason).toBe("hash_in");

    const outside = computeTraceSamplingDecision({
      projectId: "p1",
      traceId: "trace-1",
      sampleRate: 0.1,
    });
    expect(outside.bucket).toBe(487692088);
    expect(outside.keep).toBe(false);
    expect(outside.reason).toBe("hash_out");
  });

  it("handles explicit and invalid rates", () => {
    expect(
      computeTraceSamplingDecision({
        projectId: "p1",
        traceId: "trace-1",
        sampleRate: 0,
      }),
    ).toMatchObject({ keep: false, reason: "rate_zero" });

    expect(
      computeTraceSamplingDecision({
        projectId: "p1",
        traceId: "trace-1",
        sampleRate: 1,
      }),
    ).toMatchObject({ keep: true, reason: "rate_one" });

    for (const sampleRate of [-0.1, 1.5, Number.NaN]) {
      expect(
        computeTraceSamplingDecision({
          projectId: "p1",
          traceId: "trace-1",
          sampleRate,
        }),
      ).toMatchObject({ keep: true, reason: "invalid_rate" });
    }
  });

  it("handles unconfigured projects and missing trace ids", () => {
    expect(
      computeTraceSamplingDecision({
        projectId: "p1",
        traceId: "trace-1",
      }),
    ).toMatchObject({ keep: true, reason: "not_configured" });

    expect(
      computeTraceSamplingDecision({
        projectId: "",
        traceId: "trace-1",
        sampleRate: 0.2,
      }),
    ).toMatchObject({ keep: true, reason: "not_configured" });

    expect(
      computeTraceSamplingDecision({
        projectId: "p1",
        traceId: null,
        sampleRate: 0.2,
      }),
    ).toEqual({
      traceId: null,
      keep: true,
      reason: "no_trace_id",
      bucket: null,
    });
  });

  it("changes buckets by project and is deterministic", () => {
    const p1 = computeTraceSamplingDecision({
      projectId: "p1",
      traceId: "trace-1",
      sampleRate: 0.25,
    }).bucket;
    const p2 = computeTraceSamplingDecision({
      projectId: "p2",
      traceId: "trace-1",
      sampleRate: 0.25,
    }).bucket;

    expect(p1).toBe(487692088);
    expect(p2).not.toBe(p1);

    const first = computeTraceSamplingDecision({
      projectId: "p1",
      traceId: "trace-1",
      sampleRate: 0.25,
    });
    for (let i = 0; i < 100; i += 1) {
      expect(
        computeTraceSamplingDecision({
          projectId: "p1",
          traceId: "trace-1",
          sampleRate: 0.25,
        }),
      ).toEqual(first);
    }
  });

  it("approximates the configured rate over deterministic random traces", () => {
    const random = createDeterministicRandom(20261126);
    let kept = 0;
    const total = 10000;

    for (let i = 0; i < total; i += 1) {
      const traceId = `trace-${Math.floor(random() * 0xffffffff).toString(16)}-${i}`;
      const decision = computeTraceSamplingDecision({
        projectId: "distribution-project",
        traceId,
        sampleRate: 0.25,
      });
      if (decision.keep) kept += 1;
    }

    const ratio = kept / total;
    expect(ratio).toBeGreaterThanOrEqual(0.22);
    expect(ratio).toBeLessThanOrEqual(0.28);
  });
});

describe("resolveTraceSamplingDecision", () => {
  it("stores the local decision with NX and a second TTL", async () => {
    const redis = new FakeRedis();

    const decision = await resolveTraceSamplingDecision({
      projectId: "p1",
      traceId: "trace-1",
      sampleRate: 0.2,
      redisClient: redis,
    });

    expect(decision.reason).toBe("hash_in");
    expect(redis.setCalls).toHaveLength(1);
    expect(redis.setCalls[0]).toMatchObject({
      key: getTraceSamplingDecisionKey("p1", "trace-1"),
      value: "in",
      args: ["NX", "EX", TRACE_SAMPLING_DECISION_TTL_SECONDS],
    });
    expect(redis.setCalls[0].args).toContain(86400);
    expect(redis.setCalls[0].args).not.toContain(86400_000);
    expect(TRACE_SAMPLING_DECISION_TTL_SECONDS).toBe(86400);
  });

  it("uses the winners cached out decision", async () => {
    const key = getTraceSamplingDecisionKey("p1", "trace-1");
    const redis = new FakeRedis({ [key]: "out" });

    const decision = await resolveTraceSamplingDecision({
      projectId: "p1",
      traceId: "trace-1",
      sampleRate: 0.2,
      redisClient: redis,
    });

    expect(redis.setCalls[0]?.args).toEqual(["NX", "EX", 86400]);
    expect(redis.getCalls).toEqual([key]);
    expect(decision).toMatchObject({
      keep: false,
      reason: "cached_out",
      bucket: 487692088,
    });
  });

  it("falls back to the local hash when Redis fails", async () => {
    const redis = new FakeRedis();
    redis.setError = new Error("set failed");
    redis.getError = new Error("get failed");

    const decision = await resolveTraceSamplingDecision({
      projectId: "p1",
      traceId: "trace-1",
      sampleRate: 0.1,
      redisClient: redis,
    });

    expect(decision).toMatchObject({
      keep: false,
      reason: "hash_out",
      bucket: 487692088,
    });
  });

  it("does not access Redis for invalid rates or unconfigured projects", async () => {
    const redis = new FakeRedis();

    await resolveTraceSamplingDecision({
      projectId: "p1",
      traceId: "trace-1",
      sampleRate: Number.NaN,
      redisClient: redis,
    });
    await resolveTraceSamplingDecision({
      projectId: "p1",
      traceId: "trace-1",
      redisClient: redis,
    });

    expect(redis.setCalls).toHaveLength(0);
    expect(redis.getCalls).toHaveLength(0);
  });

  it("does not reject when redis is missing get or set", async () => {
    await expect(
      resolveTraceSamplingDecision({
        projectId: "p1",
        traceId: "trace-1",
        sampleRate: 0.2,
        redisClient: {},
      }),
    ).resolves.toMatchObject({ reason: "hash_in", keep: true });
  });
});
