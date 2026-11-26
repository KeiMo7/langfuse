import crypto from "node:crypto";
import { env } from "../../env";
import { logger } from "../logger";
import { recordIncrement } from "../instrumentation";
import type { IngestionEventType } from "./types";

export const TRACE_SAMPLING_DECISION_TTL_SECONDS = 86400;

export type TraceSamplingReason =
  | "not_configured"
  | "no_trace_id"
  | "invalid_rate"
  | "rate_zero"
  | "rate_one"
  | "hash_in"
  | "hash_out"
  | "cached_in"
  | "cached_out";

export interface TraceSamplingDecision {
  traceId: string | null;
  keep: boolean;
  reason: TraceSamplingReason;
  bucket: number | null;
}

type SamplingRedisClient = {
  get: (key: string) => Promise<string | null>;
  set: (
    key: string,
    value: string,
    ...args: (string | number)[]
  ) => Promise<unknown>;
};

function isSamplingRedisClient(value: unknown): value is SamplingRedisClient {
  return (
    !!value &&
    typeof value === "object" &&
    "get" in value &&
    "set" in value &&
    typeof value.get === "function" &&
    typeof value.set === "function"
  );
}

export function isTraceIdInSample(params: {
  projectId: string | null;
  event: IngestionEventType;
}): { isSampled: boolean; isSamplingConfigured: boolean } {
  const { projectId, event } = params;

  const sampledProjects = env.LANGFUSE_INGESTION_PROCESSING_SAMPLED_PROJECTS;

  if (!projectId || !sampledProjects.has(projectId))
    return { isSampled: true, isSamplingConfigured: false };

  const sampleRate = sampledProjects.get(projectId);
  if (sampleRate === undefined)
    return { isSampled: true, isSamplingConfigured: true };

  const traceId = parseTraceId(event) ?? null;
  const decision = computeTraceSamplingDecision({
    projectId,
    traceId,
    sampleRate,
  });

  return {
    isSampled: decision.keep,
    isSamplingConfigured: decision.reason !== "not_configured",
  };
}

export function parseTraceId(
  event: IngestionEventType,
): string | null | undefined {
  if (event.type === "trace-create") return event.body.id;

  return "traceId" in event.body ? event.body.traceId : null;
}

export function computeTraceSamplingDecision({
  projectId,
  traceId,
  sampleRate,
}: {
  projectId: string | null | undefined;
  traceId: string | null | undefined;
  sampleRate?: number | null;
}): TraceSamplingDecision {
  if (!projectId) {
    return {
      traceId: traceId ?? null,
      keep: true,
      reason: "not_configured",
      bucket: null,
    };
  }

  if (!traceId) {
    return {
      traceId: null,
      keep: true,
      reason: "no_trace_id",
      bucket: null,
    };
  }

  if (sampleRate === undefined || sampleRate === null) {
    return {
      traceId,
      keep: true,
      reason: "not_configured",
      bucket: null,
    };
  }

  const bucket = computeTraceSamplingBucket(projectId, traceId);

  if (
    typeof sampleRate !== "number" ||
    Number.isNaN(sampleRate) ||
    sampleRate < 0 ||
    sampleRate > 1
  ) {
    logger.error(`Invalid sample rate ${String(sampleRate)}`);

    // Be conservative and keep the trace ID in sample for invalid configs.
    return { traceId, keep: true, reason: "invalid_rate", bucket };
  }

  if (sampleRate === 0) {
    return { traceId, keep: false, reason: "rate_zero", bucket };
  }

  if (sampleRate === 1) {
    return { traceId, keep: true, reason: "rate_one", bucket };
  }

  return bucket / 0xffffffff < sampleRate
    ? { traceId, keep: true, reason: "hash_in", bucket }
    : { traceId, keep: false, reason: "hash_out", bucket };
}

export async function resolveTraceSamplingDecision({
  projectId,
  traceId,
  sampleRate,
  redisClient,
}: {
  projectId: string | null | undefined;
  traceId: string | null | undefined;
  sampleRate?: number | null;
  redisClient?: unknown;
}): Promise<TraceSamplingDecision> {
  const localDecision = computeTraceSamplingDecision({
    projectId,
    traceId,
    sampleRate,
  });
  const hasValidRate = isValidSampleRate(sampleRate);

  if (!projectId || !traceId || !hasValidRate) {
    return localDecision;
  }

  if (!isSamplingRedisClient(redisClient)) {
    recordCacheError(projectId);
    return localDecision;
  }
  const cacheClient = redisClient;

  const key = getTraceSamplingDecisionKey(projectId, traceId);
  const value = localDecision.keep ? "in" : "out";

  let setResult: unknown;
  try {
    setResult = await cacheClient.set(
      key,
      value,
      "NX",
      "EX",
      TRACE_SAMPLING_DECISION_TTL_SECONDS,
    );
  } catch {
    recordCacheError(projectId);
    return localDecision;
  }

  if (setResult === "OK") {
    return localDecision;
  }

  try {
    return await readCachedDecision(cacheClient, key, localDecision, projectId);
  } catch {
    recordCacheError(projectId);
    return localDecision;
  }
}

async function readCachedDecision(
  redisClient: NonNullable<SamplingRedisClient>,
  key: string,
  localDecision: TraceSamplingDecision,
  projectId: string,
): Promise<TraceSamplingDecision> {
  if (!redisClient.get) return localDecision;

  const cachedValue = await redisClient.get(key);

  if (cachedValue === "in") {
    return { ...localDecision, keep: true, reason: "cached_in" };
  }

  if (cachedValue === "out") {
    return { ...localDecision, keep: false, reason: "cached_out" };
  }

  if (cachedValue !== null) {
    logger.warn("Ignoring invalid trace sampling decision cache value", {
      projectId,
      value: cachedValue,
    });
  }

  return localDecision;
}

export function getTraceSamplingDecisionKey(
  projectId: string,
  traceId: string,
): string {
  return `langfuse:ingestion:trace-sampling:${projectId}:${traceId}`;
}

function isValidSampleRate(
  sampleRate: number | null | undefined,
): sampleRate is number {
  return (
    typeof sampleRate === "number" &&
    !Number.isNaN(sampleRate) &&
    sampleRate >= 0 &&
    sampleRate <= 1
  );
}

function computeTraceSamplingBucket(
  projectId: string,
  traceId: string,
): number {
  const hash = crypto
    .createHash("sha256")
    .update(`${projectId}:${traceId}`, "utf8")
    .digest("hex");

  return parseInt(hash.substring(0, 8), 16);
}

function recordCacheError(projectId: string): void {
  try {
    recordIncrement("langfuse.ingestion.sampling_decision_cache_error", 1, {
      projectId,
    });
  } catch {
    // Metrics must never change ingestion behavior.
  }
}
