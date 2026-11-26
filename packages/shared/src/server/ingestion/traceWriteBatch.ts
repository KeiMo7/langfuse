import { randomUUID } from "crypto";

import { logger } from "../logger";
import {
  getClickhouseEntityType,
  type IngestionEntityTypes,
} from "../clickhouse/schemaUtils";
import type { IngestionEventType } from "./types";
import type { TraceSamplingDecision } from "./sampling";

export const TRACE_WRITE_BATCH_MIN_ITEMS = 2;
export const TRACE_WRITE_BATCH_MAX_ITEMS = 256;

export interface IngestionBatchItem {
  type: IngestionEventType["type"];
  eventBodyId: string;
  fileKey: string;
  bucketPrefix?: string;
  skipS3List?: boolean;
}

export interface TraceWriteItem extends IngestionBatchItem {
  events: IngestionEventType[];
}

export interface TraceWriteBatch {
  batchId: string;
  projectId: string;
  traceId: string | null;
  items: TraceWriteItem[];
  eventCount: number;
  oversized?: boolean;
}

export interface DroppedTraceWriteBatch {
  traceId: string;
  eventCount: number;
  reason: TraceSamplingDecision["reason"] | string;
}

export interface PackTraceWriteBatchesResult {
  batches: TraceWriteBatch[];
  dropped: DroppedTraceWriteBatch[];
  oversized: TraceWriteBatch[];
}

export interface PackTraceWriteGroup extends Partial<TraceWriteItem> {
  type: TraceWriteItem["type"];
  eventBodyId: string;
  entityType?: IngestionEntityTypes;
  // Compatibility with processEventBatch's internal reducer shape.
  data?: IngestionEventType[];
  key?: string;
}

export interface PackTraceWriteBatchesOptions {
  projectId: string;
  groups?: PackTraceWriteGroup[] | Record<string, PackTraceWriteGroup>;
  items?: PackTraceWriteGroup[] | Record<string, PackTraceWriteGroup>;
  sortedBatchByEventBodyId?: Record<string, PackTraceWriteGroup>;
  decisions?:
    | Map<string, TraceSamplingDecision>
    | Record<string, TraceSamplingDecision>;
  samplingDecisions?:
    | Map<string, TraceSamplingDecision>
    | Record<string, TraceSamplingDecision>;
  decisionByTraceId?:
    | Map<string, TraceSamplingDecision>
    | Record<string, TraceSamplingDecision>;
  skipS3ListFor?: (group: PackTraceWriteGroup) => boolean | undefined;
  singletonEntityTypes?: readonly IngestionEntityTypes[] | readonly string[];
  createBatchId?: () => string;
}

type MutableTraceWriteBatch = TraceWriteBatch & {
  singleton?: boolean;
};

export function packTraceWriteBatches(
  options: PackTraceWriteBatchesOptions,
): PackTraceWriteBatchesResult {
  const {
    projectId,
    decisions,
    samplingDecisions,
    decisionByTraceId,
    skipS3ListFor,
    singletonEntityTypes = ["score"],
    createBatchId = randomUUID,
  } = options;

  const source =
    options.groups ?? options.items ?? options.sortedBatchByEventBodyId ?? [];
  const groups = Array.isArray(source) ? source : Object.values(source);
  const decisionTable = decisions ?? samplingDecisions ?? decisionByTraceId;
  const singletonTypes = new Set<string>(singletonEntityTypes);

  const batchOrder: MutableTraceWriteBatch[] = [];
  const batchByPackKey = new Map<string, MutableTraceWriteBatch>();
  const droppedByTraceId = new Map<string, DroppedTraceWriteBatch>();

  groups.forEach((sourceGroup, index) => {
    const events = sourceGroup.events ?? sourceGroup.data ?? [];
    const fileKey = sourceGroup.fileKey ?? sourceGroup.key ?? "";
    const traceId = getTraceIdFromEvents(events);
    const decision = resolveDecision(decisionTable, traceId, projectId);

    if (!decision.keep && traceId) {
      const existing = droppedByTraceId.get(traceId);
      const eventCount = events.length;

      if (existing) {
        existing.eventCount += eventCount;
      } else {
        droppedByTraceId.set(traceId, {
          traceId,
          eventCount,
          reason: decision.reason,
        });
      }

      return;
    }

    const entityType =
      sourceGroup.entityType ??
      getClickhouseEntityType(String(sourceGroup.type));
    const singleton = singletonTypes.has(entityType);

    let packKey: string;
    if (!traceId) {
      packKey = `independent:${index}:${sourceGroup.eventBodyId}`;
    } else if (singleton) {
      packKey = `singleton:${entityType}:${sourceGroup.eventBodyId}`;
    } else {
      packKey = `trace:${traceId}`;
    }

    let batch = batchByPackKey.get(packKey);
    if (!batch) {
      batch = {
        batchId: createBatchId(),
        projectId,
        traceId,
        items: [],
        eventCount: 0,
        singleton,
      };
      batchByPackKey.set(packKey, batch);
      batchOrder.push(batch);
    }

    const skipS3List = skipS3ListFor
      ? skipS3ListFor(sourceGroup)
      : sourceGroup.skipS3List;

    const item: TraceWriteItem = {
      type: sourceGroup.type,
      eventBodyId: sourceGroup.eventBodyId,
      fileKey,
      ...(sourceGroup.bucketPrefix !== undefined
        ? { bucketPrefix: sourceGroup.bucketPrefix }
        : {}),
      ...(skipS3List !== undefined ? { skipS3List: !!skipS3List } : {}),
      events,
    };

    batch.items.push(item);
    batch.eventCount += events.length;
  });

  const batches: TraceWriteBatch[] = [];
  const oversized: TraceWriteBatch[] = [];

  for (const batch of batchOrder) {
    const { singleton: _singleton, ...publicBatch } = batch;

    if (batch.items.length > TRACE_WRITE_BATCH_MAX_ITEMS) {
      publicBatch.oversized = true;
      oversized.push(publicBatch);
    } else {
      batches.push(publicBatch);
    }
  }

  return {
    batches,
    dropped: [...droppedByTraceId.values()],
    oversized,
  };
}

export function getTraceIdFromEvents(
  events: IngestionEventType[],
): string | null {
  const traceCreate = events.find((event) => event.type === "trace-create");
  if (traceCreate && "id" in traceCreate.body) {
    const id = traceCreate.body.id;
    return typeof id === "string" ? id : null;
  }

  const event = events[0];
  if (event && "traceId" in event.body) {
    const traceId = event.body.traceId;
    return typeof traceId === "string" ? traceId : null;
  }

  return null;
}

function resolveDecision(
  decisions:
    | Map<string, TraceSamplingDecision>
    | Record<string, TraceSamplingDecision>
    | undefined,
  traceId: string | null,
  projectId: string,
): TraceSamplingDecision {
  const keepDecision: TraceSamplingDecision = {
    traceId,
    keep: true,
    reason: traceId ? "hash_in" : "no_trace_id",
    bucket: null,
  };

  if (!traceId || !decisions) return keepDecision;

  const decision =
    decisions instanceof Map ? decisions.get(traceId) : decisions[traceId];

  if (!decision) {
    logger.warn("Missing trace sampling decision; keeping trace", {
      projectId,
      traceId,
    });
    return keepDecision;
  }

  return decision;
}
