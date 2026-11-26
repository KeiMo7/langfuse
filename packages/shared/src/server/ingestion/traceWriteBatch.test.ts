import { describe, expect, it } from "vitest";

import { IngestionEvent } from "../queues";
import {
  packTraceWriteBatches,
  type PackTraceWriteGroup,
} from "./traceWriteBatch";

const event = (type: string, id: string, traceId?: string) =>
  ({
    id: `event-${id}`,
    type,
    body: traceId ? { id, traceId } : { id },
  }) as unknown as NonNullable<PackTraceWriteGroup["events"]>[number];

const group = (
  index: number,
  overrides: Partial<PackTraceWriteGroup> = {},
): PackTraceWriteGroup => ({
  type: "span-create",
  eventBodyId: `body-${index}`,
  fileKey: `file-${index}`,
  bucketPrefix: "prefix/",
  events: [event("span-create", `body-${index}`, "trace-1")],
  entityType: "observation",
  ...overrides,
});

const authCheck = {
  validKey: true,
  scope: { projectId: "p1" },
} as const;

describe("packTraceWriteBatches", () => {
  it("groups non-score bodies by trace and leaves scores singleton", () => {
    const result = packTraceWriteBatches({
      projectId: "p1",
      createBatchId: (() => "batch-1") as () => string,
      groups: [
        group(0, {
          type: "trace-create",
          eventBodyId: "trace-1",
          entityType: "trace",
          events: [event("trace-create", "trace-1")],
        }),
        group(1),
        group(2, {
          type: "score-create",
          eventBodyId: "score-1",
          entityType: "score",
        }),
      ],
      decisions: new Map([
        [
          "trace-1",
          {
            traceId: "trace-1",
            keep: true,
            reason: "hash_in",
            bucket: 1,
          },
        ],
      ]),
    });

    expect(result.dropped).toEqual([]);
    expect(result.batches).toHaveLength(2);
    expect(result.batches[0].batchId).toBe("batch-1");
    expect(result.batches[0].items.map((item) => item.eventBodyId)).toEqual([
      "trace-1",
      "body-1",
    ]);
    expect(result.batches[1].items).toHaveLength(1);
    expect("entityType" in result.batches[1].items[0]).toBe(false);
  });

  it("returns dropped traces without batches", () => {
    const result = packTraceWriteBatches({
      projectId: "p1",
      groups: [group(0)],
      decisions: new Map([
        [
          "trace-1",
          {
            traceId: "trace-1",
            keep: false,
            reason: "rate_zero",
            bucket: 1,
          },
        ],
      ]),
    });

    expect(result.batches).toEqual([]);
    expect(result.oversized).toEqual([]);
    expect(result.dropped).toEqual([
      { traceId: "trace-1", eventCount: 1, reason: "rate_zero" },
    ]);
  });

  it("separates oversized traces", () => {
    const result = packTraceWriteBatches({
      projectId: "p1",
      groups: Array.from({ length: 257 }, (_, index) => group(index)),
    });

    expect(result.batches).toEqual([]);
    expect(result.oversized).toHaveLength(1);
    expect(result.oversized[0].items).toHaveLength(257);
    expect(result.oversized[0].oversized).toBe(true);
  });
});

describe("IngestionEvent queue contract", () => {
  it("accepts legacy and two-to-256 item payloads", () => {
    const legacy = IngestionEvent.safeParse({
      data: { type: "trace-create", eventBodyId: "trace-1" },
      authCheck,
    });
    expect(legacy.success).toBe(true);

    const batch = IngestionEvent.safeParse({
      data: {
        traceId: "trace-1",
        items: [
          { type: "trace-create", eventBodyId: "trace-1", fileKey: "f1" },
          { type: "span-create", eventBodyId: "span-1", fileKey: "f2" },
        ],
      },
      authCheck,
    });
    expect(batch.success).toBe(true);
  });

  it.each([1, 257])("rejects %s items", (count) => {
    const result = IngestionEvent.safeParse({
      data: {
        traceId: "trace-1",
        items: Array.from({ length: count }, (_, index) => ({
          type: "span-create",
          eventBodyId: `body-${index}`,
          fileKey: `file-${index}`,
        })),
      },
      authCheck,
    });

    expect(result.success).toBe(false);
  });

  it("requires exactly one payload form", () => {
    expect(
      IngestionEvent.safeParse({
        data: { eventBodyId: "trace-1" },
        authCheck,
      }).success,
    ).toBe(false);

    expect(
      IngestionEvent.safeParse({
        data: {
          type: "trace-create",
          eventBodyId: "trace-1",
          items: [
            { type: "trace-create", eventBodyId: "a", fileKey: "a" },
            { type: "span-create", eventBodyId: "b", fileKey: "b" },
          ],
        },
        authCheck,
      }).success,
    ).toBe(false);
  });
});
