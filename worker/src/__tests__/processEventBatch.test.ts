import { beforeEach, describe, it, expect, assert, vi } from "vitest";
import { eventTypes } from "../../../packages/shared/src/server/ingestion/types";
import { processEventBatch } from "../../../packages/shared/src/server/ingestion/processEventBatch";
import { computeTraceSamplingDecision } from "../../../packages/shared/src/server/ingestion/sampling";
import { env } from "../../../packages/shared/src/env";
import * as instrumentation from "../../../packages/shared/src/server/instrumentation";
import { ServiceUnavailableError } from "../../../packages/shared/src/errors/ServiceUnavailableError";
import { logger } from "../../../packages/shared/src/server/logger";
import {
  createUnknownSdkIngestionAttribution,
  UNKNOWN_INGESTION_SDK_VALUE,
} from "../../../packages/shared/src/server/ingestion/ingestionAttribution";

const { getQueueInstanceMock, queueAddMock, uploadJsonMock } = vi.hoisted(
  () => ({
    getQueueInstanceMock: vi.fn(),
    queueAddMock: vi.fn(),
    uploadJsonMock: vi.fn(),
  }),
);

vi.mock("../../../packages/shared/src/server/redis/redis", () => ({
  redis: {},
}));

vi.mock("../../../packages/shared/src/env", () => ({
  env: {
    LANGFUSE_INGESTION_PROCESSING_SAMPLED_PROJECTS: new Map(),
    LANGFUSE_INGESTION_QUEUE_DELAY_MS: 5000,
    LANGFUSE_S3_EVENT_UPLOAD_BUCKET: "event-upload",
    LANGFUSE_S3_EVENT_UPLOAD_PREFIX: "",
    LANGFUSE_S3_EVENT_UPLOAD_FORCE_PATH_STYLE: "false",
    LANGFUSE_SKIP_S3_LIST_FOR_OBSERVATIONS_PROJECT_IDS: "",
  },
}));

vi.mock("../../../packages/shared/src/server/redis/ingestionQueue", () => ({
  IngestionQueue: {
    getInstance: getQueueInstanceMock,
  },
}));

vi.mock("../../../packages/shared/src/server/services/StorageService", () => ({
  StorageService: undefined,
  storageKeys: () => undefined,
  StorageServiceFactory: {
    getInstance: () => ({
      uploadJson: uploadJsonMock,
    }),
  },
}));

const createTraceCreateEvent = () => {
  const timestamp = "2024-10-12T12:13:14.123Z";

  return {
    id: "event-id",
    timestamp,
    type: eventTypes.TRACE_CREATE,
    body: {
      id: "trace-id",
      timestamp,
      name: "trace",
      environment: "default",
    },
  };
};

const createObservationEvent = (
  index: number | string,
  traceId = "trace-id",
) => {
  const timestamp = "2024-10-12T12:13:14.123Z";

  return {
    id: `span-event-${index}`,
    timestamp,
    type: eventTypes.SPAN_CREATE,
    body: {
      id: `span-body-${index}`,
      traceId,
      name: `span-${index}`,
      environment: "default",
    },
  };
};

const createScoreEvent = (id: string, traceId: string) => {
  const timestamp = "2024-10-12T12:13:14.123Z";

  return {
    id: `${id}-event`,
    timestamp,
    type: eventTypes.SCORE_CREATE,
    body: {
      id,
      traceId,
      name: `${id}-name`,
      value: 1,
      environment: "default",
    },
  };
};

const createAuthCheck = () =>
  ({
    validKey: true as const,
    scope: {
      projectId: "project-id",
      accessLevel: "project" as const,
      publicKey: "pk-lf-public",
    },
  }) as const;

const setSampleRate = (rate: number | undefined) => {
  const projects =
    env.LANGFUSE_INGESTION_PROCESSING_SAMPLED_PROJECTS as unknown as Map<
      string,
      number
    >;
  if (rate === undefined) projects.delete("project-id");
  else projects.set("project-id", rate);
};

describe("processEventBatch", () => {
  const loggerErrorSpy = vi.spyOn(logger, "error");

  beforeEach(() => {
    vi.clearAllMocks();
    setSampleRate(undefined);
    getQueueInstanceMock.mockReturnValue({ add: queueAddMock });
    queueAddMock.mockResolvedValue(undefined);
    uploadJsonMock.mockResolvedValue(undefined);
  });

  it("returns early on empty input", async () => {
    // Auth check with missing projectId will cause an exception unless
    // there is an early return in processEventBatch
    const authCheck = {
      validKey: true as const,
      scope: {
        projectId: null,
        accessLevel: "project" as const,
      },
    };

    const attribution = createUnknownSdkIngestionAttribution({ authCheck });

    assert.doesNotThrow(
      async () => await processEventBatch([], authCheck, { attribution }),
      "UnauthorizedError",
    );

    const res = await processEventBatch([], authCheck, { attribution });
    expect(res.successes).toEqual([]);
    expect(res.errors).toEqual([]);
  });

  it("adds request attribution to ingestion queue payloads", async () => {
    const authCheck = {
      validKey: true as const,
      scope: {
        projectId: "project-id",
        accessLevel: "project" as const,
        publicKey: "pk-lf-public",
      },
    };

    const result = await processEventBatch(
      [createTraceCreateEvent()],
      authCheck,
      {
        delay: 0,
        attribution: {
          ingestionApiKey: "pk-lf-attributed",
          ingestionSdkName: "python",
          ingestionSdkVersion: "3.4.0",
        },
      },
    );

    expect(result).toEqual({
      successes: [{ id: "event-id", status: 201 }],
      errors: [],
    });
    expect(queueAddMock).toHaveBeenCalledTimes(1);
    expect(queueAddMock.mock.calls[0][1].payload.data).toMatchObject({
      ingestionApiKey: "pk-lf-attributed",
      ingestionSdkName: "python",
      ingestionSdkVersion: "3.4.0",
    });
  });

  it("enqueues scores without delay while other api events keep it", async () => {
    vi.useFakeTimers({ toFake: ["Date"] });
    vi.setSystemTime(new Date("2024-10-12T12:00:00.000Z"));

    try {
      const authCheck = {
        validKey: true as const,
        scope: {
          projectId: "project-id",
          accessLevel: "project" as const,
        },
      };
      const timestamp = "2024-10-12T12:13:14.123Z";

      await processEventBatch(
        [
          createTraceCreateEvent(),
          {
            id: "score-event-id",
            timestamp,
            type: eventTypes.SCORE_CREATE,
            body: {
              id: "score-id",
              traceId: "trace-id",
              name: "accuracy",
              value: 1,
              environment: "default",
            },
          },
        ],
        authCheck,
        { attribution: createUnknownSdkIngestionAttribution({ authCheck }) },
      );

      const delayByType = Object.fromEntries(
        queueAddMock.mock.calls.map((call) => [
          call[1].payload.data.type,
          call[2].delay,
        ]),
      );
      expect(delayByType).toEqual({
        [eventTypes.TRACE_CREATE]: 5000,
        [eventTypes.SCORE_CREATE]: 0,
      });
    } finally {
      vi.useRealTimers();
    }
  });

  it("forwards explicit unknown SDK attribution", async () => {
    const authCheck = {
      validKey: true as const,
      scope: {
        projectId: "project-id",
        accessLevel: "project" as const,
        publicKey: "pk-lf-public",
      },
    };

    await processEventBatch([createTraceCreateEvent()], authCheck, {
      delay: 0,
      attribution: createUnknownSdkIngestionAttribution({ authCheck }),
    });

    expect(queueAddMock.mock.calls[0][1].payload.data).toMatchObject({
      ingestionApiKey: "pk-lf-public",
      ingestionSdkName: UNKNOWN_INGESTION_SDK_VALUE,
      ingestionSdkVersion: UNKNOWN_INGESTION_SDK_VALUE,
    });
  });
  it("rejects with a retryable 503 when S3 throttles the upload", async () => {
    const authCheck = {
      validKey: true as const,
      scope: {
        projectId: "project-id",
        accessLevel: "project" as const,
        publicKey: "pk-lf-public",
      },
    };
    uploadJsonMock.mockRejectedValue(
      new Error("Failed to upload JSON", {
        cause: Object.assign(new Error("Please reduce your request rate."), {
          name: "SlowDown",
        }),
      }),
    );

    const error = await processEventBatch(
      [createTraceCreateEvent()],
      authCheck,
      {
        delay: 0,
        attribution: createUnknownSdkIngestionAttribution({ authCheck }),
      },
    ).catch((e: unknown) => e);

    expect(error).toBeInstanceOf(ServiceUnavailableError);
    expect(error).toMatchObject({ httpCode: 503, retryAfterSeconds: 2 });
    expect(queueAddMock).not.toHaveBeenCalled();
    expect(loggerErrorSpy).not.toHaveBeenCalledWith(
      "Failed to upload event to S3",
      expect.anything(),
    );
  });

  it("does not report a retryable 503 when another upload failed for a different reason", async () => {
    const authCheck = {
      validKey: true as const,
      scope: {
        projectId: "project-id",
        accessLevel: "project" as const,
        publicKey: "pk-lf-public",
      },
    };
    const otherTrace = createTraceCreateEvent();
    otherTrace.id = "other-event-id";
    otherTrace.body.id = "other-trace-id";
    uploadJsonMock
      .mockRejectedValueOnce(
        Object.assign(new Error("throttled"), { name: "SlowDown" }),
      )
      .mockRejectedValueOnce(new Error("Access Denied"));

    const error = await processEventBatch(
      [createTraceCreateEvent(), otherTrace],
      authCheck,
      {
        delay: 0,
        attribution: createUnknownSdkIngestionAttribution({ authCheck }),
      },
    ).catch((e: unknown) => e);

    expect(error).toBeInstanceOf(Error);
    expect(error).not.toBeInstanceOf(ServiceUnavailableError);
    const uploadErrorLogs = loggerErrorSpy.mock.calls.filter(
      ([message]) => message === "Failed to upload event to S3",
    );
    expect(uploadErrorLogs).toEqual([
      [
        "Failed to upload event to S3",
        { error: expect.objectContaining({ message: "Access Denied" }) },
      ],
    ]);
  });

  it("merges trace and observation bodies while keeping the score task independent", async () => {
    setSampleRate(1);
    const authCheck = createAuthCheck();

    const result = await processEventBatch(
      [
        createTraceCreateEvent(),
        createObservationEvent(1),
        createObservationEvent(2),
        createObservationEvent(3),
        createScoreEvent("score-1", "trace-id"),
      ],
      authCheck,
      {
        delay: 0,
        attribution: createUnknownSdkIngestionAttribution({ authCheck }),
      },
    );

    expect(result.successes).toHaveLength(5);
    expect(uploadJsonMock).toHaveBeenCalledTimes(5);
    expect(queueAddMock).toHaveBeenCalledTimes(2);

    const payloads = queueAddMock.mock.calls.map(
      (call) => call[1].payload.data,
    );
    const batchPayload = payloads.find((payload) => payload.items);
    const scorePayload = payloads.find(
      (payload) => payload.type === "score-create",
    );

    expect(batchPayload.traceId).toBe("trace-id");
    expect(batchPayload.items).toHaveLength(4);
    expect(batchPayload.items.map((item) => item.eventBodyId)).toEqual([
      "trace-id",
      "span-body-1",
      "span-body-2",
      "span-body-3",
    ]);
    expect(batchPayload.items.every((item) => !("events" in item))).toBe(true);
    expect(scorePayload).toMatchObject({
      type: "score-create",
      eventBodyId: "score-1",
      traceId: "trace-id",
    });
  });

  it("uploads three kept non-score bodies and enqueues one three-item task", async () => {
    setSampleRate(1);
    const authCheck = createAuthCheck();

    await processEventBatch(
      [
        createTraceCreateEvent(),
        createObservationEvent(1),
        createObservationEvent(2),
      ],
      authCheck,
      {
        delay: 0,
        attribution: createUnknownSdkIngestionAttribution({ authCheck }),
      },
    );

    expect(uploadJsonMock).toHaveBeenCalledTimes(3);
    expect(queueAddMock).toHaveBeenCalledTimes(1);
    expect(queueAddMock.mock.calls[0][1].payload.data.items).toHaveLength(3);
  });

  it("does not upload or enqueue dropped traces but still returns 201", async () => {
    setSampleRate(0);
    const authCheck = createAuthCheck();

    const result = await processEventBatch(
      [createTraceCreateEvent(), createObservationEvent(1)],
      authCheck,
      {
        delay: 0,
        attribution: createUnknownSdkIngestionAttribution({ authCheck }),
      },
    );

    expect(result.successes).toEqual([
      { id: "event-id", status: 201 },
      { id: "span-event-1", status: 201 },
    ]);
    expect(result.errors).toEqual([]);
    expect(uploadJsonMock).not.toHaveBeenCalled();
    expect(queueAddMock).not.toHaveBeenCalled();
  });

  it("handles kept and dropped traces with a score on the kept trace", async () => {
    setSampleRate(0.5);

    let keptTraceId = "";
    let droppedTraceId = "";
    for (let i = 0; i < 1000 && (!keptTraceId || !droppedTraceId); i += 1) {
      const candidate = `sample-trace-${i}`;
      const decision = computeTraceSamplingDecision({
        projectId: "project-id",
        traceId: candidate,
        sampleRate: 0.5,
      });
      if (decision.keep && !keptTraceId) keptTraceId = candidate;
      if (!decision.keep && !droppedTraceId) droppedTraceId = candidate;
    }

    const keptTrace = createTraceCreateEvent();
    keptTrace.body.id = keptTraceId;
    const droppedTrace = createTraceCreateEvent();
    droppedTrace.id = "dropped-event-id";
    droppedTrace.body.id = droppedTraceId;

    const authCheck = createAuthCheck();
    const result = await processEventBatch(
      [
        keptTrace,
        createObservationEvent("kept", keptTraceId),
        createScoreEvent("score-kept", keptTraceId),
        droppedTrace,
      ],
      authCheck,
      {
        delay: 0,
        attribution: createUnknownSdkIngestionAttribution({ authCheck }),
      },
    );

    expect(result.successes).toHaveLength(4);
    expect(uploadJsonMock).toHaveBeenCalledTimes(3);

    const uploadedBodyIds = uploadJsonMock.mock.calls.flatMap((call) =>
      call[1].map((event: { body: { id: string } }) => event.body.id),
    );
    expect(uploadedBodyIds).toEqual(
      expect.arrayContaining([keptTraceId, "span-body-kept", "score-kept"]),
    );
    expect(uploadedBodyIds).not.toContain(droppedTraceId);

    expect(queueAddMock).toHaveBeenCalledTimes(2);
    const payloads = queueAddMock.mock.calls.map(
      (call) => call[1].payload.data,
    );
    const batchPayload = payloads.find((payload) => payload.items);
    const scoreCall = queueAddMock.mock.calls.find(
      (call) => call[1].payload.data.type === "score-create",
    );

    expect(batchPayload.items).toHaveLength(2);
    expect(scoreCall?.[2].delay).toBe(0);
    expect(getQueueInstanceMock).toHaveBeenCalledWith({
      shardingKey: `project-id-${keptTraceId}`,
    });
  });

  it("falls back to 257 legacy tasks for an oversized trace and records one metric", async () => {
    setSampleRate(1);
    const recordIncrementSpy = vi.spyOn(instrumentation, "recordIncrement");
    const authCheck = createAuthCheck();
    const events = Array.from({ length: 257 }, (_, index) =>
      createObservationEvent(index, "oversized-trace"),
    );

    const result = await processEventBatch(events, authCheck, {
      delay: 0,
      attribution: createUnknownSdkIngestionAttribution({ authCheck }),
    });

    expect(result.successes).toHaveLength(257);
    expect(uploadJsonMock).toHaveBeenCalledTimes(257);
    expect(queueAddMock).toHaveBeenCalledTimes(257);
    expect(
      queueAddMock.mock.calls.every(
        (call) => !call[1].payload.data.items && call[1].payload.data.type,
      ),
    ).toBe(true);
    expect(
      recordIncrementSpy.mock.calls.filter(
        ([name]) => name === "langfuse.ingestion.write_batch_fallback",
      ),
    ).toHaveLength(1);

    recordIncrementSpy.mockRestore();
  });
});
