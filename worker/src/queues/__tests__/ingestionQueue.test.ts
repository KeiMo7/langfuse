import { afterAll, beforeEach, describe, expect, it, vi } from "vitest";
import { type Job } from "bullmq";
import {
  getS3EventStorageClient,
  hasS3SlowdownFlag,
  markProjectIngestFailure,
  markProjectS3Slowdown,
  type QueueName,
  StorageServiceFactory,
  storageKeys,
  type TQueueJobTypes,
} from "@langfuse/shared/src/server";
import { ingestionQueueProcessorBuilder } from "../ingestionQueue";
import { IngestionService } from "../../services/IngestionService";
import { env } from "../../env";

const mocks = vi.hoisted(() => {
  const mergeAndWrite = vi.fn();
  const addToQueue = vi.fn();
  const flushAll = vi.fn();
  const redisExists = vi.fn();
  const redisSet = vi.fn();
  const download = vi.fn();
  const listFiles = vi.fn();

  return {
    mergeAndWrite,
    addToQueue,
    flushAll,
    redisExists,
    redisSet,
    download,
    listFiles,
    writer: { addToQueue, flushAll },
  };
});

vi.mock("@langfuse/shared/src/server", async (importOriginal) => ({
  ...(await importOriginal<typeof import("@langfuse/shared/src/server")>()),
  getS3EventStorageClient: vi.fn(),
  hasS3SlowdownFlag: vi.fn(),
  markProjectS3Slowdown: vi.fn(),
  markProjectIngestFailure: vi.fn(),
  redis: {
    exists: mocks.redisExists,
    set: mocks.redisSet,
  },
}));

vi.mock("../../services/ClickhouseWriter", () => ({
  ClickhouseWriter: { getInstance: vi.fn(() => mocks.writer) },
  TableName: { BlobStorageFileLog: "blob_storage_file_log" },
}));

vi.mock("../../services/IngestionService", () => ({
  IngestionService: vi.fn(function MockIngestionService(this: {
    mergeAndWrite: typeof mocks.mergeAndWrite;
  }) {
    this.mergeAndWrite = mocks.mergeAndWrite;
  }),
}));

const projectId = "slowdown-project";

const ingestionJob = {
  data: {
    id: "job-1",
    timestamp: new Date(),
    name: "ingestion-job",
    payload: {
      authCheck: {
        validKey: true,
        scope: { projectId, accessLevel: "project" },
      },
      data: {
        type: "trace-create",
        eventBodyId: "trace-1",
        fileKey: "file-1",
        skipS3List: true,
      },
    },
  },
  attemptsMade: 0,
  opts: { attempts: 5 },
} as unknown as Job<TQueueJobTypes[QueueName.IngestionQueue]>;

const traceEvent = {
  id: "event-1",
  type: "trace-create",
  body: { id: "trace-1" },
};
const observationEvent = {
  id: "event-2",
  type: "span-create",
  body: { id: "span-1", traceId: "trace-1" },
};

const batchJob = {
  data: {
    id: "batch-job-1",
    timestamp: new Date(),
    name: "ingestion-job",
    payload: {
      authCheck: {
        validKey: true,
        scope: { projectId, accessLevel: "project" },
      },
      data: {
        traceId: "trace-1",
        items: [
          {
            type: "trace-create",
            eventBodyId: "trace-1",
            fileKey: "trace-file",
            bucketPrefix: "prefix/",
            skipS3List: false,
          },
          {
            type: "span-create",
            eventBodyId: "span-1",
            fileKey: "span-file",
            bucketPrefix: "prefix/",
            skipS3List: true,
          },
        ],
      },
    },
  },
  attemptsMade: 0,
  opts: { attempts: 5 },
} as unknown as Job<TQueueJobTypes[QueueName.IngestionQueue]>;

describe("ingestion queue S3 SlowDown handling", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    vi.mocked(hasS3SlowdownFlag).mockResolvedValue(false);

    const storage = StorageServiceFactory.getInstance({
      bucketName: "events",
      credentials: storageKeys("test", "test"),
      region: "us-east-1",
      endpoint: "http://127.0.0.1:1",
      forcePathStyle: true,
      useAzureBlob: false,
      useGoogleCloudStorage: false,
    });
    const slowDown = Object.assign(
      new Error("Please reduce your request rate."),
      { name: "SlowDown", Code: "SlowDown" },
    );
    vi.spyOn(
      (storage as unknown as { client: { send: () => Promise<unknown> } })
        .client,
      "send",
    ).mockRejectedValue(slowDown);
    vi.mocked(getS3EventStorageClient).mockReturnValue(storage);
  });

  it("marks the project for the secondary queue when the storage client wraps a SlowDown", async () => {
    const processor = ingestionQueueProcessorBuilder(true);

    await expect(processor(ingestionJob)).rejects.toThrow(
      "Failed to download file from S3",
    );

    expect(markProjectS3Slowdown).toHaveBeenCalledWith(projectId);
    expect(markProjectIngestFailure).toHaveBeenCalledWith(projectId, {
      source: "ingestion_queue",
      reason: "s3_slowdown",
    });
  });
});

describe("ingestion queue trace write batches", () => {
  const originalSeenCache = env.LANGFUSE_ENABLE_REDIS_SEEN_EVENT_CACHE;

  beforeEach(() => {
    vi.clearAllMocks();
    vi.mocked(hasS3SlowdownFlag).mockResolvedValue(false);
    env.LANGFUSE_ENABLE_REDIS_SEEN_EVENT_CACHE = "true";

    mocks.redisExists.mockResolvedValue(0);
    mocks.redisSet.mockResolvedValue("OK");
    mocks.mergeAndWrite.mockResolvedValue(undefined);
    mocks.listFiles.mockResolvedValue([
      {
        file: "prefix/trace-file.json",
        createdAt: new Date("2024-01-01T00:00:00Z"),
      },
    ]);
    mocks.download.mockImplementation(async (path: string) => {
      if (path === "prefix/trace-file.json") {
        return JSON.stringify([traceEvent]);
      }
      if (path === "prefix/span-file.json") {
        return JSON.stringify([observationEvent]);
      }
      throw new Error(`unexpected path ${path}`);
    });
    vi.mocked(getS3EventStorageClient).mockReturnValue({
      download: mocks.download,
      listFiles: mocks.listFiles,
    } as never);
  });

  afterAll(() => {
    env.LANGFUSE_ENABLE_REDIS_SEEN_EVENT_CACHE = originalSeenCache;
  });

  it("processes items in trace then observation order with one service and no flushAll", async () => {
    const processor = ingestionQueueProcessorBuilder(false);

    await processor(batchJob);

    expect(IngestionService).toHaveBeenCalledTimes(1);
    expect(mocks.mergeAndWrite).toHaveBeenCalledTimes(2);

    const firstCall = mocks.mergeAndWrite.mock.calls[0][0];
    const secondCall = mocks.mergeAndWrite.mock.calls[1][0];

    expect(firstCall.eventType).toBe("trace");
    expect(firstCall.events).toEqual([traceEvent]);
    expect(secondCall.eventType).toBe("observation");
    expect(secondCall.events).toEqual([observationEvent]);
    expect(mocks.flushAll).not.toHaveBeenCalled();

    expect(mocks.redisSet).toHaveBeenCalledWith(
      "langfuse:ingestion:recently-processed:slowdown-project:trace-create:trace-1:trace-file",
      "1",
      "EX",
      300,
    );
    expect(mocks.redisSet).toHaveBeenCalledWith(
      "langfuse:ingestion:recently-processed:slowdown-project:span-create:span-1:span-file",
      "1",
      "EX",
      300,
    );
  });

  it("skips already seen items on retry and redownloads only failed items", async () => {
    const processor = ingestionQueueProcessorBuilder(false);

    mocks.mergeAndWrite
      .mockResolvedValueOnce(undefined)
      .mockRejectedValueOnce(new Error("observation write failed"));

    await expect(processor(batchJob)).rejects.toBeInstanceOf(AggregateError);

    expect(mocks.redisSet).toHaveBeenCalledWith(
      expect.stringContaining(":trace-create:trace-1:trace-file"),
      "1",
      "EX",
      300,
    );

    mocks.redisExists.mockImplementation(async (key: string) =>
      key.endsWith(":trace-file") ? 1 : 0,
    );
    mocks.download.mockClear();
    mocks.mergeAndWrite.mockClear();
    mocks.mergeAndWrite.mockResolvedValue(undefined);

    await processor(batchJob);

    expect(mocks.download).toHaveBeenCalledTimes(1);
    expect(mocks.download).toHaveBeenCalledWith("prefix/span-file.json");
    expect(mocks.mergeAndWrite).toHaveBeenCalledTimes(1);
    expect(mocks.mergeAndWrite.mock.calls[0][0].eventType).toBe("observation");
  });

  it("continues after item failures and aggregates them with counts", async () => {
    const failingJob = {
      ...batchJob,
      data: {
        ...batchJob.data,
        payload: {
          ...batchJob.data.payload,
          data: {
            traceId: "trace-1",
            items: [
              {
                type: "trace-create",
                eventBodyId: "trace-1",
                fileKey: "empty-file",
                bucketPrefix: "prefix/",
              },
              {
                type: "span-create",
                eventBodyId: "missing-span",
                fileKey: "missing-file",
                bucketPrefix: "prefix/",
                skipS3List: true,
              },
              {
                type: "generation-create",
                eventBodyId: "bad-json",
                fileKey: "bad-json-file",
                bucketPrefix: "prefix/",
                skipS3List: true,
              },
            ],
          },
        },
      },
    } as unknown as Job<TQueueJobTypes[QueueName.IngestionQueue]>;

    mocks.listFiles.mockResolvedValueOnce([
      {
        file: "prefix/empty-file.json",
        createdAt: new Date("2024-01-01T00:00:00Z"),
      },
    ]);
    mocks.download.mockImplementation(async (path: string) => {
      if (path === "prefix/empty-file.json") return "[]";
      if (path === "prefix/missing-file.json") {
        throw new Error("NoSuchKey: object does not exist");
      }
      if (path === "prefix/bad-json-file.json") return "{not json";
      throw new Error(`unexpected path ${path}`);
    });

    const processor = ingestionQueueProcessorBuilder(false);

    await expect(processor(failingJob)).rejects.toMatchObject({
      message: expect.stringContaining("1 succeeded and 2 failed"),
      cause: expect.objectContaining({
        message: "NoSuchKey: object does not exist",
      }),
      errors: [expect.any(SyntaxError)],
    });
  });
});
