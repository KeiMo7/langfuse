import { Job, Processor, UnrecoverableError } from "bullmq";
import {
  clickhouseClient,
  getClickhouseEntityType,
  getCurrentSpan,
  getS3EventStorageClient,
  hasS3SlowdownFlag,
  IngestionEventType,
  isS3SlowDownError,
  logger,
  markProjectIngestFailure,
  markProjectS3Slowdown,
  QueueName,
  rawEventBucketPrefix,
  recordDistribution,
  recordHistogram,
  recordIncrement,
  redis,
  SecondaryIngestionQueue,
  TQueueJobTypes,
  type IngestionAttribution,
  UNKNOWN_INGESTION_SDK_VALUE,
  toClickhouseDateTime,
} from "@langfuse/shared/src/server";
import { prisma } from "@langfuse/shared/src/db";

import { env, v4WritesToEventsTable } from "../env";
import { IngestionService } from "../services/IngestionService";
import { ClickhouseWriter, TableName } from "../services/ClickhouseWriter";
import { chunk } from "lodash";
import { randomUUID } from "crypto";
import {
  exceedsNonSlowDownAttemptBudget,
  logRetryableJobFailure,
} from "./jobFailureLog";

/**
 * Attempts for secondary-queue failures other than S3 SlowDown. The queue's
 * larger default budget only exists to outlast throttling.
 */
const SECONDARY_QUEUE_NON_SLOWDOWN_ATTEMPTS = 5;

const ENTITY_PROCESSING_ORDER: Record<string, number> = {
  trace: 0,
  observation: 1,
  dataset_run_item: 2,
  score: 3,
};

type IngestionJobData =
  TQueueJobTypes[QueueName.IngestionQueue]["payload"]["data"];
type BatchItem = NonNullable<IngestionJobData["items"]>[number];
type ResolvedIngestionItem = Omit<BatchItem, "fileKey"> & {
  fileKey?: string;
};

type DownloadedIngestionItem = {
  events: IngestionEventType[];
  eventFiles: { file: string; createdAt: Date }[];
  bucketPrefix: string;
  firstWriteAt: Date;
};

type ItemProcessingResult = {
  item: ResolvedIngestionItem;
  status: "processed" | "skipped" | "empty";
};

export const ingestionQueueProcessorBuilder = (
  enableRedirectToSecondaryQueue: boolean,
): Processor => {
  const projectIdsToRedirectToSecondaryQueue =
    env.LANGFUSE_SECONDARY_INGESTION_QUEUE_ENABLED_PROJECT_IDS?.split(",") ??
    [];

  return async (job: Job<TQueueJobTypes[QueueName.IngestionQueue]>) => {
    const projectId = job.data.payload.authCheck.scope.projectId;

    try {
      const span = getCurrentSpan();
      if (span) {
        span.setAttribute("messaging.bullmq.job.input.id", job.data.id);
        span.setAttribute("messaging.bullmq.job.input.projectId", projectId);
        span.setAttribute(
          "messaging.bullmq.job.input.eventBodyId",
          job.data.payload.data.eventBodyId ?? "",
        );
        span.setAttribute(
          "messaging.bullmq.job.input.type",
          job.data.payload.data.type ?? "",
        );
        span.setAttribute(
          "messaging.bullmq.job.input.fileKey",
          job.data.payload.data.fileKey ?? "",
        );
        if (job.data.payload.data.traceId) {
          span.setAttribute(
            "messaging.bullmq.job.input.traceId",
            job.data.payload.data.traceId,
          );
        }
      }

      // We write the new file into the ClickHouse event log to keep track for retention and deletions
      const clickhouseWriter = ClickhouseWriter.getInstance();
      const data = job.data.payload.data;
      const isBatchJob = Array.isArray(data.items);

      if (isBatchJob && (data.items!.length < 2 || data.items!.length > 256)) {
        throw new Error(
          `Invalid ingestion batch item count: ${data.items!.length}`,
        );
      }

      const legacyBucketPrefix = data.type
        ? (data.bucketPrefix ??
          rawEventBucketPrefix({
            projectId,
            entityType: getClickhouseEntityType(data.type),
            rawEntityIdSegment: data.eventBodyId ?? "",
          }))
        : undefined;

      const legacyItem: ResolvedIngestionItem | null = !isBatchJob
        ? {
            type: data.type!,
            eventBodyId: data.eventBodyId!,
            ...(data.fileKey !== undefined ? { fileKey: data.fileKey } : {}),
            ...(data.skipS3List !== undefined
              ? { skipS3List: data.skipS3List }
              : {}),
            ...(legacyBucketPrefix !== undefined
              ? { bucketPrefix: legacyBucketPrefix }
              : {}),
          }
        : null;

      // If fileKey was processed within the last minutes, i.e. has a match in redis, we skip processing.
      // `eventBodyId` is the raw (un-sanitized) ID; for legacy pre-fix data
      // it may contain "/" which is unusual but valid in a Redis key.
      if (legacyItem && (await isItemSeen(projectId, legacyItem))) {
        logger.debug(
          `Skipping ingestion event ${legacyItem.fileKey} for project ${projectId}`,
        );
        return;
      }

      // Check if project should be redirected to secondary queue
      const shouldRedirectEnv =
        projectIdsToRedirectToSecondaryQueue.includes(projectId);
      const shouldRedirectSlowdown = await hasS3SlowdownFlag(projectId);

      if (
        enableRedirectToSecondaryQueue &&
        (shouldRedirectEnv || shouldRedirectSlowdown)
      ) {
        logger.debug(
          `Redirecting ingestion event to secondary queue for project ${projectId}`,
          {
            reason: shouldRedirectSlowdown ? "s3_slowdown_flag" : "env_config",
          },
        );
        const shardingKey = getJobShardingKey(projectId, data, legacyItem);
        const secondaryQueue = SecondaryIngestionQueue.getInstance({
          shardingKey,
        });
        if (secondaryQueue) {
          await secondaryQueue.add(QueueName.IngestionSecondaryQueue, job.data);
          // If we don't redirect, we continue with the ingestion. Otherwise, we finish here.
          return;
        }
      }

      const s3Client = getS3EventStorageClient(
        env.LANGFUSE_S3_EVENT_UPLOAD_BUCKET,
      );

      logger.debug(
        `Processing ingestion event ${
          enableRedirectToSecondaryQueue ? "" : "secondary"
        }`,
        {
          projectId,
          payload: data,
        },
      );

      const forwardToEventsTable =
        data.forwardToEventsTable ?? v4WritesToEventsTable(env);
      const attribution: IngestionAttribution = {
        ingestionApiKey: data.ingestionApiKey ?? "",
        ingestionSdkName: data.ingestionSdkName || UNKNOWN_INGESTION_SDK_VALUE,
        ingestionSdkVersion:
          data.ingestionSdkVersion || UNKNOWN_INGESTION_SDK_VALUE,
      };

      let ingestionService: IngestionService | null = null;
      const getIngestionService = () => {
        if (!redis) throw new Error("Redis not available");
        if (!prisma) throw new Error("Prisma not available");
        if (!ingestionService) {
          ingestionService = new IngestionService(
            redis,
            prisma,
            clickhouseWriter,
            clickhouseClient(),
          );
        }
        return ingestionService;
      };

      const downloadItem = async (
        item: ResolvedIngestionItem,
      ): Promise<DownloadedIngestionItem> => {
        const clickhouseEntityType = getClickhouseEntityType(item.type);
        const bucketPrefix =
          item.bucketPrefix ??
          rawEventBucketPrefix({
            projectId,
            entityType: clickhouseEntityType,
            rawEntityIdSegment: item.eventBodyId,
          });

        let eventFiles: { file: string; createdAt: Date }[] = [];
        const events: IngestionEventType[] = [];
        let totalS3DownloadSizeBytes = 0;

        // Check if we should skip S3 list operation
        const shouldSkipS3List = item.skipS3List && item.fileKey;

        if (shouldSkipS3List) {
          // Direct file download - skip S3 list operation
          const filePath = `${bucketPrefix}${item.fileKey}.json`;
          eventFiles = [{ file: filePath, createdAt: new Date() }];

          const file = await s3Client.download(filePath);
          const fileSize = file.length;

          recordHistogram("langfuse.ingestion.s3_file_size_bytes", fileSize, {
            skippedS3List: "true",
          });
          totalS3DownloadSizeBytes += fileSize;

          const parsedFile = JSON.parse(file);
          events.push(
            ...(Array.isArray(parsedFile) ? parsedFile : [parsedFile]),
          );
        } else {
          eventFiles = await s3Client.listFiles(bucketPrefix);
          // Listings are ordered by key (client-supplied event ids), not by
          // upload time. Sort so arrival order is upload order; score merges
          // rely on that for same-timestamp re-sends.
          eventFiles.sort(
            (a, b) => a.createdAt.getTime() - b.createdAt.getTime(),
          );

          // Process files in batches
          // If a user has 5k events, this will likely take 100 seconds.
          const downloadAndParseFile = async (fileRef: { file: string }) => {
            const file = await s3Client.download(fileRef.file);
            const fileSize = file.length;

            recordHistogram("langfuse.ingestion.s3_file_size_bytes", fileSize, {
              skippedS3List: "false",
            });
            totalS3DownloadSizeBytes += fileSize;

            const parsedFile = JSON.parse(file);
            return Array.isArray(parsedFile) ? parsedFile : [parsedFile];
          };

          const S3_CONCURRENT_READS = env.LANGFUSE_S3_CONCURRENT_READS;
          const batches = chunk(eventFiles, S3_CONCURRENT_READS);
          for (const batch of batches) {
            const batchEvents = await Promise.all(
              batch.map(downloadAndParseFile),
            );
            events.push(...batchEvents.flat());
          }
        }

        recordDistribution(
          "langfuse.ingestion.count_files_distribution",
          eventFiles.length,
          {
            kind: clickhouseEntityType,
          },
        );
        span?.setAttribute(
          "langfuse.ingestion.event.count_files",
          eventFiles.length,
        );
        span?.setAttribute(
          "langfuse.ingestion.event.kind",
          clickhouseEntityType,
        );
        span?.setAttribute(
          "langfuse.ingestion.s3_all_files_size_bytes",
          totalS3DownloadSizeBytes,
        );

        return {
          events,
          eventFiles,
          bucketPrefix,
          firstWriteAt: eventFiles[0]?.createdAt ?? new Date(),
        };
      };

      const addBlobStorageFileLog = (
        item: ResolvedIngestionItem,
        downloaded: DownloadedIngestionItem,
      ) => {
        if (env.LANGFUSE_ENABLE_BLOB_STORAGE_FILE_LOG !== "true") return;
        if (!item.fileKey) return;

        const clickhouseEntityType = getClickhouseEntityType(item.type);
        // Recover the canonical entity id from the downloaded event body, not
        // from the queue payload. On replay, `eventBodyId` is the literal
        // S3-side segment; `event.body.id` is the raw SDK id.
        const canonicalEntityId =
          downloaded.events[0]?.body?.id ?? item.eventBodyId;
        const fileName = `${item.fileKey}.json`;

        clickhouseWriter.addToQueue?.(TableName.BlobStorageFileLog, {
          id: randomUUID(),
          project_id: projectId,
          entity_type: clickhouseEntityType,
          entity_id: canonicalEntityId,
          event_id: item.fileKey,
          bucket_name: env.LANGFUSE_S3_EVENT_UPLOAD_BUCKET,
          bucket_path: `${downloaded.bucketPrefix}${fileName}`,
          created_at: toClickhouseDateTime(),
          updated_at: toClickhouseDateTime(),
          event_ts: toClickhouseDateTime(),
          is_deleted: 0,
        });
      };

      const processItem = async (
        item: ResolvedIngestionItem,
      ): Promise<ItemProcessingResult> => {
        if (await isItemSeen(projectId, item)) {
          return { item, status: "skipped" };
        }

        const downloaded = await downloadItem(item);

        if (downloaded.events.length === 0) {
          logger.warn(
            `No events found for project ${projectId} and event ${item.eventBodyId}`,
          );
          return { item, status: "empty" };
        }

        addBlobStorageFileLog(item, downloaded);

        const canonicalEntityId =
          downloaded.events[0].body?.id ?? item.eventBodyId;
        const ingestionService = getIngestionService();

        await ingestionService.mergeAndWrite({
          eventType: getClickhouseEntityType(downloaded.events[0].type),
          projectId,
          entityId: canonicalEntityId,
          createdAtTimestamp: downloaded.firstWriteAt,
          events: downloaded.events,
          forwardToEventsTable,
          attribution,
        });

        await markItemFilesSeen(projectId, item, downloaded.eventFiles);

        return { item, status: "processed" };
      };

      if (legacyItem) {
        await processItem(legacyItem);
        return;
      }

      if (!data.items || data.items.length === 0) {
        throw new Error("Ingestion batch job is missing items");
      }

      // Construct the service once. Its PromptService and SkillCacheService
      // constructors are therefore also invoked once for the whole trace batch.
      getIngestionService();

      const orderedItems = data.items
        .map((item, index) => ({
          item,
          index,
          entityType: getClickhouseEntityType(item.type),
        }))
        .sort((a, b) => {
          const orderDiff =
            (ENTITY_PROCESSING_ORDER[a.entityType] ?? Number.MAX_SAFE_INTEGER) -
            (ENTITY_PROCESSING_ORDER[b.entityType] ?? Number.MAX_SAFE_INTEGER);

          return orderDiff !== 0 ? orderDiff : a.index - b.index;
        });

      let successCount = 0;
      const failures: { item: ResolvedIngestionItem; error: unknown }[] = [];

      for (const { item } of orderedItems) {
        try {
          const result = await processItem(item);
          successCount += 1;

          if (result.status === "skipped") {
            recordIncrement("langfuse.ingestion.batch_item_skipped", 1, {
              projectId,
              type: item.type,
            });
          }
        } catch (error) {
          if (isS3SlowDownError(error)) {
            logger.warn(
              "S3 SlowDown error during batch item processing; stopping batch",
              {
                projectId,
                fileKey: item.fileKey,
                eventBodyId: item.eventBodyId,
              },
            );
            throw error;
          }

          logger.error("Batch ingestion item failed", {
            projectId,
            fileKey: item.fileKey,
            eventBodyId: item.eventBodyId,
            errorName: error instanceof Error ? error.name : "UnknownError",
            errorMessage:
              error instanceof Error ? error.message : String(error),
          });
          failures.push({ item, error });
        }
      }

      if (failures.length > 0) {
        const failureCount = failures.length;
        const firstError = failures[0].error;
        const remainingErrors = failures.slice(1).map(({ error }) => error);
        const message = `Trace write batch processing finished with ${successCount} succeeded and ${failureCount} failed item(s)`;

        throw new AggregateError(remainingErrors, message, {
          cause: firstError,
        });
      }
    } catch (e) {
      // Check if this is a SlowDown error and mark the project for secondary queue
      if (isS3SlowDownError(e)) {
        logger.warn(
          "S3 SlowDown error during ingestion processing, marking project for secondary queue",
          { projectId, error: e },
        );
        await markProjectS3Slowdown(projectId);
        markProjectIngestFailure(projectId, {
          source: "ingestion_queue",
          reason: "s3_slowdown",
        });
      } else {
        markProjectIngestFailure(projectId, {
          source: "ingestion_queue",
          reason: "processing_error",
        });
      }

      logRetryableJobFailure({
        message: `Failed job ingestion processing for ${projectId}`,
        error: e,
        job,
      });
      if (
        !enableRedirectToSecondaryQueue &&
        exceedsNonSlowDownAttemptBudget(
          job,
          e,
          SECONDARY_QUEUE_NON_SLOWDOWN_ATTEMPTS,
        )
      ) {
        throw new UnrecoverableError(
          e instanceof Error ? e.message : String(e),
        );
      }
      throw e;
    }
  };
};

function getJobShardingKey(
  projectId: string,
  data: IngestionJobData,
  legacyItem: ResolvedIngestionItem | null,
): string {
  if (data.items) {
    return `${projectId}-${data.traceId ?? "unknown"}`;
  }

  let entityType: ReturnType<typeof getClickhouseEntityType> | undefined;
  if (legacyItem) {
    entityType = getClickhouseEntityType(legacyItem.type);
  } else if (data.type) {
    entityType = getClickhouseEntityType(data.type);
  }

  if (entityType === "score" && data.traceId) {
    return `${projectId}-${data.traceId}`;
  }

  return `${projectId}-${data.eventBodyId ?? legacyItem?.eventBodyId ?? "unknown"}`;
}

function buildSeenKey(
  projectId: string,
  item: Pick<ResolvedIngestionItem, "type" | "eventBodyId">,
  fileKey: string,
): string {
  return `langfuse:ingestion:recently-processed:${projectId}:${item.type}:${item.eventBodyId}:${fileKey}`;
}

async function isItemSeen(
  projectId: string,
  item: ResolvedIngestionItem,
): Promise<boolean> {
  if (
    !item.fileKey ||
    env.LANGFUSE_ENABLE_REDIS_SEEN_EVENT_CACHE !== "true" ||
    !redis
  ) {
    return false;
  }

  const fileKey = item.fileKey;
  const exists = await redis.exists(buildSeenKey(projectId, item, fileKey));
  recordIncrement("langfuse.ingestion.recently_processed_cache", 1, {
    type: item.type,
    skipped: exists ? "true" : "false",
  });

  return !!exists;
}

async function markItemFilesSeen(
  projectId: string,
  item: ResolvedIngestionItem,
  eventFiles: { file: string }[],
): Promise<void> {
  if (env.LANGFUSE_ENABLE_REDIS_SEEN_EVENT_CACHE !== "true" || !redis) {
    return;
  }

  try {
    // We use Promise.all internally instead of a redis.pipeline since autoPipelining should handle it correctly
    // while being redis cluster aware.
    await Promise.all(
      eventFiles.map((eventFile) => {
        const fileKey = (eventFile.file.split("/").pop() ?? "").replace(
          ".json",
          "",
        );
        return redis!.set(
          buildSeenKey(projectId, item, fileKey),
          "1",
          "EX",
          300, // 5 minutes
        );
      }),
    );
  } catch (e) {
    logger.warn(
      "Failed to set recently-processed cache. Continuing processing.",
      e,
    );
  }
}
