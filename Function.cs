using System.Text.Json;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using Amazon.Lambda.Core;
using Amazon.Lambda.SQSEvents;
using Amazon.S3;
using ClientOnboardingLambda.Models;
using ClientOnboardingLambda.Services;
using DocumentIngestLambda.Services;

[assembly: LambdaSerializer(typeof(Amazon.Lambda.Serialization.SystemTextJson.DefaultLambdaJsonSerializer))]

namespace DocumentIngestLambda;

/// <summary>
/// SQS-triggered worker: parses S3 event notifications, re-indexes the affected tenant via
/// <see cref="DocumentIngestService"/>, and writes INGEST#latest status rows for the dashboard.
/// </summary>
public class Function
{
    private static readonly JsonSerializerOptions JsonOptions = new() { PropertyNameCaseInsensitive = true };

    public async Task FunctionHandler(SQSEvent sqsEvent, ILambdaContext context)
    {
        var config = AppConfig.Load();
        var ingestService = CreateIngestService(config);
        var statusWriter = new IngestStatusWriter(new DynamoDbRepository(new AmazonDynamoDBClient(), config.DynamoDbTableName));

        var tenantIds = sqsEvent.Records
            .SelectMany(ParseTenantIdsFromRecord)
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .ToList();

        if (tenantIds.Count == 0)
        {
            context.Logger.LogWarning("No tenant ids parsed from SQS batch.");
            return;
        }

        foreach (var tenantId in tenantIds)
        {
            var tenant = TenantRegistry.Find(tenantId);
            if (tenant is null)
            {
                context.Logger.LogWarning($"Unknown tenant id in S3 key: {tenantId}");
                continue;
            }

            var startedAt = DateTime.UtcNow;
            await statusWriter.WriteLatestAsync(tenant, "running", startedAt, triggerKey: tenantId);

            try
            {
                context.Logger.LogInformation($"Starting ingest for {tenant.TenantId}");
                var summary = await ingestService.IngestTenantAsync(tenant, CancellationToken.None);
                await statusWriter.WriteLatestAsync(
                    tenant,
                    "completed",
                    startedAt,
                    completedAt: DateTime.UtcNow,
                    pdfCount: summary.PdfCount,
                    chunkCount: summary.ChildChunkCount,
                    triggerKey: tenantId);

                context.Logger.LogInformation(summary.Message);
            }
            catch (Exception ex)
            {
                context.Logger.LogError($"Ingest failed for {tenant.TenantId}: {ex}");
                await statusWriter.WriteLatestAsync(
                    tenant,
                    "failed",
                    startedAt,
                    completedAt: DateTime.UtcNow,
                    error: ex.Message,
                    triggerKey: tenantId);
                throw;
            }
        }
    }

    private static DocumentIngestService CreateIngestService(AppConfig config)
    {
        var openAi = new OpenAiService(config.OpenAiApiKey);
        var dynamoDb = new DynamoDbRepository(new AmazonDynamoDBClient(), config.DynamoDbTableName);
        var qdrant = new QdrantClient(config.QdrantUrl, config.QdrantApiKey);
        return new DocumentIngestService(config, new AmazonS3Client(), dynamoDb, qdrant, openAi);
    }

    private static IEnumerable<string> ParseTenantIdsFromRecord(SQSEvent.SQSMessage record)
    {
        S3EventNotification? payload;
        try
        {
            payload = JsonSerializer.Deserialize<S3EventNotification>(record.Body, JsonOptions);
        }
        catch (JsonException)
        {
            yield break;
        }

        if (payload?.Records is null)
        {
            yield break;
        }

        foreach (var s3Record in payload.Records)
        {
            var key = s3Record.S3?.Object?.Key;
            if (string.IsNullOrWhiteSpace(key))
            {
                continue;
            }

            key = Uri.UnescapeDataString(key.Replace('+', ' '));

            var tenantId = key.Split('/', StringSplitOptions.RemoveEmptyEntries).FirstOrDefault();
            if (!string.IsNullOrWhiteSpace(tenantId) &&
                tenantId.StartsWith("tenant_", StringComparison.OrdinalIgnoreCase))
            {
                yield return tenantId;
            }
        }
    }

    private sealed class S3EventNotification
    {
        public List<S3EventRecord>? Records { get; set; }
    }

    private sealed class S3EventRecord
    {
        public S3Entity? S3 { get; set; }
    }

    private sealed class S3Entity
    {
        public S3Object? Object { get; set; }
    }

    private sealed class S3Object
    {
        public string? Key { get; set; }
    }
}
