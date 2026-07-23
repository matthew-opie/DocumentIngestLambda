using Amazon.DynamoDBv2.Model;
using ClientOnboardingLambda.Models;
using ClientOnboardingLambda.Services;

namespace DocumentIngestLambda.Services;

public sealed class IngestStatusWriter(DynamoDbRepository dynamoDb)
{
    public Task WriteLatestAsync(
        TenantInfo tenant,
        string status,
        DateTime startedAt,
        DateTime? completedAt = null,
        int? pdfCount = null,
        int? chunkCount = null,
        string? error = null,
        string? triggerKey = null,
        CancellationToken cancellationToken = default)
    {
        var attributes = new Dictionary<string, AttributeValue>
        {
            ["status"] = new (status),
            ["startedAt"] = new (startedAt.ToString("O")),
            ["tenantId"] = new (tenant.TenantId)
        };

        if (completedAt.HasValue)
        {
            attributes["completedAt"] = new AttributeValue(completedAt.Value.ToString("O"));
        }

        if (pdfCount.HasValue)
        {
            attributes["pdfCount"] = new AttributeValue { N = pdfCount.Value.ToString() };
        }

        if (chunkCount.HasValue)
        {
            attributes["chunkCount"] = new AttributeValue { N = chunkCount.Value.ToString() };
        }

        if (!string.IsNullOrWhiteSpace(error))
        {
            attributes["error"] = new (error);
        }

        if (!string.IsNullOrWhiteSpace(triggerKey))
        {
            attributes["triggerKey"] = new (triggerKey);
        }

        return dynamoDb.PutItemAsync(tenant.PartitionKey, "INGEST#latest", attributes, cancellationToken);
    }
}
