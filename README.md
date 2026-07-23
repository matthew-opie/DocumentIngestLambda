# Document Ingest Lambda

Async worker that re-indexes tenant PDFs when new files land in S3. Part of the [Multi-Tenant RAG Onboarding Platform](https://github.com/matthew-opie/ClientOnboardingLambda).

## Architecture

```
S3 (tenant_*.pdf) → Event notification → SQS → DocumentIngestLambda
                                                      │
                                                      ├─ DocumentIngestService (shared library)
                                                      ├─ DynamoDB (chunks + INGEST#latest)
                                                      └─ Qdrant (child embeddings)
```

| Setting | Value |
|---------|-------|
| Trigger | SQS (`onboarding-ingest-queue`) |
| Runtime | .NET 10 (`dotnet10`) |
| Architecture | arm64 |
| Memory | 2048 MB |
| Timeout | 900 s |

## Prerequisites

This project references [`ClientOnboardingLambda`](https://github.com/matthew-opie/ClientOnboardingLambda) for shared ingest logic (`DocumentIngestService`, chunking, Qdrant, DynamoDB). Clone both repos side by side:

```
lambdas/
├── ClientOnboardingLambda/
└── document-ingest-lambda/
```

## Environment variables

Same as the query Lambda (set on **both** functions):

| Variable | Required | Description |
|----------|----------|-------------|
| `OPENAI_API_KEY` | Yes | Embeddings (`text-embedding-3-small`) |
| `QDRANT_URL` | Yes | Qdrant REST base URL |
| `QDRANT_API_KEY` | Yes | Qdrant API key |
| `DYNAMODB_TABLE_NAME` | No | Default `OnboardingPlatform` |
| `SEED_BUCKET_NAME` | Yes | S3 bucket with `tenant_001/` … PDFs |

## Deploy

```powershell
cd document-ingest-lambda
dotnet lambda deploy-function DocumentIngestLambda
```

Wire SQS as the event source and ensure S3 publishes `ObjectCreated` events for keys matching `tenant_*.pdf`.

## Ingest status

Each run writes `INGEST#latest` on the tenant partition (`running` → `completed` | `failed`). The query Lambda exposes this at `GET /tenants/{tenantId}/ingest-status`.

## Handler flow

1. Parse S3 event payload from each SQS record and extract `tenant_XXX` from the object key.
2. Mark ingest status `running`.
3. Call `DocumentIngestService.IngestTenantAsync` (delete old vectors/chunks, re-chunk PDFs, embed, upsert).
4. Mark status `completed` with PDF/chunk counts, or `failed` with error message.

## Local build

```powershell
dotnet build
```

Requires `ClientOnboardingLambda` at `../ClientOnboardingLambda/`.
