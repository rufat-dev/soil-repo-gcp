# Soil Report Function (.NET + BigQuery)

Minimal HTTP service for Cloud Run / Cloud Functions 2nd gen (Cloud Run runtime) that:

- Responds to `GET /` with `200 OK` for health checks.
- Responds to `GET /users` by querying BigQuery table `soil-report-486813.crm.users`.
- Supports safe pagination via `limit` and `offset`.

The app uses Application Default Credentials (ADC), so in Cloud Run it authenticates with the runtime service account automatically (no keyfiles in code).

## Endpoints

- `GET /` -> `200 OK` with body `"OK"`
- `GET /users` -> JSON array with fields:
  - `user_id`
  - `email`
  - `phone_number`
  - `full_name`
  - `role`
  - `created_at` (ISO 8601 when available)
  - `updated_at` (ISO 8601 when available)

### Query params for `/users`

- `limit`: default `200`, min `1`, max `1000`
- `offset`: default `0`, min `0`, max `100000`

Rows are ordered by `created_at DESC NULLS LAST`.

## Configuration (Environment Variables)

- `BQ_PROJECT_ID` (default: `soil-report-486813`)
- `BQ_DATASET` (default: `crm`)
- `BQ_TABLE` (default: `users`)
- `ALLOWED_ORIGINS` (optional, comma-separated)
  - Empty/unset: no CORS policy enabled (no cross-origin access)
  - `*`: allow any origin (not recommended for sensitive/public prod APIs)
  - Example specific origins:
    - `https://app.example.com,https://admin.example.com`

## Local Run (dotnet)

Prerequisites:

- .NET 8 SDK
- Access to GCP via ADC (e.g. `gcloud auth application-default login`) if testing BigQuery locally
- Billing enabled on your GCP project

Run:

```bash
dotnet restore src/SoilReportFn/SoilReportFn.csproj
dotnet run --project src/SoilReportFn/SoilReportFn.csproj
```

Test:

```bash
curl -i http://localhost:8080/
curl -s "http://localhost:8080/users?limit=50&offset=0" | jq
```

## Local Run (Docker)

Build and run:

```bash
docker build -t soil-report-fn:local -f Dockerfile .
docker run --rm -p 8080:8080 \
  -e BQ_PROJECT_ID=soil-report-486813 \
  -e BQ_DATASET=crm \
  -e BQ_TABLE=users \
  soil-report-fn:local
```

Then:

```bash
curl -i http://localhost:8080/
curl -s "http://localhost:8080/users?limit=25&offset=0"
```

## Required IAM Roles (runtime service account)

Grant these roles to the Cloud Run service account used by this service:

- `roles/bigquery.jobUser`
- `roles/bigquery.dataViewer` (or dataset-level equivalent on dataset `crm`)

## Deploy from GitHub (Cloud Run continuous deploy)

1. Push this repository to GitHub (`main` branch).
2. In Google Cloud Console, open **Cloud Run**.
3. Click **Create Service** (or equivalent flow for source-based deploy).
4. Choose **Continuously deploy from repository**.
5. Connect/select your GitHub repo.
6. Set:
   - **Branch**: `main`
   - **Build type**: `Dockerfile`
   - **Dockerfile path**: `/Dockerfile`
7. Set runtime environment variables:
   - `BQ_PROJECT_ID=soil-report-486813`
   - `BQ_DATASET=crm`
   - `BQ_TABLE=users`
   - Optional: `ALLOWED_ORIGINS=...`
8. Select the runtime service account that has BigQuery permissions above.
9. Deploy.

Cloud Run expects traffic on port `8080`; this repo is configured accordingly.

## Example Calls After Deploy

Assume:

```bash
SERVICE_URL="https://YOUR_CLOUD_RUN_URL"
```

Health:

```bash
curl -i "$SERVICE_URL/"
```

Users:

```bash
curl -s "$SERVICE_URL/users"
curl -s "$SERVICE_URL/users?limit=100&offset=0"
```

## Notes

- BigQuery usage requires project billing to be enabled.
- Errors from BigQuery are logged server-side with details; API clients receive safe 500 responses.
