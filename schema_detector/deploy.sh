#!/bin/bash

PROJECT_ID="lc-qas-lake-house-0707"
REGION="us-central1"
FUNCTION_NAME="schema-change-detector"
PUBSUB_TOPIC="schema-detector-trigger"
SERVICE_ACCOUNT="schema-detector-sa"
SA_EMAIL="$SERVICE_ACCOUNT@$PROJECT_ID.iam.gserviceaccount.com"

echo "🚀 Starting deployment..."

# ---- 1. Create Service Account ----
echo "Creating service account..."
gcloud iam service-accounts create $SERVICE_ACCOUNT \
  --display-name="Schema Detector SA" \
  --project=$PROJECT_ID

# ---- 2. Grant BigQuery permissions ----
echo "Granting BigQuery roles..."
gcloud projects add-iam-policy-binding $PROJECT_ID \
  --member="serviceAccount:$SA_EMAIL" \
  --role="roles/bigquery.dataEditor"

gcloud projects add-iam-policy-binding $PROJECT_ID \
  --member="serviceAccount:$SA_EMAIL" \
  --role="roles/bigquery.jobUser"

# ---- 3. Create Pub/Sub topic ----
echo "Creating Pub/Sub topic..."
gcloud pubsub topics create $PUBSUB_TOPIC --project=$PROJECT_ID

# ---- 4. Deploy Cloud Function ----
echo "Deploying Cloud Function..."
gcloud functions deploy $FUNCTION_NAME \
  --gen2 \
  --runtime=python311 \
  --region=$REGION \
  --source=. \
  --entry-point=detect_schema_changes \
  --trigger-topic=$PUBSUB_TOPIC \
  --service-account=$SA_EMAIL \
  --memory=256MB \
  --timeout=120s \
  --project=$PROJECT_ID

# ---- 5. Create Cloud Scheduler job (runs daily at 6am UTC) ----
echo "Creating Cloud Scheduler job..."
gcloud scheduler jobs create pubsub schema-detector-job \
  --location=$REGION \
  --schedule="0 6 * * *" \
  --topic=$PUBSUB_TOPIC \
  --message-body='{"trigger":"scheduled"}' \
  --time-zone="UTC" \
  --project=$PROJECT_ID

echo "✅ Deployment complete!"
echo "📅 Scheduler: runs daily at 06:00 UTC"
echo "🔁 To trigger manually:"
echo "   gcloud scheduler jobs run schema-detector-job --location=$REGION"