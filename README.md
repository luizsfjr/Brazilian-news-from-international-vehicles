# Brazilian News Ingestion (Guardian API -> Dataproc -> GCS)

Este projeto executa ingestao de noticias do Guardian, trata paginacao e grava parquet no GCS usando PySpark em cluster Dataproc.

## 1. Estrutura

- Codigo principal: `src/ingestion.py`
- Segredo da API: Secret Manager
- Destino de dados: `gs://<bucket>/brazilian_news/parquet`

## 2. Pre-requisitos

- Projeto GCP ativo
- APIs habilitadas:
  - Dataproc API
  - Secret Manager API
  - Cloud Storage API
- `gcloud` autenticado no Cloud Shell

## 3. Variaveis de ambiente (Cloud Shell)

```bash
export PROJECT_ID="lc-qas-lake-house-0707"
export REGION="us-central1"
export CLUSTER="cluster-news"
export BUCKET="gcp-lc-datalakehouse-raw"
export SECRET_ID="API_KEY_THE_GUARDIAN"
gcloud config set project $PROJECT_ID
```

## 4. Criar segredo da API no Secret Manager

```bash
echo -n "SUA_API_KEY_GUARDIAN" | gcloud secrets create $SECRET_ID --data-file=-
```

Se o segredo ja existir:

```bash
echo -n "SUA_API_KEY_GUARDIAN" | gcloud secrets versions add $SECRET_ID --data-file=-
```

## 5. Permissoes IAM minimas para o Dataproc

```bash
PROJECT_NUMBER=$(gcloud projects describe $PROJECT_ID --format='value(projectNumber)')
SA="${PROJECT_NUMBER}-compute@developer.gserviceaccount.com"
```

Permitir leitura do segredo:

```bash
gcloud secrets add-iam-policy-binding $SECRET_ID \
  --member="serviceAccount:${SA}" \
  --role="roles/secretmanager.secretAccessor"
```

Permitir escrita/leitura no bucket:

```bash
gcloud storage buckets add-iam-policy-binding gs://$BUCKET \
  --member="serviceAccount:${SA}" \
  --role="roles/storage.objectAdmin"
```

Permitir worker Dataproc:

```bash
gcloud projects add-iam-policy-binding $PROJECT_ID \
  --member="serviceAccount:${SA}" \
  --role="roles/dataproc.worker"
```

## 6. Rede (se cluster private/internal IP only)

Se aparecer `Network is unreachable` ao instalar pacotes pip no startup, crie Cloud NAT:

```bash
gcloud compute routers create dp-router \
  --network=default \
  --region=$REGION

gcloud compute routers nats create dp-nat \
  --router=dp-router \
  --region=$REGION \
  --nat-all-subnet-ip-ranges \
  --auto-allocate-nat-external-ips
```

## 7. Criar cluster Dataproc (versao enxuta)

```bash
gcloud dataproc clusters create $CLUSTER \
  --region=$REGION \
  --image-version=2.2 \
  --single-node \
  --master-machine-type=e2-standard-2 \
  --master-boot-disk-size=50GB \
  --properties='^#^dataproc:pip.packages=google-cloud-secret-manager==2.25.0,google-auth==2.38.0,python-dotenv==1.0.1,requests==2.32.3'
```

Se o nome ja existir:

```bash
gcloud dataproc clusters delete $CLUSTER --region=$REGION -q
```

## 8. Enviar script para GCS

```bash
git clone your repo
```

```bash
gcloud storage cp src/ingestion.py gs://$BUCKET/jobs/ingestion.py
```

## 9. Submeter job PySpark

```bash
gcloud dataproc jobs submit pyspark gs://$BUCKET/jobs/ingestion.py \
  --cluster=$CLUSTER \
  --region=$REGION \
  --properties="spark.yarn.appMasterEnv.GOOGLE_CLOUD_PROJECT=$PROJECT_ID,spark.executorEnv.GOOGLE_CLOUD_PROJECT=$PROJECT_ID"
```

## 10. Validar execucao

Listar jobs:

```bash
gcloud dataproc jobs list --region=$REGION
```

Ver detalhes:

```bash
gcloud dataproc jobs wait <JOB_ID> --region=$REGION --project=$PROJECT_ID
```

Conferir arquivos de saida:

```bash
gcloud storage ls gs://$BUCKET/brazilian_news/parquet
```

## 11. Troubleshooting rapido

- Erro `Network is unreachable` no `pip install`
  - Causa: cluster sem saida internet.
  - Correcao: criar Cloud NAT.

- Erro `DISKS_TOTAL_GB quota`
  - Causa: cluster grande demais para cota.
  - Correcao: reduzir disco/maquina/workers (ex.: `--single-node`, `50GB`).

- Erro `ALREADY_EXISTS`
  - Causa: nome de cluster ja usado.
  - Correcao: deletar cluster antigo ou usar outro nome.

## 12. Encerrar recursos

```bash
gcloud dataproc clusters delete $CLUSTER --region=$REGION -q
```

