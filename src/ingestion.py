import requests
from requests.adapters import HTTPAdapter
from datetime import date, datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pathlib import Path
from dotenv import load_dotenv
from typing import Any
import os
from urllib3.util.retry import Retry
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, BooleanType

class Ingestion:

    def __init__(
                self,
                search: str,
                secret_id: str = "API_KEY_THE_GUARDIAN",
                secret_version: str = "latest"
    ):
        load_dotenv()
        self.search = search
        self.api_key = self._resolve_api_key(secret_id, secret_version)

        # Check API KEY existence
        if not self.api_key:
            raise EnvironmentError("API_KEY is not configured")

        # Retry logic with backoff for transient network errors
        self.session = requests.Session()
        retries = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
        )
        self.session.mount("https://", HTTPAdapter(max_retries=retries))

    def _resolve_api_key(self, secret_id: str, secret_version: str) -> str:
        # Local/dev fallback
        env_key = os.getenv("API_KEY_THE_GUARDIAN")
        if env_key:
            return env_key

        project_id = os.getenv("PROJECT_ID")
        secret_id = os.getenv("API_KEY_THE_GUARDIAN", secret_id)

        if not project_id:
            try:
                import google.auth
                _, detected_project = google.auth.default()
                project_id = detected_project
            except Exception:
                project_id = None

        if not project_id:
            raise EnvironmentError(
                "Set GOOGLE_CLOUD_PROJECT (or GCP_PROJECT) to read API key from Secret Manager."
            )

        try:
            from google.cloud import secretmanager

            client = secretmanager.SecretManagerServiceClient()
            name = (
                f"projects/{project_id}/secrets/{secret_id}/versions/{secret_version}"
            )
            response = client.access_secret_version(name=name)
            api_key = response.payload.data.decode("utf-8").strip()
            if not api_key:
                raise EnvironmentError(f"Secret {secret_id} is empty.")
            return api_key
        except Exception as exc:
            raise EnvironmentError(
                "Could not load API key from Secret Manager. "
                "Check secret name, permissions, and google-cloud-secret-manager dependency."
            ) from exc
    
    def get_data(
                self,
                from_date: str | None = None,
                to_date: str = str(date.today().isoformat()), 
                page_size: int = 50,
                max_pages: int | None = None,
                timeout_seconds: int = 30,
                overlap_days: int = 30,
    ) -> dict[str, Any]:
        # If no from_date is provided, default to `overlap_days` before to_date
        if from_date is None:
            from_date = (
                date.fromisoformat(to_date) - timedelta(days=overlap_days)
            ).isoformat()

        url = "https://content.guardianapis.com/search"
        params = {
            "from-date": from_date,
            "to-date": to_date,
            "q": self.search,
            "api-key": self.api_key,
            "page-size": page_size,
            "page": 1,
        }

        response = self.session.get(url, params=params, timeout=timeout_seconds)
        response.raise_for_status()
        first_payload = response.json()
        first_response = first_payload.get("response", {})

        # if status != ok then stop the requests returning the results
        if first_response.get("status") != "ok":
            return first_payload

        total_pages = int(first_response.get("pages", 1))
        if max_pages is not None:
            total_pages = min(total_pages, max_pages)

        all_results = list(first_response.get("results", []))

        for page in range(2, total_pages + 1):
            print(f"=========Extracting page {page}=========")
            params["page"] = page
            page_response = self.session.get(url, params=params, timeout=timeout_seconds)
            page_response.raise_for_status()
            page_payload = page_response.json()
            page_data = page_payload.get("response", {})
            all_results.extend(page_data.get("results", []))

        merged_response = dict(first_response)
        merged_response["results"] = all_results
        merged_response["currentPage"] = total_pages
        merged_response["fetchedPages"] = total_pages
        merged_response["pageSize"] = page_size

        return {"response": merged_response}

    def to_spark_dataframe(
                self, 
                spark: SparkSession, 
                result: dict[str, Any]
    ) -> DataFrame:
        response_block = result.get("response", {})
        results = response_block.get("results", [])

        def parse_guardian_datetime(value: str | None) -> datetime | None:
            if not value:
                return None
            # Guardian format: 2026-02-11T10:00:13Z
            return datetime.fromisoformat(value.replace("Z", "+00:00"))

        rows = [
            {
                "id": item.get("id"),
                "type": item.get("type"),
                "section_id": item.get("sectionId"),
                "section_name": item.get("sectionName"),
                "published_at": parse_guardian_datetime(item.get("webPublicationDate")),
                "title": item.get("webTitle"),
                "web_url": item.get("webUrl"),
                "api_url": item.get("apiUrl"),
                "is_hosted": item.get("isHosted"),
                "pillar_id": item.get("pillarId"),
                "pillar_name": item.get("pillarName"),
            }
            for item in results
        ]

        schema = StructType([
                StructField('id', StringType(), False),
                StructField('type', StringType(), True),
                StructField('section_id', StringType(), True),
                StructField('section_name', StringType(), True),
                StructField('published_at', TimestampType(), True),
                StructField('title', StringType(), True),
                StructField('web_url', StringType(), True),
                StructField('api_url', StringType(), True),
                StructField('is_hosted',  BooleanType(), True),
                StructField('pillar_id', StringType(), True),
                StructField('pillar_name', StringType(), True),
            ])

        return spark.createDataFrame(rows, schema=schema)

    def save_dataframe(
                self,
                spark: SparkSession,
                df_news: DataFrame,
                output_base: str = "gs://gcp-lakehouse-raw/brazilian_news"
    ) -> str:
        base_path = Path(output_base)
        parquet_path = str(base_path / "parquet")

        if Path(parquet_path).exists():
            df_existing = spark.read.parquet(parquet_path)
            df_merged = df_existing.union(df_news).dropDuplicates(["id"])
        else:
            # First run — nothing on disk yet
            df_merged = df_news.dropDuplicates(["id"])

        df_merged.write.mode("overwrite").parquet(parquet_path)
        return parquet_path

if __name__ == "__main__":

    spark = (
        SparkSession.builder
        .appName("brazillian-news-etl")
        .master("local[*]")
        .config("spark.local.dir", "C:\\spark-temp")
        .getOrCreate()
    )
    
    ingest1 = Ingestion("Brazil")
    # Fetches the last 30 days by default.
    # Pass from_date explicitly to override: ingest1.get_data(from_date="2026-01-01")
    result = ingest1.get_data()
    status = result.get("response", {}).get("status", "unknown")
    try:
        if status == "ok":
            df_news = ingest1.to_spark_dataframe(spark, result)
            df_news.printSchema()
            df_news.show(truncate=False)
            parquet_path = ingest1.save_dataframe(spark, df_news)
            print(f"Saved parquet to: {parquet_path}")
        else:
            print("****Fail in get data****")
    finally:
        spark.stop()
