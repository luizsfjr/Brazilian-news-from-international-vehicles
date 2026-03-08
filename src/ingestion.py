import uuid
import requests
from requests.adapters import HTTPAdapter
from datetime import date, datetime, timedelta, timezone
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pathlib import Path
from dotenv import load_dotenv
from typing import Any
import os
from urllib3.util.retry import Retry
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, BooleanType
from pyspark.sql.utils import AnalysisException


class Ingestion:
    """Ingestion pipeline for Guardian API extraction and raw storage."""

    def __init__(
                self,
                search: str,
                secret_id: str = "API_KEY_THE_GUARDIAN",
                secret_version: str = "latest"
    ):
        """Initialize ingestion settings and the HTTP client.

        Args:
            search: Search term used in Guardian API queries.
            secret_id: Secret Manager secret id for the API key.
            secret_version: Secret Manager secret version to read.

        Returns:
            None.

        Raises:
            EnvironmentError: If the API key cannot be resolved.
        """
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
        """Resolve the Guardian API key from env vars or Secret Manager.

        Args:
            secret_id: Secret id that stores the API key.
            secret_version: Secret version to access.

        Returns:
            str: Resolved API key.

        Raises:
            EnvironmentError: If project configuration is missing, secret access
                fails, or the secret is empty.
        """
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
                "Set PROJECT_ID to read API key from Secret Manager."
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
                timeout_seconds: int = 30,
                overlap_days: int = 1,
    ) -> dict[str, Any]:
        """Fetch paginated Guardian Search API results and merge all pages.

        Args:
            from_date: Initial date in ISO format (YYYY-MM-DD). If None, a date
                is derived from `to_date - overlap_days`.
            to_date: Final date in ISO format (YYYY-MM-DD).
            page_size: Number of records per API page.
            timeout_seconds: Timeout for each HTTP request.
            overlap_days: Days to look back when `from_date` is not provided.

        Returns:
            dict[str, Any]: API payload with merged records under
                `response.results`.

        Raises:
            requests.HTTPError: If any API request returns an HTTP error.
            ValueError: If `to_date` is not a valid ISO date.
        """
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
        print(f"=========Extracting page 1=========")
        first_payload = response.json()
        first_response = first_payload.get("response", {})

        # if status != ok then stop the requests returning the results
        if first_response.get("status") != "ok":
            return first_payload

        total_pages = int(first_response.get("pages", 1))

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
        merged_response["fetchedPages"] = total_pages
        merged_response["pageSize"] = page_size

        return {"response": merged_response}

    def to_raw_data(
                self,
                spark: SparkSession, 
                result: dict[str, Any]
    ) -> DataFrame:
        """Normalize API records into a flat raw structure.

        Args:
            result: Full API response dictionary returned by `get_data`.
            spark: Active Spark Session

        Returns:
            Spark Dataframe
        """
        response_block = result.get("response", {})
        results = response_block.get("results", [])

        run_id = str(uuid.uuid4())
        rows = [
            {
                "id": item.get("id"),
                "type": item.get("type"),
                "section_id": item.get("sectionId"),
                "section_name": item.get("sectionName"),
                "published_at": item.get("webPublicationDate"),
                "title": item.get("webTitle"),
                "web_url": item.get("webUrl"),
                "api_url": item.get("apiUrl"),
                "is_hosted": item.get("isHosted"),
                "pillar_id": item.get("pillarId"),
                "pillar_name": item.get("pillarName"),
                # Ingestion metadata
                "_ingested_at_utc":   datetime.now(timezone.utc).isoformat(),
                "_run_id":            run_id,
                "_search_term":       self.search,
            }
            for item in results
        ]

        return spark.createDataFrame(rows)

    def save_dataframe(
                self,
                df_news: DataFrame,
                output_base: str = "gs://gcp-lc-datalakehouse-raw/brazilian_news"
    ) -> str:
        """Write data to a date-stamped Parquet path in GCS.

        Args:
            df_news: Spark DataFrame containing normalized news records.
            output_base: Base output URI (local path or `gs://` URI).

        Returns:
            str: Final parquet destination path used in this execution.
        """

        base = output_base.rstrip("/")
        ingestion_date = date.today().isoformat()
        parquet_path = f"{base}/parquet-{ingestion_date}"

        df_news.write.mode("append").parquet(parquet_path)

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
    # Fetches d-1 by default.
    # Pass from_date explicitly to override: ingest1.get_data(from_date="2026-01-01")
    result = ingest1.get_data(from_date="2026-01-01")
    status = result.get("response", {}).get("status", "unknown")
    try:
        if status == "ok":
            df_news = ingest1.to_raw_data(spark,result)
            df_news.printSchema()
            df_news.show(truncate=False)
            parquet_path = ingest1.save_dataframe(df_news)
            print(f"Saved parquet to: {parquet_path}")
        else:
            print("****Fail in get data****")
    finally:
        spark.stop()
