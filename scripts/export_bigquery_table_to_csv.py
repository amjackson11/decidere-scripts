#!/usr/bin/env python3
"""
Export a BigQuery table to a local CSV file via Google Cloud Storage.

BigQuery exports table data to Cloud Storage first. This script performs that
export, downloads the resulting CSV locally, and optionally removes the temporary
GCS object afterward.

Authentication:
    This script uses Google Application Default Credentials.

    Run:
        gcloud auth application-default login

Example:
    python scripts/export_bigquery_table_to_csv.py \\
        --project my-gcp-project \\
        --dataset analytics \\
        --table daily_metrics \\
        --bucket my-export-bucket \\
        --gcs-object exports/daily_metrics.csv \\
        --output daily_metrics.csv \\
        --delete-gcs-object
"""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

from google.cloud import bigquery
from google.cloud import storage
from google.cloud.exceptions import GoogleCloudError


LOGGER = logging.getLogger(__name__)


def export_bigquery_table_to_csv(
    *,
    project_id: str,
    dataset_id: str,
    table_id: str,
    bucket_name: str,
    gcs_object_name: str,
    output_path: Path,
    location: str = "US",
    delete_gcs_object: bool = False,
) -> None:
    """
    Export a BigQuery table to a local CSV file via Google Cloud Storage.

    Args:
        project_id:
            Google Cloud project ID.
        dataset_id:
            BigQuery dataset ID.
        table_id:
            BigQuery table ID.
        bucket_name:
            Destination Google Cloud Storage bucket.
        gcs_object_name:
            Object path/name to create in the GCS bucket.
        output_path:
            Local path where the CSV should be downloaded.
        location:
            BigQuery job location. Defaults to "US".
        delete_gcs_object:
            Whether to delete the intermediate GCS object after download.

    Raises:
        GoogleCloudError:
            If a BigQuery or Cloud Storage operation fails.
        OSError:
            If the local output path cannot be written.
    """
    output_path = output_path.expanduser().resolve()
    output_path.parent.mkdir(parents=True, exist_ok=True)

    bq_client = bigquery.Client(project=project_id)
    storage_client = storage.Client(project=project_id)

    table_ref = f"{project_id}.{dataset_id}.{table_id}"
    destination_uri = f"gs://{bucket_name}/{gcs_object_name}"

    LOGGER.info("Exporting BigQuery table %s to %s", table_ref, destination_uri)

    extract_job_config = bigquery.ExtractJobConfig(
        destination_format=bigquery.DestinationFormat.CSV,
        print_header=True,
    )

    extract_job = bq_client.extract_table(
        source=table_ref,
        destination_uris=destination_uri,
        job_config=extract_job_config,
        location=location,
    )
    extract_job.result()

    LOGGER.info("Downloading %s to %s", destination_uri, output_path)

    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(gcs_object_name)
    blob.download_to_filename(str(output_path))

    if delete_gcs_object:
        LOGGER.info("Deleting intermediate GCS object %s", destination_uri)
        blob.delete()

    LOGGER.info("Export complete: %s", output_path)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Export a BigQuery table to a local CSV file via Google Cloud Storage."
        )
    )

    parser.add_argument(
        "--project",
        required=True,
        help="Google Cloud project ID.",
    )
    parser.add_argument(
        "--dataset",
        required=True,
        help="BigQuery dataset ID.",
    )
    parser.add_argument(
        "--table",
        required=True,
        help="BigQuery table ID.",
    )
    parser.add_argument(
        "--bucket",
        required=True,
        help="Google Cloud Storage bucket used for the intermediate export.",
    )
    parser.add_argument(
        "--gcs-object",
        required=True,
        help="GCS object name for the exported CSV, e.g. exports/table.csv.",
    )
    parser.add_argument(
        "--output",
        required=True,
        type=Path,
        help="Local output CSV path.",
    )
    parser.add_argument(
        "--location",
        default="US",
        help='BigQuery job location. Defaults to "US".',
    )
    parser.add_argument(
        "--delete-gcs-object",
        action="store_true",
        help="Delete the intermediate GCS object after downloading it locally.",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable verbose logging.",
    )

    return parser.parse_args()


def configure_logging(verbose: bool) -> None:
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s %(levelname)-8s %(message)s",
    )


def main() -> int:
    args = parse_args()
    configure_logging(args.verbose)

    try:
        export_bigquery_table_to_csv(
            project_id=args.project,
            dataset_id=args.dataset,
            table_id=args.table,
            bucket_name=args.bucket,
            gcs_object_name=args.gcs_object,
            output_path=args.output,
            location=args.location,
            delete_gcs_object=args.delete_gcs_object,
        )
    except GoogleCloudError as exc:
        LOGGER.error("Google Cloud operation failed: %s", exc)
        return 1
    except OSError as exc:
        LOGGER.error("Local file operation failed: %s", exc)
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())