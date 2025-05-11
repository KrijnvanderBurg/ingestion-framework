"""
ingestion_framework

This module contains the main entry point for the ingestion framework CLI.
This CLI tool processes a configuration file to create and execute data ingestion jobs.
Parses command-line arguments and executes a job based on the provided configuration file.

Example:
    To use this module from command line:
        $ python -m ingestion_framework --filepath /path/to/config.json

    To import and use programmatically:
        >>> from ingestion_framework.__main__ import main
        >>> main()  # Will require command line arguments
"""

import logging
from argparse import ArgumentParser

from ingestion_framework.job import Job
from ingestion_framework.utils.log_handler import set_logger

logger: logging.Logger = set_logger(__name__)


def main() -> None:
    """
    Entry point for the ingestion framework CLI.

    Parses command-line arguments, expecting a filepath to a configuration file.
    Creates and executes a Job instance from the provided configuration file.

    Raises:
        SystemExit: If required arguments are missing or invalid
        FileNotFoundError: If the specified configuration file does not exist
        ValueError: If the configuration file has invalid format
        Exception: For any other unexpected errors during job execution

    Example:
        Command line usage:
        ```
        $ python -m ingestion_framework --filepath /path/to/config.json
        ```
    """
    logger.info("Starting something...")

    parser = ArgumentParser(description="config driven etl.")
    parser.add_argument("--filepath", required=True, type=str, help="confeti filepath")
    args = parser.parse_args()
    logger.info("args: %s", args)

    filepath: str = args.filepath

    if filepath != "":
        job = Job.from_file(filepath=filepath)
        job.execute()

    logger.info("Exiting.")


if __name__ == "__main__":
    main()
