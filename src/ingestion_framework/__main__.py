"""
TODO
"""

import logging
from argparse import ArgumentParser

from ingestion_framework.job import Job
from ingestion_framework.utils.log_handler import set_logger

logger: logging.Logger = set_logger(__name__)


def main() -> None:
    """TODO"""
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
