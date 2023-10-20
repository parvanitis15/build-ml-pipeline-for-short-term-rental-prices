#!/usr/bin/env python
"""
Download from W&B the raw dataset and apply some basic data cleaning, exporting the result to a new artifact
"""
import argparse
import logging

import pandas as pd
import wandb


logging.basicConfig(level=logging.INFO, format="%(asctime)-15s %(message)s")
logger = logging.getLogger()


def go(args):

    run = wandb.init(job_type="basic_cleaning")
    run.config.update(args)

    # 1. Download artifact
    logger.info("Downloading artifact")
    artifact_local_path = run.use_artifact(args.input_artifact).file()
    logger.info("Downloaded artifact")

    # 3. Clean data
    logger.info("Cleaning data")
    df = pd.read_csv(artifact_local_path)
    df = df.dropna()
    df = df[df.price.between(args.min_price, args.max_price)]
    logger.info("Cleaned data")

    # 4. Upload artifact
    logger.info("Uploading artifact")
    artifact = wandb.Artifact(
        args.output_artifact,
        type=args.output_type,
        description=args.output_description,
    )
    logger.info("Uploaded artifact")

    # 5. Log output artifact
    logger.info("Logging artifact")
    artifact.add_file(artifact_local_path)
    run.log_artifact(artifact)
    logger.info("Logged artifact")

    # 6. Finish run
    logger.info("Finishing run")
    run.finish()
    logger.info("Finished run")


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="A very basic data cleaning")


    parser.add_argument(
        "--input_artifact", 
        type=str,
        help="The input artifact containing the raw data",
        required=True
    )

    parser.add_argument(
        "--output_artifact", 
        type=str,
        help="The output artifact containing the cleaned data",
        required=True
    )

    parser.add_argument(
        "--output_type", 
        type=str,
        help="The output artifact type",
        required=True
    )

    parser.add_argument(
        "--output_description", 
        type=str,
        help="The output artifact description",
        required=True
    )

    parser.add_argument(
        "--min_price", 
        type=float,
        help="The minimum price to consider",
        required=True
    )

    parser.add_argument(
        "--max_price", 
        type=float,
        help="The maximum price to consider",
        required=True
    )


    args = parser.parse_args()

    go(args)
