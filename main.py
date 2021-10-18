import argparse
import csv
import datetime
import json
import logging
import os
import redis
import socket

import requests
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
from azure.storage.blob import ContainerClient

logger = logging.getLogger("report")

EMAILS = ("ajones@bink.com", "operations@bink.com", "sarmstrong@bink.com")
DISABLE_ENV_CRED = os.getenv("DISABLE_ENV_CRED", "true") == "true"

redis_url = os.getenv("REDIS_URL", "redis://redis:6379/0")

def is_leader():
    r = redis.Redis.from_url(redis_url)
    lock_key = "wasabi-report-lock"
    hostname = socket.gethostname()
    is_leader = False

    with r.pipeline() as pipe:
        try:
            pipe.watch(lock_key)
            leader_host = pipe.get(lock_key)
            if leader_host in (hostname.encode(), None):
                pipe.multi()
                pipe.setex(lock_key, 10, hostname)
                pipe.execute()
                is_leader = True
        except redis.WatchError:
            pass
    return is_leader


def send_email(subject: str, text: str):
    # Disabling env cred hides usless azure warning
    cred = DefaultAzureCredential(exclude_environment_credential=DISABLE_ENV_CRED)
    kvclient = SecretClient(vault_url="https://bink-uksouth-prod-com.vault.azure.net/", credential=cred)

    mailgun_secret = json.loads(kvclient.get_secret("mailgun").value)
    mailgun_api_key = mailgun_secret["MAILGUN_API_KEY"]
    mailgun_api = mailgun_secret["MAILGUN_API"]
    mailgun_domain = mailgun_secret["MAILGUN_DOMAIN"]

    for email in EMAILS:
        logger.info(f"Sending email to {email}")
        resp = requests.post(
            f"{mailgun_api}/{mailgun_domain}/messages",
            auth=("api", mailgun_api_key),
            data={"from": "Wasabi Report <wasabireport@bink.com>", "to": email, "subject": subject, "text": text},
        )
        if resp.status_code != 200:
            logger.warning(f"mailgun status code expected 200 got {resp.status_code}")


def run():
    if is_leader():
        logger.info("Getting Wasabi rollup file")

        connection_string = os.getenv("BLOB_CONNECTION_STRING")
        if not connection_string:
            logger.error("No blob storage connection string")
            return

        with ContainerClient.from_connection_string(
            connection_string, "harmonia-archive"
        ) as container_client:  # type: ContainerClient
            date = datetime.datetime.now()
            formatted_date = date.strftime("%Y/%m/%d")
            file_prefix = f"{formatted_date}/wasabi-club/Bink Catch All File"
            subject = f"Wasabi Catch All File {formatted_date}"

            email_body = f"File for {formatted_date} not found"
            for file in container_client.list_blobs(name_starts_with=file_prefix):
                logger.info("Found file")
                blob = container_client.get_blob_client(file.name)
                data = blob.download_blob().readall().decode()

                dates = set()

                csv_file = csv.DictReader(data.splitlines())
                for row in csv_file:
                    dates.add(row["Date"])

                email_body = f"Founds dates: {', '.join(dates)}"
                break
            else:
                logger.info("Did not find file")

            send_email(subject, email_body)


def main():
    root_logger = logging.getLogger()
    handler = logging.StreamHandler()
    formatter = logging.Formatter("%(asctime)s %(name)-12s %(levelname)-8s %(message)s")
    handler.setFormatter(formatter)
    root_logger.addHandler(handler)
    root_logger.setLevel(logging.INFO)
    logger.setLevel(logging.INFO)

    for logger_name in ("azure",):
        logging.getLogger(logger_name).setLevel(logging.WARNING)

    parser = argparse.ArgumentParser()
    parser.add_argument("--now", action="store_true", help="Run now")

    args = parser.parse_args()
    if args.now:
        run()
    else:
        logger.info("Starting scheduler")
        scheduler = BlockingScheduler()
        scheduler.add_job(run, CronTrigger.from_crontab("0 7 * * *"))
        scheduler.start()


if __name__ == "__main__":
    main()
