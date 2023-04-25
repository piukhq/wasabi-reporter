import argparse
import csv
import datetime
import json
import logging
import os
import socket

import redis
import requests
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
from azure.storage.blob import ContainerClient
from pythonjsonlogger import jsonlogger

logger = logging.getLogger("report")

EMAILS = ("ajones@bink.com", "operations@bink.com", "sarmstrong@bink.com", "dpayton@bink.com", "devops@bink.com")
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
    kvclient = SecretClient(vault_url="https://uksouth-prod-qj46.vault.azure.net/", credential=cred)

    mailgun_secret = json.loads(kvclient.get_secret("mailgun").value)
    mailgun_api_key = mailgun_secret["MAILGUN_API_KEY"]
    mailgun_api = mailgun_secret["MAILGUN_API"]
    mailgun_domain = mailgun_secret["MAILGUN_DOMAIN"]

    for email in EMAILS:
        logging.info(f"Sending email to {email}")
        resp = requests.post(
            f"{mailgun_api}/{mailgun_domain}/messages",
            auth=("api", mailgun_api_key),
            data={"from": "Wasabi Report <wasabireport@bink.com>", "to": email, "subject": subject, "text": text},
        )
        if resp.status_code != 200:
            logging.warning(f"mailgun status code expected 200 got {resp.status_code}")


def run():
    if is_leader():
        logging.info("Getting Wasabi rollup file")

        connection_string = os.getenv("BLOB_CONNECTION_STRING")
        if not connection_string:
            logging.error("No blob storage connection string")
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
                logging.info("Found file", extra={"filename": file})
                blob = container_client.get_blob_client(file.name)
                data = blob.download_blob().readall().decode()

                dates = set()

                csv_file = csv.DictReader(data.splitlines())
                for row in csv_file:
                    dates.add(row["Date"])

                email_body = f"Founds dates: {', '.join(dates)}"
                break
            else:
                logging.info("Did not find file")

            logging.warning("Sending email", extra={"E-Mail Subject": subject, "E-Mail Body": email_body})
            send_email(subject, email_body)


def main():
    logger = logging.getLogger()
    logHandler = logging.StreamHandler()
    logFmt = jsonlogger.JsonFormatter(timestamp=True)
    logHandler.setFormatter(logFmt)
    logger.addHandler(logHandler)

    for logger_name in ("azure",):
        logging.getLogger(logger_name).setLevel(logging.WARNING)

    parser = argparse.ArgumentParser()
    parser.add_argument("--now", action="store_true", help="Run now")

    args = parser.parse_args()
    if args.now:
        logging.warning(msg="Running Wasabi Reporter now")
        run()
    else:
        logging.warning(msg="Starting Wasabi Reporter...")
        scheduler = BlockingScheduler()
        scheduler.add_job(run, CronTrigger.from_crontab("0 7 * * *"))
        scheduler.start()


if __name__ == "__main__":
    main()
