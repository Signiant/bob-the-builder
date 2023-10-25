import argparse
import re
from json import JSONDecodeError
from datadog_api_client import ApiClient, Configuration
from datadog_api_client.v2.api.service_definition_api import ServiceDefinitionApi
from datadog_api_client.v2.model.service_definition_schema_versions import ServiceDefinitionSchemaVersions
from datetime import datetime, timedelta, timezone
import logging
import os
import requests
import sys
import json

logging.basicConfig(format="%(asctime)s - %(levelname)8s: %(message)s", stream=sys.stdout)
logging.getLogger("requests").setLevel(logging.CRITICAL)
logging.getLogger("urllib3").setLevel(logging.CRITICAL)

SCHEDULE = ""  # Set desired cron formatted pattern for scheduled pipelines (e.g. "0 0 13 ? * 7 *")
WORKSPACE = ""  # Set Bitbucket workspace name to be used in HTTP requests


def get_default_branch(repo_slug: str) -> str | None:
    """
    Get the name of a repository's default branch.

    :param repo_slug: the repository the default branch is being retrieved from
    :return: the name of a repositories default branch
    """
    url = f"https://api.bitbucket.org/2.0/repositories/{WORKSPACE}/{repo_slug}/refs/branches/"

    headers = {
        "Accept": "application/json"
    }

    auth = get_bitbucket_credentials()

    response = requests.request(
        "GET",
        url,
        auth=auth,
        headers=headers,
        params={
            "q": "name=\"main\" OR name=\"master\""
        }
    )

    try:
        if "error" in json.loads(response.text):
            logging.error("Failed to get default branch name: " + json.loads(response.text)["error"]["message"])
            return

        branches = json.loads(response.text)['values']
    except JSONDecodeError:
        logging.error("Failed to get default branch name: " + response.reason)
        return

    default_branch = branches[0]["name"]
    return default_branch


def get_schedules(repo_slug: str) -> dict | None:
    """
    Get all scheduled pipelines for a repo

    :param repo_slug: the name of the repo to retrieve the scheduled pipelines from
    :return: all scheduled pipelines for a repo
    """
    logging.debug(f"Retrieving scheduled pipelines for repo: {repo_slug}...")

    url = f"https://api.bitbucket.org/2.0/repositories/{WORKSPACE}/{repo_slug}/pipelines_config/schedules"

    headers = {
        "Accept": "application/json"
    }

    auth = get_bitbucket_credentials()

    response = requests.request(
        "GET",
        url,
        auth=auth,
        headers=headers
    )

    try:
        if "error" in json.loads(response.text):
            logging.error("Failed to retrieve scheduled pipelines: " + json.loads(response.text)["error"]["message"])
            return
    except JSONDecodeError:
        logging.error("Failed to retrieve scheduled pipelines: " + response.reason)
        return

    schedules = json.loads(response.text)['values']
    return schedules


def delete_schedule(repo_slug: str, dry_run: bool) -> None:
    """
    Delete a scheduled pipeline

    :param repo_slug: the name of the repo containing the to-be-deleted scheduled pipeline
    :param dry_run: a flag that causes script to not make changes
    """
    logging.debug(f"Deleting scheduled pipeline for repo: {repo_slug}...")

    default_branch = get_default_branch(repo_slug)
    schedules = get_schedules(repo_slug)

    if schedules is None:
        if dry_run:
            logging.error("")
        else:
            logging.error("Failed to delete scheduled pipeline.")
        return

    if dry_run:
        logging.info(f"Eligible for Schedule Deletion: {repo_slug}")
        return

    for schedule in schedules:
        # Delete any schedule set to run on Saturday at 13:00 UTC that is targeting the default branch
        if schedule['cron_pattern'] == SCHEDULE and schedule['target']['selector']['pattern'] == default_branch:
            schedule_uuid = schedule["uuid"]
            url = (f"https://api.bitbucket.org/2.0/repositories/{WORKSPACE}/{repo_slug}/"
                   f"pipelines_config/schedules/{schedule_uuid}")

            headers = {
                "Accept": "application/json"
            }

            auth = get_bitbucket_credentials()

            response = requests.request(
                "DELETE",
                url,
                auth=auth,
                headers=headers
            )

            try:
                if "error" in json.loads(response.text):
                    logging.error("Failed to delete scheduled pipeline: " +
                                  json.loads(response.text)["error"]["message"])
                    return
            except JSONDecodeError:
                if response.status_code != 204:
                    logging.error("Failed to delete scheduled pipeline: " + response.reason)
                    return

            break

    logging.debug(f"Scheduled pipeline deleted for repo: {repo_slug}.")


def create_schedule(repo_slug: str, dry_run: bool) -> None:
    """
    Create a scheduled pipeline in a repo

    :param repo_slug: the name of the repo to create a scheduled pipeline in
    :param dry_run: a flag that causes script to not make changes
    """
    logging.debug(f"Creating scheduled pipeline for repo: {repo_slug}...")

    default_branch = get_default_branch(repo_slug)
    schedules = get_schedules(repo_slug)

    for schedule in schedules:
        if schedule['cron_pattern'] == SCHEDULE and schedule['target']['selector']['pattern'] == default_branch:
            logging.error("Failed to create scheduled pipeline: this schedule already exists.")
            return

    if dry_run:
        logging.info(f"Eligible for Scheduling: {repo_slug}")
        return

    url = f"https://api.bitbucket.org/2.0/repositories/{WORKSPACE}/{repo_slug}/pipelines_config/schedules"

    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json"
    }

    auth = get_bitbucket_credentials()

    payload = json.dumps(
        {
            "type": "pipeline_schedule",
            "target": {
                "type": "pipeline_ref_target",
                "selector": {
                    "pattern": default_branch,
                    "type": "branches"
                },
                "ref_name": default_branch,
                "ref_type": "branch"
            },
            "enabled": True,
            "cron_pattern": SCHEDULE
        }
    )

    response = requests.request(
        "POST",
        url,
        auth=auth,
        data=payload,
        headers=headers
    )

    try:
        if "error" in json.loads(response.text):
            logging.error("Failed to create scheduled pipeline: " + json.loads(response.text)["error"]["message"])
            return
    except JSONDecodeError:
        logging.error("Failed to create scheduled pipeline: " + response.reason)
        return

    logging.debug(f"Scheduled pipeline created for repo: {repo_slug}.")


def check_development_status(pipelines: list, test: bool) -> bool:
    """
    Determine if a repository is currently in development

    :param pipelines: a page of the latest pipelines in the repo
    :param test: a flag that sets the recent build time to 2 minutes
    :return: a boolean determining if a repository is in development
    """
    logging.debug("Checking development status...")

    recent_pipelines = 0

    for pipeline in pipelines:
        # Convert created_on date value to usable datetime format
        creation_str = pipeline["created_on"].replace("T", " ").replace("Z", "")
        creation_date = datetime.strptime(creation_str, '%Y-%m-%d %H:%M:%S.%f')

        # Get today's date in UTC
        today_str = datetime.now(tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S.%f')
        today = datetime.strptime(today_str, '%Y-%m-%d %H:%M:%S.%f')

        if test:
            recent = timedelta(minutes=2)
        else:
            recent = timedelta(weeks=1)

        # Check if pipeline was executed by user recently
        if today - creation_date <= recent:
            recent_pipelines += 1

            if pipeline["trigger"]["name"] != "SCHEDULE" or recent_pipelines > 1:
                logging.debug("This repo is in development.")
                return True

    logging.debug("This repo is not in development")
    return False


def get_bitbucket_credentials() -> tuple:
    """
    Get Bitbucket credentials from environment

    :return: Bitbucket credentials
    """
    return os.getenv('BB_USER_ID'), os.getenv('BB_APP_PASS')


def get_latest_pipelines(repo_slug: str) -> list | None:
    """
    Get a page of the latest pipelines in a repository

    :param repo_slug: the name of the repo containing the pipelines to be retrieved
    :return: the latest pipelines
    """
    logging.debug(f"Retrieving latest pipelines for repo: {repo_slug}...")

    url = f"https://api.bitbucket.org/2.0/repositories/{WORKSPACE}/{repo_slug}/pipelines"

    headers = {
        "Accept": "application/json"
    }

    auth = get_bitbucket_credentials()

    response = requests.request(
        "GET",
        url,
        auth=auth,
        headers=headers,
        params={
            "sort": "-created_on"
        }
    )

    try:
        if "error" in json.loads(response.text):
            logging.error("Failed to get latest pipelines: " + json.loads(response.text)["error"]["message"])
            return

        pipelines = json.loads(response.text)['values']
    except JSONDecodeError:
        logging.error("Failed to get latest pipelines: " + response.reason)
        return

    return pipelines


def match_override(repo_slug: str, override: list | tuple) -> bool:
    """
    Search for a repository in the override list

    :param repo_slug: A repo slug to search for in the list of override patterns
    :param override: A list of patterns to ignore
    :return: a boolean dictating if a repo slug should be ignored
    """
    for pattern in override:
        pattern = re.compile(pattern)
        if pattern.search(repo_slug) is not None:
            return True

    return False


def get_active_services() -> list:
    """
    Retrieve the repo names associated with the services listed in the Datadog service catalog

    :return: a list of repository names
    """
    logging.info("Retrieving active services...")

    services = []

    configuration = Configuration()
    with ApiClient(configuration) as api_client:
        api_instance = ServiceDefinitionApi(api_client)
        page = 0

        while True:
            response = api_instance.list_service_definitions(
                schema_version=ServiceDefinitionSchemaVersions.V2_1,
                page_number=page
            )

            if "errors" in response:
                logging.error(response["errors"][0])
                break

            for service in response['data']:
                # Extract repo name from Bitbucket repo URL in service definition
                url_components = service['attributes']['schema']['links'][-1]['url'].split("/")

                if url_components[4] != "workspace":
                    repo_slug = url_components[4]
                    services.append(repo_slug)

            # Stop making requests when the response is empty
            if not response["data"]:
                break
            else:
                page += 1

    return services


def process_services(repositories: list[str], override: list[str], dry_run: bool, test: bool) -> None:
    """
    Begin processing services

    :param repositories: a list of repositories to process
    :param override: a list of repository names to ignore
    :param dry_run: a flag that causes script to not make changes
    :param test: a flag that sets the recent build time to 2 minutes
    """
    logging.info("Processing services...")

    if repositories:
        active_services = repositories
    else:
        active_services = get_active_services()

    for i, service in enumerate(active_services):
        logging.info(f"Processing service: {service}...")

        if override:
            if match_override(service, override):
                logging.info(f"Bitbucket repo for service {service} overridden. Skipping...")
                continue

        pipelines = get_latest_pipelines(service)

        if not pipelines:
            logging.info(f"No pipelines found in repo for service: {service}. Skipping...")
            continue

        in_development = check_development_status(pipelines, test)

        if not in_development:
            create_schedule(service, dry_run)
        else:
            delete_schedule(service, dry_run)

    logging.info("Services processed.")


def lambda_handler(event: dict, _) -> None:
    """
        A function that handles events received by a Lambda

        :param event: data to be processed
        :param _: lambda invocation, function, and runtime environment info
        """
    repositories = event.get("repositories")
    override = event.get("override")
    dry_run = event.get("dry_run", False)
    verbose = event.get("verbose", False)
    test = event.get("test", False)

    # Configure root logger level
    if verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    else:
        logging.getLogger().setLevel(logging.INFO)

    process_services(repositories, override, dry_run, test)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Bitbucket repository build scheduling script")

    parser.add_argument(
        "-r", "--repositories",
        help="A list of repositories to process. Processes all active services by default",
        dest="repositories",
        nargs="+"
    )
    parser.add_argument(
        "-o", "--override",
        help="A list of repositories to ignore.",
        dest="override",
        nargs="+"
    )
    parser.add_argument(
        "-d", "--dry_run",
        help="Run script in dry run mode, making no changes.",
        dest="dry_run",
        action='store_true'
    )
    parser.add_argument(
        "-v", "--verbose",
        help="Cause script to be verbose, outputting more info in the logs.",
        dest="verbose",
        action='store_true'
    )
    parser.add_argument(
        "-t", "--test",
        help="Set recent build time to 2 minutes.",
        dest="test",
        action='store_true'
    )

    args = parser.parse_args()
    lambda_handler(event={"repositories": args.repositories,
                          "override": args.override,
                          "dry_run": args.dry_run,
                          "verbose": args.verbose,
                          "test": args.test},
                   _=None)
