import enum
import os
import time
from pprint import pprint

import requests

# Store your dbt Cloud API token securely in your workflow tool
# API_KEY = os.getenv('DBT_CLOUD_API_TOKEN')
API_KEY = '383c602284.3144998ddb74e820ff37882304b721c30061aa52b9e643d10fa41611ea91a275'


# These are documented on the dbt Cloud API docs
class DbtJobRunStatus(enum.IntEnum):
    QUEUED = 1
    STARTING = 2
    RUNNING = 3
    SUCCESS = 10
    ERROR = 20
    CANCELLED = 30


def get_accounts(account_id=''):
    url_text = 'https://cloud.getdbt.com/api/v2/accounts/'
    if account_id:
        url_text += f'{account_id}/'

    res = requests.get(
        url=url_text,
        headers={'Authorization': f"Token {API_KEY}"},
    )

    try:
        res.raise_for_status()
    except:
        print(f"API token (last four): ...{API_KEY[-4:]}")
        raise

    response_payload = res.json()

    return response_payload


def get_projects(account_id, project_id=''):
    url_text = f'https://cloud.getdbt.com/api/v2/accounts/{account_id}/projects/'
    if project_id:
        url_text += f'{project_id}/'

    res = requests.get(
        url=url_text,
        headers={'Authorization': f"Token {API_KEY}"},
    )

    try:
        res.raise_for_status()
    except:
        print(f"API token (last four): ...{API_KEY[-4:]}")
        raise

    response_payload = res.json()

    return response_payload


def get_jobs(account_id, job_id=''):
    url_text = f'https://cloud.getdbt.com/api/v2/accounts/{account_id}/jobs/'
    if job_id:
        url_text += f'{job_id}/'

    res = requests.get(
        url=url_text,
        headers={'Authorization': f"Token {API_KEY}"},
    )

    try:
        res.raise_for_status()
    except:
        print(f"API token (last four): ...{API_KEY[-4:]}")
        raise

    response_payload = res.json()

    return response_payload


def get_artifact(account_id, run_id, path='manifest.json'):
    url_text = f'https://cloud.getdbt.com/api/v2/accounts/{account_id}/runs/{run_id}/artifacts/{path}'

    res = requests.get(
        url=url_text,
        headers={'Authorization': f"Token {API_KEY}"},
    )

    try:
        res.raise_for_status()
    except requests.exceptions.HTTPError:
        print(f"API token (last four): ...{API_KEY[-4:]}")
        print(f"account_id:  {account_id}. run_id:  {run_id}.")
        # pprint(res.json())
        raise

    response_payload = res.json()

    return response_payload


def _trigger_job(account_id, job_id) -> int:
    res = requests.post(
        url=f"https://cloud.getdbt.com/api/v2/accounts/{account_id}/jobs/{job_id}/run/",
        headers={'Authorization': f"Token {API_KEY}"},
        json={
            # Optionally pass a description that can be viewed within the dbt Cloud API.
            # See the API docs for additional parameters that can be passed in,
            # including `schema_override`
            'cause': f"Triggered by my workflow!",
        }
    )

    try:
        res.raise_for_status()
    except:
        print(f"API token (last four): ...{API_KEY[-4:]}")
        raise

    response_payload = res.json()
    return response_payload['data']['id']


def _get_job_run(account_id, job_run_id):
    res = requests.get(
        url=f"https://cloud.getdbt.com/api/v2/accounts/{account_id}/runs/{job_run_id}/",
        headers={'Authorization': f"Token {API_KEY}"},
    )

    res.raise_for_status()
    response_payload = res.json()
    return response_payload


def _get_job_run_status(account_id, job_run_id):

    response_payload = _get_job_run(account_id, job_run_id)
    return response_payload['data']['status']


def run(account_id, job_id):
    job_run_id = _trigger_job(account_id, job_id)

    print(f"job_run_id = {job_run_id}")

    while True:
        time.sleep(5)

        status = _get_job_run_status(account_id, job_run_id)

        print(f"status = {status}")

        if status == DbtJobRunStatus.SUCCESS:
            break
        elif status == DbtJobRunStatus.ERROR or status == DbtJobRunStatus.CANCELLED:
            raise Exception("Failure!")

    response_payload = _get_job_run(account_id, job_run_id)

    return response_payload


if __name__ == '__main__':
    # run(39643, 52818)

    m = get_artifact(39643, 45284864, 'run_results.json')

    print(m)

    # tests = []
    # for value in m['nodes'].values():
    #     if value:
    #         try:
    #             if value['test_metadata']['name'] == 'equality_where':
    #                 tests.append(value)
    #         except KeyError:
    #             pass
    #
    # pprint(tests)
