import os
import requests

from pprint import pprint

JOB_ID = 0

# Store your dbt Cloud API token securely in your workflow tool
# API_KEY = os.getenv('DBT_CLOUD_API_TOKEN')
API_KEY = '383c602284.3144998ddb74e820ff37882304b721c30061aa52b9e643d10fa41611ea91a275'


class ErrorsListedInResponse(Exception):
    pass


def query_graphql_api(graphql_query):

    url_text = 'https://metadata.cloud.getdbt.com/graphql'
    headers = {
        "Accept": "application/json",
        "Authorization": f"Bearer {API_KEY}"
    }

    res = requests.post(url_text, data={"query": graphql_query}, headers=headers)

    response_payload = res.json()

    try:
        res.raise_for_status()

        if response_payload['errors']:
            raise ErrorsListedInResponse

    except (requests.exceptions.HTTPError, ErrorsListedInResponse):
        print(f"API token (last four): ...{API_KEY[-4:]}")
        pprint(response_payload['errors'])
        pprint(graphql_query)
        raise
    except KeyError:
        pass

    return response_payload


def get_models_for_job_run(job_id, run_id):

    graphql_query = f"""{{
        models(jobId: {job_id}, runId: {run_id}) {{
            accountId
            projectId
            jobId
            runId
            name
            uniqueId
            resourceType
            database
            schema
            alias
            status
            runGeneratedAt
            compileStartedAt
            compileCompletedAt
            executeStartedAt
            executeCompletedAt
            executionTime
            tests {{
                accountId
                projectId
                jobId
                runId
                name
                uniqueId
                resourceType
                columnName
                status
                error
                fail
                warn
            }}
        }}
    }}"""

    response_payload = query_graphql_api(graphql_query)

    models = response_payload['data']['models']

    if models:
        models_for_job_run = [model for model in models if model['runGeneratedAt']]
    else:
        models_for_job_run = None

    return models_for_job_run


def get_tests_for_job_run(job_id, run_id):

    graphql_query = f"""{{
        tests(jobId: {job_id}, runId: {run_id}) {{
            accountId
            projectId
            jobId
            runId
            name
            uniqueId
            resourceType
            columnName
            status
            error
            fail
            warn
        }}
    }}"""

    response_payload = query_graphql_api(graphql_query)

    tests_for_job_run = response_payload['data']['tests']

    return tests_for_job_run


def get_seeds_for_job_run(job_id, run_id):

    graphql_query = f"""{{
        seeds(jobId: {job_id}, runId: {run_id}) {{
            name
            uniqueId
            alias
            database
            schema
            runGeneratedAt
            executionTime
            executeStartedAt
            executeCompletedAt
        }}
    }}"""

    response_payload = query_graphql_api(graphql_query)

    seeds = response_payload['data']['seeds']

    if seeds:
        seeds_for_job_run = [seed for seed in seeds if seed['runGeneratedAt']]
    else:
        seeds_for_job_run = None

    return seeds_for_job_run


def get_sources_for_job_run(job_id, run_id):

    graphql_query = f"""{{
        sources(jobId: {job_id}, runId: {run_id}) {{
            accountId
            projectId
            jobId
            runId
            name
            uniqueId
            resourceType
            database
            schema
            runGeneratedAt
            tests {{
                accountId
                projectId
                jobId
                runId
                name
                uniqueId
                resourceType
                columnName
                status
                error
                fail
                warn
            }}
        }}
    }}"""

    response_payload = query_graphql_api(graphql_query)

    sources = response_payload['data']['sources']

    if sources:
        sources_for_job_run = [source for source in sources if source['tests']]
    else:
        sources_for_job_run = None

    return sources_for_job_run


def get_resources_for_job_run(job_id, run_id):
    models = get_models_for_job_run(job_id, run_id)
    seeds = get_seeds_for_job_run(job_id, run_id)
    sources = get_sources_for_job_run(job_id, run_id)
    # tests = get_tests_for_job_run_resources(job_id, run_id, models, sources)

    resources = {'models': models, 'seeds': seeds, 'sources': sources}  # , 'tests': tests}

    return resources


if __name__ == '__main__':
    res_for_run = get_resources_for_job_run(52818, 41944633)
    pprint(res_for_run)
