from pprint import pprint
import time
from datetime import datetime
from typing import Any, List

import dbt_cloud_admin
import gcp_bigquery
import dbt_cloud_metedata

DBT_CLOUD_CITY_BLOCK_ACCOUNT = 39643
DBT_CLOUD_GENERATE_MANIFEST_JOB_ID = 56929


def write_job_run_details_to_bigquery(job_run_details):

    job_data = job_run_details['data']

    bq_dbt_job_run_column_names = ['account_id', 'project_id', 'job_definition_id', 'id', 'status_humanized',
                                   'created_at', 'dequeued_at', 'started_at', 'finished_at',
                                   'duration', 'queued_duration', 'run_duration'
                                   ]

    bq_dbt_job_run_record = [tuple(job_data[x] for x in bq_dbt_job_run_column_names)]

    bq_table_name = 'cityblock-data.dbt.dbt_cloud_job_run'
    gcp_bigquery.load_records_to_bigquery(bq_table_name, bq_dbt_job_run_record)
    
    
def get_job_run_metadata(job_id, job_run_id):

    models = []
    for i in range(8):
        time.sleep(15)
            
        models = dbt_cloud_metedata.get_models_for_job_run(job_id, job_run_id)

        test_found = False
        if models is not None:
            for model in models:
                if model['tests']:
                    test_found = True
                    break

            if not models or test_found:
                break

    sources = []
    for i in range(8):
        if i > 0:
            time.sleep(15)
            
        sources = dbt_cloud_metedata.get_sources_for_job_run(job_id, job_run_id)

        test_found = False
        if sources is not None:
            for source in sources:
                if source['tests']:
                    test_found = True
                    break

            if not sources or test_found:
                break

    job_run_metadata = {'models': models, 'sources': sources}

    print(f'get_job_run_metadata complete. Retrieved {len(models)} models and {len(sources)} sources.')
    return job_run_metadata


def write_job_run_test_results_to_bigquery(tests):

    api_fields_ordered_for_target = \
        [
            'accountId',
            'projectId',
            'jobId',
            'runId',
            'test_on_unique_id',
            'uniqueId'
        ]

    bq_records = []
    for test in tests:
        if test['test_result_count'] > 0:
            try:
                hash_keys = test['test_result_hash_keys']
                test_field_values = []
                for api_field in api_fields_ordered_for_target:
                    next_value = None
                    try:
                        next_value = test[api_field]
                    except KeyError:
                        pass

                    test_field_values.append(next_value)

                for hash_key in hash_keys:
                    bq_record = test_field_values.copy()
                    bq_record.append(hash_key)

                    bq_records.append(tuple(bq_record))
            except KeyError:
                pass

            # print(f"format test results for bigquery:  {test_field_values[4]}")

    bq_table_name = 'cityblock-data.dbt.dbt_cloud_job_run_test_result'

    try:
        gcp_bigquery.load_records_to_bigquery(bq_table_name, bq_records)
    except ValueError as err:
        pprint(bq_table_name)
        pprint(bq_records[:10])
        raise err


def write_job_run_test_metadata_to_bigquery(tests):

    api_fields_ordered_for_target = \
        [
            'accountId',
            'projectId',
            'jobId',
            'runId',
            'uniqueId',
            'name',
            'alias',
            'description',
            'resourceType',
            'test_on_unique_id',
            'test_on_database',
            'test_on_schema',
            'test_on_resource',
            'columnName',
            'test_type',
            'status',
            'error',
            'fail',
            'warn',
            'test_result_table',
            'test_result_columns',
            'test_result_count'
        ]

    bq_records = []
    for test in tests:
        bq_record = []
        for api_field in api_fields_ordered_for_target:
            next_value = None
            try:
                next_value = test[api_field]
            except KeyError:
                pass

            bq_record.append(next_value)

        bq_records.append(tuple(bq_record))

    bq_table_name = 'cityblock-data.dbt.dbt_cloud_job_run_test'

    try:
        gcp_bigquery.load_records_to_bigquery(bq_table_name, bq_records)
    except ValueError as err:
        pprint(bq_table_name)
        pprint(bq_records)
        raise err


def enhance_test_metadata_from_manifest(account_id, run_id, tests):

    manifest = dbt_cloud_admin.get_artifact(account_id, run_id)

    for test in tests:
        test_node = manifest['nodes'][test['uniqueId']]
        test['alias'] = test_node['alias']
        test['description'] = test_node['description']
        test['test_result_table'] = test_node['relation_name'].replace('`', '')
        try:
            test['test_type'] = test_node['test_metadata']['name']
        except KeyError:
            pass

    return tests


def enhance_test_metadata_from_run_results(account_id, run_id, tests):

    run_results = dbt_cloud_admin.get_artifact(account_id, run_id, 'run_results.json')
    results = run_results['results']

    for test in tests:
        test_unique_id = test['uniqueId']
        for result in results:
            if result['unique_id'] == test_unique_id:
                test['test_result_count'] = result['failures']

    return tests


def write_job_run_resources_to_bigquery(resources):

    api_fields_ordered_for_target = \
        [
            'accountId',
            'projectId',
            'jobId',
            'runId',
            'uniqueId',
            'name',
            'resourceType',
            'database',
            'schema',
            'alias',
            'status',
            'runGeneratedAt',
            'compileStartedAt',
            'compileCompletedAt',
            'executeStartedAt',
            'executeCompletedAt',
            'executionTime'
        ]

    bq_records = []

    for resource in resources:
        bq_record = []
        for api_field in api_fields_ordered_for_target:
            next_value = None
            try:
                next_value = resource[api_field]
            except KeyError:
                pass

            bq_record.append(next_value)

        bq_records.append(tuple(bq_record))

    bq_table_name = 'cityblock-data.dbt.dbt_cloud_job_run_resource'
    gcp_bigquery.load_records_to_bigquery(bq_table_name, bq_records)


def write_job_run_metadata_to_bigquery(job_run_metadata):

    resources = job_run_metadata['models'] + job_run_metadata['sources']
    write_job_run_resources_to_bigquery(resources)


def extract_test_metadata(job_run_metadata):

    resources = job_run_metadata['models'] + job_run_metadata['sources']
    tests = []

    for resource in resources:

        resource_tests = resource['tests']
        resource_database = resource['database']
        resource_schema = resource['schema']
        resource_unique_id = resource['uniqueId']

        if resource['resourceType'] == 'model':
            resource_name = resource['alias']
        else:
            resource_name = resource['name']

        for resource_test in resource_tests:
            resource_test['test_on_unique_id'] = resource_unique_id
            resource_test['test_on_database'] = resource_database
            resource_test['test_on_schema'] = resource_schema
            resource_test['test_on_resource'] = resource_name

        tests.extend(resource_tests)

    print(f'extract_test_metadata complete. Retrieved {len(tests)} tests.')

    return tests


def merge_test_metadata_with_results(tests):

    tests_with_results: List[Any] = []
    test_on_table_metadata_dict = {}

    for test in tests:
        test_result_table = test['test_result_table']
        test_result_table_metadata = gcp_bigquery.get_table_metadata(test_result_table)

        test_on_table = '.'.join([test['test_on_database'], test['test_on_schema'], test['test_on_resource']])

        try:
            test_on_table_metadata = test_on_table_metadata_dict[test_on_table]
        except KeyError:
            test_on_table_metadata = gcp_bigquery.get_table_metadata(test_on_table)
            test_on_table_metadata_dict[test_on_table] = test_on_table_metadata

        test_result_columns = []

        for test_result_schema_field in test_result_table_metadata['schema']:
            if test_result_schema_field.field_type != 'RECORD' and test_result_schema_field.mode != 'REPEATED':
                for test_on_table_schema_field in test_on_table_metadata['schema']:
                    if test_result_schema_field.name == test_on_table_schema_field.name:
                        test_result_columns.append(test_result_schema_field.name)
                        break

        test_with_results = test

        try:
            test_with_results['test_result_count']
        except KeyError:
            test_with_results['test_result_count'] = test_result_table_metadata['num_rows']

        if test_result_columns:
            test_with_results['test_result_columns'] = test_result_columns
            test_with_results['test_result_hash_keys'] = gcp_bigquery.get_hash_keys(test_result_table,
                                                                                    test_result_columns
                                                                                    )

        tests_with_results.append(test_with_results)

    return tests_with_results


def process_tests(account_id, run_id, job_run_metadata):

    tests = extract_test_metadata(job_run_metadata)

    enhanced_tests = enhance_test_metadata_from_manifest(account_id, run_id, tests)
    enhanced_tests = enhance_test_metadata_from_run_results(account_id, run_id, enhanced_tests)

    merged_tests = merge_test_metadata_with_results(enhanced_tests)

    # write_job_run_test_metadata_to_bigquery(merged_tests)

    write_job_run_test_results_to_bigquery(merged_tests)

    print(f'process_tests complete. {len(enhanced_tests)} tests processed.')


def write_all_test_metadata_to_bigquery(tests):

    keys_ordered_for_target = \
        [
            'unique_id',
            'name',
            'alias',
            'description',
            'test_type',
            'severity',
            'warn_if',
            'error_if',
            'test_on_unique_id',
            'test_on_database',
            'test_on_schema',
            'test_on_resource',
            'test_on_columns',
            'ref_unique_id',
            'ref_database',
            'ref_schema',
            'ref_resource',
            'package_name',
            'original_file_path',
            'database',
            'schema',
            'relation_name',
        ]

    bq_records = []
    for test in tests:
        bq_record = []
        for key in keys_ordered_for_target:
            next_value = None
            try:
                next_value = test[key]
            except KeyError:
                pass

            bq_record.append(next_value)

        bq_records.append(tuple(bq_record))

    bq_table_name = 'cityblock-data.dbt.dbt_tests'

    try:
        gcp_bigquery.truncate_table(bq_table_name)
        gcp_bigquery.load_records_to_bigquery(bq_table_name, bq_records)
    except ValueError as err:
        pprint(bq_table_name)
        pprint(bq_records)
        raise err


def get_all_test_metadata_from_manifest(account_id, run_id):

    artifact = dbt_cloud_admin.get_artifact(account_id, run_id)

    nodes = artifact['nodes']
    all_resources = {**nodes, **artifact['sources']}

    test_keys = ['test_metadata',
                 'depends_on',
                 'config',
                 'database',
                 'schema',
                 'unique_id',
                 'package_name',
                 'original_file_path',
                 'name',
                 'alias',
                 'description',
                 'column_name',
                 'relation_name'
                 ]

    config_keys = ['error_if',
                   'severity',
                   'warn_if'
                   ]

    tests = []
    all_keys = set()
    for key, node in nodes.items():
        if node['resource_type'] == 'test':
            test = {k: v for k, v in node.items() if k in test_keys}

            config = {k: v for k, v in test.pop('config').items() if k in config_keys}
            test.update(config)

            test_metadata = None
            test_column_name = None
            try:
                test_metadata = test.pop('test_metadata')

                test['test_type'] = test_metadata['name']

                test_column_name = test.pop('column_name')
            except KeyError:
                pass

            test_on_columns = []
            if test_column_name:
                test_on_columns = [test_column_name]
            elif test_metadata:
                test_metadata_kwargs = test_metadata['kwargs']

                for kwarg_key, kwarg_value in test_metadata_kwargs.items():
                    if 'column' in kwarg_key:
                        if isinstance(kwarg_value, list):
                            test_on_columns.extend(kwarg_value)
                        elif isinstance(kwarg_value, str):
                            test_on_columns.append(kwarg_value)

            if test_on_columns:
                test['test_on_columns'] = test_on_columns

            resource_keys = test.pop('depends_on')['nodes']

            try:
                test_on_resource_key = resource_keys[-1]
                test['test_on_unique_id'] = test_on_resource_key
                test_on_resource = all_resources[test_on_resource_key]
                test['test_on_database'] = test_on_resource['database']
                test['test_on_schema'] = test_on_resource['schema']
                if test_on_resource['resource_type'] == 'model':
                    test['test_on_resource'] = test_on_resource['alias']
                else:
                    test['test_on_resource'] = test_on_resource['name']

                if len(resource_keys) > 1:
                    ref_resource_key = resource_keys[0]
                    test['ref_unique_id'] = ref_resource_key
                    ref_resource = all_resources[ref_resource_key]
                    test['ref_database'] = ref_resource['database']
                    test['ref_schema'] = ref_resource['schema']
                    if ref_resource['resource_type'] == 'model':
                        test['ref_resource'] = ref_resource['alias']
                    else:
                        test['ref_resource'] = ref_resource['name']
            except KeyError:
                pass

            all_keys |= set(test.keys())
            tests.append(test)

    # pprint(all_keys)
    # pprint(tests[:10])
    return tests


def refresh_all_test_metadata_in_big_query(account_id=DBT_CLOUD_CITY_BLOCK_ACCOUNT,
                                           job_id=DBT_CLOUD_GENERATE_MANIFEST_JOB_ID
                                           ):

    job_run_details = dbt_cloud_admin.run(account_id, job_id)

    run_id = job_run_details['data']['id']

    all_tests = get_all_test_metadata_from_manifest(account_id, run_id)

    write_all_test_metadata_to_bigquery(all_tests)


def process_dbt_job_in_data_quality_framework(job_id, account_id=DBT_CLOUD_CITY_BLOCK_ACCOUNT):

    start_time = datetime.now()
    start_time_str = start_time.strftime("%H:%M:%S")
    print("Start Time =", start_time_str)

    job_run_details = dbt_cloud_admin.run(account_id, job_id)

    write_job_run_details_to_bigquery(job_run_details)

    run_id = job_run_details['data']['id']

    job_run_metadata = get_job_run_metadata(job_id, run_id)

    write_job_run_metadata_to_bigquery(job_run_metadata)

    process_tests(account_id, run_id, job_run_metadata)

    end_time = datetime.now()
    end_time_str = end_time.strftime("%H:%M:%S")
    print("Start Time =", end_time_str)

    run_time = end_time - start_time
    run_minutes = divmod(run_time.seconds, 60)
    print(f"Run Time. Minutes: {run_minutes[0]}, Seconds: {run_minutes[1]}")


if __name__ == '__main__':

    process_dbt_job_in_data_quality_framework(60996)

    # jrm = get_job_run_metadata(60996, 45284864)
    #
    # process_tests(DBT_CLOUD_CITY_BLOCK_ACCOUNT, 45284864, jrm)

    # all_test_metadata = get_all_test_metadata_from_manifest(DBT_CLOUD_CITY_BLOCK_ACCOUNT, 44528345)
    #
    # write_all_test_metadata_to_bigquery(all_test_metadata)

    # get_all_test_metadata_from_manifest(39643, 42930842)

    # refresh_all_test_metadata_in_big_query()
