from google.cloud import bigquery
from google.api_core import exceptions as google_exceptions

from pprint import pprint


def truncate_table(table_name):
    table_metadata = get_table_metadata(table_name)

    if table_metadata['num_rows'] > 0:
        bigquery_client = bigquery.Client()

        dml_statement = (
            f"TRUNCATE TABLE {table_name}")

        try:
            query_job = bigquery_client.query(dml_statement)
            query_job.result()
        except google_exceptions.GoogleAPICallError as err:
            raise err


def load_records_to_bigquery(table_name, rows_to_insert, batch_size=10000):
    # Instantiates a client
    bigquery_client = bigquery.Client()

    table = bigquery_client.get_table(table_name)  # API call

    num_rows = len(rows_to_insert)
    rows_loaded = 0

    while rows_loaded < num_rows:
        start = rows_loaded
        end = min(rows_loaded + batch_size, num_rows)

        batch_to_insert = rows_to_insert[start:end]

        try:
            errors = bigquery_client.insert_rows(table, batch_to_insert)
            if errors:
                raise Exception('insert returned errors')

            rows_loaded = end

            pprint(f'Loading records to {table_name}:  {rows_loaded} out of {num_rows}')
        except google_exceptions.GoogleAPICallError as err:
            raise err
        except:
            pprint(errors)
            pprint(batch_to_insert)
            raise


def get_table_metadata(fully_qualified_table_id):
    client = bigquery.Client()

    table = client.get_table(fully_qualified_table_id)

    table_metadata = {
        'project': table.project,
        'dataset_id': table.dataset_id,
        'table_id': table.table_id,
        'schema': table.schema,
        'description': table.description,
        'num_rows': table.num_rows,
    }

    return table_metadata


def get_unique_keys_for_tables():

    client = bigquery.Client()

    sql = f"""
        select 
            test_on_unique_id,
            test_on_database,
            test_on_schema,
            test_on_resource,
            test_on_columns
        from (
            select
                *,
                row_number() over(
                    partition by test_on_unique_id
                    order by array_length(test_on_columns)
                            ,unique_id
                ) as rnum
            FROM `cityblock-data.dbt.dbt_tests` 
            where test_type like '%unique%'
        )
        where rnum = 1
        """

    query_job = client.query(sql)

    try:
        results = query_job.result()
    except google_exceptions.BadRequest as err:
        pprint(sql)
        raise err

    rows = [dict(row) for row in results]
    return rows


def get_hash_keys(fully_qualified_table_id, columns):

    client = bigquery.Client()

    columns_as_strings = [f"ifnull(cast(`{column}` as string),'')" for column in columns]
    hash_function_input = ',"-",'.join(columns_as_strings)
    sql = f"""
        select distinct to_hex(md5(concat({hash_function_input}))) as hash_key
        from {fully_qualified_table_id}"""

    query_job = client.query(sql)

    try:
        results = query_job.result()
    except google_exceptions.BadRequest as err:
        pprint(sql)
        raise err

    hash_keys = [row.hash_key for row in results]
    return hash_keys


if __name__ == '__main__':

    # test_rows = [
    #     (9283,),
    #     (18376,),
    # ]
    #
    # test_table = 'dbt_test_framework.test_table'
    # load_records_to_bigquery(test_table, test_rows)
    #
    # table = 'cityblock-dbt.dbt_test__audit.not_null_abs_facility_flat_claimId'
    # table_metadata = get_table_metadata(table)
    # pprint(table_metadata)
    # table_columns = [schema_field.name
    #                  for schema_field in table_metadata['schema']
    #                  if schema_field.field_type != 'RECORD' and schema_field.mode != 'REPEATED'
    #                  ]
    # table_hash_keys = get_hash_keys(table, table_columns)
    # pprint(table_hash_keys[:10])

    r = get_unique_keys_for_tables()
    pprint(r)

