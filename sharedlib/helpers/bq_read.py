from google.cloud.bigquery import Client


def get_one_row_from_table(request):
    sql = get_sql_script(request, limit=1)
    return [row for row in yield_rows_from_query(sql, request)]


def yield_from_table(request):
    sql = get_sql_script(request)
    return yield_rows_from_query(sql, request)


def get_sql_script(request, limit=-1):
    table = f'`{request["SourceTable"]}`'
    limit_text = get_limit_text(limit)
    where_clause = get_where_clause(request)
    col = request["SourceColumn"]
    text = f"SELECT {col} FROM {table}{where_clause}{limit_text}"
    return text


def yield_rows_from_query(sql, request):
    source_column = request['SourceColumn']
    client = Client(request['project'])
    query_job = client.query(sql)
    query_job.result()
    destination = query_job.destination
    destination = client.get_table(destination)
    rows = client.list_rows(destination)
    for row in rows:
        yield row.get(source_column)


def get_where_clause(request):
    template = " WHERE {PartitionColumn} = '{Partition}' {Condition}"
    if request['Partition'] == 'latest':
        request['Partition'] = get_latest_partition(request)
    return template.format(**request)


def get_latest_partition(request):
    query = """
        SELECT MAX({PartitionColumn}) AS latest_partition
        FROM `{SourceTable}`
    """.format(**request)
    client = Client(project=request['project'])
    for row in client.query(query).result():
        return row.get('latest_partition')


def get_limit_text(limit):
    if limit >= 0:
        return f" LIMIT {limit}"
    else:
        return ""
