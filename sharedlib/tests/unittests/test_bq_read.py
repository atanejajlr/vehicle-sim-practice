from unittest import (
    TestCase,
    mock
)
from sharedlib.helpers.bq_read import (
    get_sql_script,
    get_limit_text,
    get_where_clause,
    get_one_row_from_table,
    yield_from_table
)


class TestBqRead(TestCase):

    def setUp(self):
        self.request = {
            "SourceTable": "jlr-cat-vehiclesim-dev.2020_ace_dev.101_ace_model_year_mappings",
            "SourceColumn": "BuildPhaseMapping_Feature",
            "topic_name": "projects/jlr-cat-vehiclesim-dev/topics/test_ingestion",
            "project": "jlr-cat-vehiclesim-dev",
            "Partition": "latest",
            "PartitionColumn": "date_of_run",
            "Condition": "AND ProductRange_Code='L460'"
        }

    @mock.patch('sharedlib.helpers.bq_read.get_sql_script')
    @mock.patch('sharedlib.helpers.bq_read.yield_rows_from_query')
    def test_get_one_row_from_table(self,
                                    yield_mock,
                                    sql_mock,
                                    ):
        row = ['foo', 'baz']
        sql_mock.return_value = 'SELECT TEST FROM TABLE'
        yield_mock.return_value = iter(row)
        actual_row = get_one_row_from_table(self.request)
        sql_mock.assert_called_with(self.request, limit=1)
        yield_mock.assert_called_with(
            'SELECT TEST FROM TABLE',
            self.request
        )
        self.assertEqual(actual_row, row)

    @mock.patch('sharedlib.helpers.bq_read.get_sql_script')
    @mock.patch('sharedlib.helpers.bq_read.yield_rows_from_query')
    def test_yield_from_table(self,
                              yield_mock,
                              sql_mock,
                              ):
        sql_mock.return_value = 'SELECT TEST FROM TABLE'
        yield_from_table(self.request)
        sql_mock.assert_called_with(self.request)
        yield_mock.assert_called_with(
            'SELECT TEST FROM TABLE',
            self.request
        )

    @mock.patch('sharedlib.helpers.bq_read.get_where_clause',
                mock.MagicMock(return_value=" WHERE date_of_run = '2021-11-02' "))
    @mock.patch('sharedlib.helpers.bq_read.get_limit_text', mock.MagicMock(return_value=' LIMIT 1'))
    def test_get_sql_script_limit_1(self):
        expected = "SELECT BuildPhaseMapping_Feature " \
                   "FROM `jlr-cat-vehiclesim-dev.2020_ace_dev.101_ace_model_year_mappings` " \
                   "WHERE date_of_run = '2021-11-02'  LIMIT 1"
        actual = get_sql_script(self.request, limit=1)
        self.assertEqual(expected, actual)

    @mock.patch('sharedlib.helpers.bq_read.get_where_clause',
                mock.MagicMock(return_value=" WHERE date_of_run = '2021-11-02' "))
    @mock.patch('sharedlib.helpers.bq_read.get_limit_text', mock.MagicMock(return_value=''))
    def test_get_sql_script_no_limit(self):
        expected = "SELECT BuildPhaseMapping_Feature " \
                   "FROM `jlr-cat-vehiclesim-dev.2020_ace_dev.101_ace_model_year_mappings` " \
                   "WHERE date_of_run = '2021-11-02' "
        actual = get_sql_script(self.request)
        self.assertEqual(expected, actual)

    def test_get_limit_text_limit_1(self):
        expected = " LIMIT 1"
        actual = get_limit_text(1)
        self.assertEqual(expected, actual)

    def test_get_limit_text_limit_neg_1(self):
        expected = ""
        actual = get_limit_text(-1)
        self.assertEqual(expected, actual)

    @mock.patch('sharedlib.helpers.bq_read.get_latest_partition', mock.MagicMock(return_value='2021-11-02'))
    def test_get_where_clause_default_partition(self):
        expected = " WHERE date_of_run = '2021-11-02' AND ProductRange_Code='L460'"
        actual = get_where_clause(self.request)
        self.assertEqual(expected, actual)

    @mock.patch('sharedlib.helpers.bq_read.get_latest_partition', mock.MagicMock(return_value='2021-11-02'))
    def test_get_where_clause_default_partition_without_cond(self):
        self.request['Condition'] = ""
        expected = " WHERE date_of_run = '2021-11-02' "
        actual = get_where_clause(self.request)
        self.assertEqual(expected, actual)

    def test_get_where_clause_defined_partition(self):
        self.request['Partition'] = '2021-10-08'
        expected = " WHERE date_of_run = '2021-10-08' AND ProductRange_Code='L460'"
        actual = get_where_clause(self.request)
        self.assertEqual(expected, actual)

    def test_get_where_clause_defined_partition_without_cond(self):
        self.request['Partition'] = '2021-10-08'
        self.request['Condition'] = ""
        expected = " WHERE date_of_run = '2021-10-08' "
        actual = get_where_clause(self.request)
        self.assertEqual(expected, actual)
