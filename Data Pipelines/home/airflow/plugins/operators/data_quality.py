from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DataQualityOperator(BaseOperator):
    """
    Verify data quality of Sparkify tables as loaded in Redshift.
    """

    ui_color = "#89DA59"

    @apply_defaults
    def __init__(self, redshift_conn_id="", target_tables=[], *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.target_tables = target_tables

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        for table in self.target_tables:
            rows = redshift.get_records(
                "SELECT COUNT(*) FROM {}".format(table))

            if len(rows) < 1 or len(rows[0]) < 1:
                self.log.error("{} returned no rows.".format(table))
                raise ValueError(
                    "Data Quality check failed on table {}.".format(table))

            self.log.info("Table {} passed.".format(table))
