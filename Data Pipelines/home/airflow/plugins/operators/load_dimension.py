from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadDimensionOperator(BaseOperator):
    """
    Operator used to load dimension tables.
    This operator uses the following arguments:

    redshift_conn_id        AWS Redshift connection ID
    target_table            Name of the AWS Redshift target table
    query                   Query name from SqlQueries (in the helper folder)
    append_flag             Boolean - TRUE appends the existing table and 
                                FALSE deletes and replaces the existing
                                table (defaults TRUE)
    """

    ui_color = "#80BD9E"

    @apply_defaults
    def __init__(
        self,
        redshift_conn_id="",
        target_table="",
        query="",
        append_flag=True,
        *args,
        **kwargs
    ):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.target_table = target_table
        self.query = query
        self.append_flag = append_flag

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        # Delete existing table if append_flag is False
        if self.append_flag == False:
            self.log.info("Deleting existing table.")
            redshift.run("DELETE FROM {}".format(self.target_table))

        self.log.info(
            "Loading dimension table {} into Redshift.".format(
                self.target_table)
        )

        redshift.run("INSERT INTO {}\n{};".format(
            self.target_table, self.query))

        self.log.info(
            "Completed loading table {} into Redshift.".format(
                self.target_table)
        )
