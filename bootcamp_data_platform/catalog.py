from aws_cdk import core
from data_lake import DataLake
from aws_cdk import (
    aws_glue as glue
)


class GlueCatalog(core.Stack):

    def __init__(self, scope: core.Construct, data_lake: DataLake, **kwargs) -> None:
        self.env = data_lake.env.value
        super().__init__(scope, id=f'{self.env}-glue-catalog', **kwargs)

        self.atomic_events_crawler = glue.CfnCrawler(
            self,
            f'{self.env}-atomic-events-crawler',
            name=f'{self.env}-atomic-events-crawler',
            description='Crawler to detect schema of data sored in data lake raw, atomic events',
            schedule=glue.CfnCrawler.ScheduleProperty(schedule_expression='cron(0/15 * * * ? *)'),
            role=data_lake.data_lake_role.role_arn,
            targets=glue.CfnCrawler.TargetsProperty(
                s3_targets=[
                    glue.CfnCrawler.S3TargetProperty(
                        path=f's3://{data_lake.data_lake_raw_bucket.bucket_name}/atomic_events')
                ]
            ),
            database_name=data_lake.data_lake_raw_database.database_name
        )

        self.orders_table = glue.Table(
            self,
            f'{self.env}-orders-table',
            table_name='orders',
            description='orders captured from Postgres using DMS CDC',
            database=data_lake.data_lake_raw_database,
            compressed=True,
            data_format=glue.DataFormat(
                input_format=glue.InputFormat.PARQUET,
                output_format=glue.OutputFormat.PARQUET,
                serialization_library=glue.SerializationLibrary.PARQUET
            ),
            s3_prefix='orders/public/orders',
            bucket=data_lake.data_lake_raw_bucket,
            columns=[
                glue.Column(name='op', type=glue.Type(input_string='string', is_primitive=True)),
                glue.Column(name='extracted_at', type=glue.Type(input_string='string', is_primitive=True)),
                glue.Column(name='created_at', type=glue.Type(input_string='timestamp', is_primitive=True)),
                glue.Column(name='order_id', type=glue.Type(input_string='int', is_primitive=True)),
                glue.Column(name='product_name', type=glue.Type(input_string='string', is_primitive=True)),
                glue.Column(name='value', type=glue.Type(input_string='double', is_primitive=True))
            ]
        )
