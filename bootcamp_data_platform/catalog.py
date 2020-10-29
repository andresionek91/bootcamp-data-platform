from common import Environment
from aws_cdk import core
from app import DataLake
from aws_cdk import (
    aws_glue as glue
)


class Catalog(core.Stack):

    def __init__(self, scope: core.Construct, environment: Environment, data_lake: DataLake, **kwargs) -> None:
        self.env = environment.value
        super().__init__(scope, id=f'{self.env}-catalog', **kwargs)

        self.atomic_events_crawler = glue.CfnCrawler(
            self,
            f'{self.env}-atomic-events-crawler',
            name=f'{self.env}-atomic-events-crawler',
            description='Crawler to detect schema of data sored in data lake raw, atomic events',
            schedule=glue.CfnCrawler.ScheduleProperty(schedule_expression='cron(* 15 * * ? *)'),
            role=data_lake.data_lake_role.role_arn,
            targets=glue.CfnCrawler.TargetsProperty(
                s3_targets=[
                    glue.CfnCrawler.S3TargetProperty(path=f's3://{data_lake.data_lake_raw_bucket.bucket_name}/atomic_events')
                ]
            ),
            database_name=data_lake.data_lake_raw_database.database_name,
        )

