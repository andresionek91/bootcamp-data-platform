from aws_cdk import core
from aws_cdk import (
    aws_kinesisfirehose as firehose,
    aws_iam as iam
)
from common import Environment
from data_lake import DataLake, DataLakeBucket
from data_lake_core import Layer


class RawKinesisRole(iam.Role):

    def __init__(self, scope: core.Construct, environment: Environment, raw_bucket: DataLakeBucket, **kwargs) -> None:
        self.environment = environment.value
        super().__init__(
            scope,
            id=f'iam-{self.environment}-data-lake-raw-firehose-role',
            assumed_by=iam.ServicePrincipal('firehose.amazonaws.com'),
            description='Role to allow Kinesis to save data to data lake raw',
        )
        self.raw_bucket = raw_bucket
        self.add_policy()

    def add_policy(self):
        policy = iam.Policy(
            self,
            id=f'iam-{self.environment}-data-lake-raw-firehose-policy',
            policy_name=f'iam-{self.environment}-data-lake-raw-firehose-policy',
            statements=[
                iam.PolicyStatement(
                    actions=[
                        's3:AbortMultipartUpload',
                        's3:GetBucketLocation',
                        's3:GetObject',
                        's3:ListBucket',
                        's3:ListBucketMultipartUploads',
                        's3:PutObject'
                    ],
                    resources=[
                        self.raw_bucket.bucket_arn,
                        f'{self.raw_bucket.bucket_arn}/*'
                    ]
                )
            ]
        )
        self.attach_inline_policy(policy)

        return policy


class AtomicEventsRawIngestion(core.Stack):

    def __init__(self, scope: core.Construct, environment: Environment, data_lake: DataLake, **kwargs) -> None:
        super().__init__(scope, id='data-lake-raw-ingestion', **kwargs)
        name = f'firehose-{environment.value}-raw-delivery-stream'
        raw_bucket = data_lake.buckets[Layer.RAW]

        kinesis_role = RawKinesisRole(self, environment=environment, raw_bucket=raw_bucket)

        s3_config = firehose.CfnDeliveryStream.ExtendedS3DestinationConfigurationProperty(
            bucket_arn=raw_bucket.bucket_arn,
            compression_format='ZIP',
            error_output_prefix='bad_records',
            prefix='atomic_events/year=!{timestamp:yyyy}/'
                   'month=!{timestamp:MM}/day=!{timestamp:dd}/hour=!{timestamp:HH}/',
            buffering_hints=firehose.CfnDeliveryStream.BufferingHintsProperty(
                interval_in_seconds=60,
                size_in_m_bs=1
            ),
            role_arn=kinesis_role.role_arn
        )

        atomic_events = firehose.CfnDeliveryStream(
            self,
            id=name,
            delivery_stream_name=name,
            delivery_stream_type='DirectPut',
            extended_s3_destination_configuration=s3_config
        )