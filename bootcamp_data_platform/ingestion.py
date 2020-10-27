from aws_cdk import core
from aws_cdk import (
    aws_kinesisfirehose as firehose,
    aws_iam as iam,
    aws_dms as dms,
    aws_ec2 as ec2
)
from common import Environment, Common
from data_lake import DataLake, DataLakeBucket
import json


class RawKinesisRole(iam.Role):

    def __init__(self, scope: core.Construct, environment: str, raw_bucket: DataLakeBucket, **kwargs) -> None:
        self.environment = environment
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


class RawDMSRole(iam.Role):

    def __init__(self, scope: core.Construct, environment: str, raw_bucket: DataLakeBucket, **kwargs) -> None:
        self.environment = environment
        super().__init__(
            scope,
            id=f'iam-{self.environment}-data-lake-raw-dms-role',
            assumed_by=iam.ServicePrincipal('dms.amazonaws.com'),
            description='Role to allow DMS to save data to data lake raw',
        )
        self.raw_bucket = raw_bucket
        self.add_policy()

    def add_policy(self):
        policy = iam.Policy(
            self,
            id=f'iam-{self.environment}-data-lake-raw-dms-policy',
            policy_name=f'iam-{self.environment}-data-lake-raw-dms-policy',
            statements=[
                iam.PolicyStatement(
                    actions=[
                        's3:PutObjectTagging',
                        's3:DeleteObject',
                        's3:ListBucket',
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


class OrdersDMS(dms.CfnReplicationTask):
    def __init__(self, scope: core.Construct, common: Common, data_lake: DataLake, **kwargs) -> None:
        self.rds_endpoint = dms.CfnEndpoint(
            scope,
            f'dms-{common.env}-orders-rds-endpoint',
            endpoint_type='source',
            endpoint_identifier=f'dms-source-{common.env}-orders-rds-endpoint',
            engine_name='postgres',
            password=core.CfnDynamicReference(
                core.CfnDynamicReferenceService.SECRETS_MANAGER,
                key=f'{common.orders_rds.secret.secret_arn}:SecretString:password').to_string(),
            username=core.CfnDynamicReference(
                core.CfnDynamicReferenceService.SECRETS_MANAGER,
                key=f'{common.orders_rds.secret.secret_arn}:SecretString:username').to_string(),
            database_name=core.CfnDynamicReference(
                core.CfnDynamicReferenceService.SECRETS_MANAGER,
                key=f'{common.orders_rds.secret.secret_arn}:SecretString:dbname').to_string(),
            port=5432,
            server_name=common.orders_rds.db_instance_endpoint_address,
        )

        self.s3_endpoint = dms.CfnEndpoint(
            scope,
            f'dms-{common.env}-orders-s3-endpoint',
            endpoint_type='target',
            engine_name='s3',
            endpoint_identifier=f'dms-target-{common.env}-orders-s3-endpoint',
            extra_connection_attributes="DataFormat=parquet;maxFileSize=131072;timestampColumnName=extracted_at;includeOpForFullLoad=true;cdcInsertsAndUpdates=true",
            s3_settings=dms.CfnEndpoint.S3SettingsProperty(
                bucket_name=data_lake.data_lake_raw_bucket.bucket_name,
                bucket_folder='orders',
                compression_type='gzip',
                csv_delimiter=',',
                csv_row_delimiter='\n',
                service_access_role_arn=RawDMSRole(scope, common.env, data_lake.data_lake_raw_bucket).role_arn
            )
        )

        self.dms_sg = ec2.SecurityGroup(
            scope,
            f'dms-{common.env}-sg',
            vpc=common.custom_vpc,
            security_group_name=f'dms-{common.env}-sg',
        )

        self.dms_subnet_group = dms.CfnReplicationSubnetGroup(
            scope,
            f'dms-{common.env}-replication-subnet',
            replication_subnet_group_description='dms replication instance subnet group',
            subnet_ids=[subnet.subnet_id for subnet in common.custom_vpc.private_subnets],
            replication_subnet_group_identifier=f'dms-{common.env}-replication-subnet'
        )

        self.instance = dms.CfnReplicationInstance(
            scope,
            f'dms-replication-instance-{common.env}',
            allocated_storage=100,
            publicly_accessible=False,
            engine_version='3.3.2',
            replication_instance_class='dms.t2.small',
            replication_instance_identifier=f'dms-{common.env}-replication-instance',
            vpc_security_group_ids=[
                self.dms_sg.security_group_id
            ],
            replication_subnet_group_identifier=self.dms_subnet_group.replication_subnet_group_identifier

        )

        super().__init__(
            scope,
            f'{common.env}-dms-task-orders-rds',
            migration_type='full-load-and-cdc',
            replication_task_identifier=f'{common.env}-dms-task-orders-rds',
            replication_instance_arn=self.instance.ref,
            source_endpoint_arn=self.rds_endpoint.ref,
            target_endpoint_arn=self.s3_endpoint.ref,

            table_mappings=json.dumps(
                {
                    "rules": [
                        {
                            "rule-type": "selection",
                            "rule-id": "1",
                            "rule-name": "1",
                            "object-locator": {
                                "schema-name": "%",
                                "table-name": "%",
                            },
                            "rule-action": "include",
                            "filters": []
                        }
                    ]
                }
            )
        )


class RawIngestion(core.Stack):

    def __init__(self, scope: core.Construct, common: Common, data_lake: DataLake, **kwargs) -> None:
        self.env = common.env
        super().__init__(scope, id=f'{self.env}-data-lake-raw-ingestion', **kwargs)
        name = f'firehose-{self.env}-raw-delivery-stream'
        raw_bucket = data_lake.data_lake_raw_bucket

        kinesis_role = RawKinesisRole(self, environment=common.env, raw_bucket=raw_bucket)

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

        self.atomic_events = firehose.CfnDeliveryStream(
            self,
            id=name,
            delivery_stream_name=name,
            delivery_stream_type='DirectPut',
            extended_s3_destination_configuration=s3_config
        )

        self.dms_replication_task = OrdersDMS(self, common, data_lake)
