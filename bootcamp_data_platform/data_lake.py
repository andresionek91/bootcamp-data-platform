from aws_cdk import core
from aws_cdk import (
    aws_s3 as s3,
    aws_glue as glue,
    aws_iam as iam,
    aws_athena as athena
)
from data_lake_core import Layer, S3Defaults
from common import Environment


class DataLakeBucket(s3.Bucket):

    def __init__(self, scope: core.Construct, environment: Environment, layer: Layer, **kwargs) -> None:
        name = f's3-belisco-{environment.value}-data-lake-{layer.value}'
        self.environment = environment
        self.layer = layer

        super().__init__(
            scope,
            name,
            bucket_name=name,
            removal_policy=core.RemovalPolicy.DESTROY,
            block_public_access=S3Defaults.block_public_access(),
            encryption=S3Defaults.encryption(),
            versioned=True,
            **kwargs
        )

        S3Defaults.lifecycle_rules(self)


class DataLakeDatabase(glue.Database):

    def __init__(self, scope: core.Construct, bucket: DataLakeBucket, **kwargs) -> None:
        name = f'glue-belisco-{bucket.environment.value}-data-lake-{bucket.layer.value}'

        super().__init__(
            scope,
            name,
            database_name=name,
            location_uri=f's3://{bucket.bucket_name}'
        )


class GlueDataLakeRole(iam.Role):

    def __init__(self, scope: core.Construct, environment: Environment, buckets: dict, **kwargs) -> None:
        self.environment = environment.value
        super().__init__(
            scope,
            id=f'iam-{self.environment}-glue-data-lake-role',
            assumed_by=iam.ServicePrincipal('glue.amazonaws.com'),
            description='Allows using Glue on Data Lake',
        )
        self.buckets_arns = [bucket.bucket_arn for layer, bucket in buckets.items()]
        self.add_policy()
        self.add_instance_profile()

    def add_policy(self):
        policy = iam.Policy(
            self,
            id=f'iam-{self.environment}-glue-data-lake-policy',
            policy_name=f'iam-{self.environment}-glue-data-lake-policy',
            statements=[
                iam.PolicyStatement(
                    actions=[
                        's3:ListBucket',
                        's3:GetObject',
                        's3:PutObject'
                    ],
                    resources=self.buckets_arns + [f'{arn}/*' for arn in self.buckets_arns]
                ),
                iam.PolicyStatement(
                    actions=[
                        'cloudwatch:PutMetricData'
                    ],
                    resources=[
                        'arn:aws:cloudwatch:*'
                    ]
                ),
                iam.PolicyStatement(
                    actions=[
                        'glue:*'
                    ],
                    resources=[
                        'arn:aws:glue:*'
                    ]
                ),
                iam.PolicyStatement(
                    actions=[
                        'logs:CreateLogGroup',
                        'logs:CreateLogStream',
                        'logs:PutLogEvents'
                    ],
                    resources=[
                        'arn:aws:logs:*:*:/aws-glue/*'
                    ]
                ),
            ]
        )
        self.attach_inline_policy(policy)

        return policy

    def add_instance_profile(self):
        instance_profile = iam.CfnInstanceProfile(
            self,
            id=f'iam-{self.environment}-glue-data-lake-instance-profile',
            instance_profile_name=f'iam-{self.environment}-glue-data-lake-instance-profile',
            roles=[self.role_name]
        )
        return instance_profile


class AthenaBucket(s3.Bucket):

    def __init__(self, scope: core.Construct, environment: Environment, **kwargs) -> None:
        name = f's3-belisco-{environment.value}-data-lake-athena-results'
        self.environment = environment

        super().__init__(
            scope,
            id=name,
            bucket_name=name,
            removal_policy=core.RemovalPolicy.DESTROY,
            block_public_access=S3Defaults.block_public_access(),
            encryption=S3Defaults.encryption(),
            versioned=True,
            **kwargs
        )

        self.add_lifecycle_rule(
            expiration=core.Duration.days(60)
        )


class AthenaWorkgroup(athena.CfnWorkGroup):

    def __init__(self, scope: core.Construct, environment: Environment, bucket: AthenaBucket, **kwargs) -> None:
        name = f's3-belisco-{environment.value}-data-lake-athena-workgroup'
        self.environment = environment
        self.bucket = bucket

        super().__init__(
            scope,
            id=name,
            name=name,
            description='Workgroup padrao para execucao de queries',
            recursive_delete_option=True,
            state='ENABLED',
            work_group_configuration=self.workgroup_configuration(),
            **kwargs
        )

    def result_configuration(self):
        result_config = athena.CfnWorkGroup.ResultConfigurationProperty(
            encryption_configuration=athena.CfnWorkGroup.EncryptionConfigurationProperty(encryption_option='SSE_S3'),
            output_location=f's3://{self.bucket.bucket_name}'
        )
        return result_config

    def workgroup_configuration(self):
        workgroup_config = athena.CfnWorkGroup.WorkGroupConfigurationProperty(
            bytes_scanned_cutoff_per_query=1000000000,
            enforce_work_group_configuration=True,
            publish_cloud_watch_metrics_enabled=True,
            result_configuration=self.result_configuration()
        )
        return workgroup_config


class DataLake(core.Stack):

    def __init__(self, scope: core.Construct, environment: Environment, **kwargs) -> None:
        super().__init__(scope, id='data-lake', **kwargs)
        self.buckets = {}
        self.databases = {}
        self.env = environment

        self.data_lake_raw()
        self.data_lake_processed()
        self.data_lake_curated()
        self.data_lake_role()
        self.athena()

    def data_lake_raw(self):
        bucket = DataLakeBucket(
            self,
            environment=self.env,
            layer=Layer.RAW
        )
        bucket.add_lifecycle_rule(
            transitions=[
                s3.Transition(
                    storage_class=s3.StorageClass.INTELLIGENT_TIERING,
                    transition_after=core.Duration.days(90)
                ),
                s3.Transition(
                    storage_class=s3.StorageClass.GLACIER,
                    transition_after=core.Duration.days(360)
                )
            ],
            enabled=True)

        database = DataLakeDatabase(self, bucket=bucket)

        self.buckets[Layer.RAW] = bucket
        self.databases[Layer.RAW] = database

        return bucket, database

    def data_lake_processed(self):
        bucket = DataLakeBucket(
            self,
            environment=self.env,
            layer=Layer.PROCESSED
        )

        database = DataLakeDatabase(self, bucket=bucket)

        self.buckets[Layer.PROCESSED] = bucket
        self.databases[Layer.PROCESSED] = database

        return bucket, database

    def data_lake_curated(self):
        bucket = DataLakeBucket(
            self,
            environment=self.env,
            layer=Layer.CURATED
        )

        database = DataLakeDatabase(self, bucket=bucket)

        self.buckets[Layer.CURATED] = bucket
        self.databases[Layer.CURATED] = database

        return bucket, database

    def data_lake_role(self):
        role = GlueDataLakeRole(
            self,
            environment=self.env,
            buckets=self.buckets
        )

        return role

    def athena(self):
        bucket = AthenaBucket(
            self,
            environment=self.env,
        )

        workgroup = AthenaWorkgroup(
            self,
            environment=self.env,
            bucket=bucket
        )

        return bucket, workgroup
