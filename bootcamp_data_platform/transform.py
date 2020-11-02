from aws_cdk import core
from data_lake import DataLake
from common import Common
from aws_cdk import (
    aws_emr as emr,
    aws_s3 as s3,
    aws_iam as iam
)


class EMRTransform(core.Stack):

    def __init__(self, scope: core.Construct, data_lake: DataLake, common: Common, **kwargs) -> None:
        self.env = data_lake.env.value
        super().__init__(scope, id=f'{self.env}-emr-transform', **kwargs)

        self.logs_bucket = s3.Bucket(
            self,
            f'{self.env}-emr-logs-bucket',
            bucket_name=f's3-belisco-{self.env}-emr-logs-bucket',
            removal_policy=core.RemovalPolicy.DESTROY
        )

        buckets_arns = [
            data_lake.data_lake_raw_bucket.bucket_arn,
            data_lake.data_lake_processed_bucket.bucket_arn,
            data_lake.data_lake_curated_bucket.bucket_arn
        ]

        self.datalake_emr_policy = iam.Policy(
            self,
            id=f'iam-{self.env}-emr-data-lake',
            policy_name=f'iam-{self.env}-emr-data-lake',
            statements=[
                iam.PolicyStatement(
                    actions=[
                        's3:*',
                    ],
                    resources=buckets_arns + [f'{arn}/*' for arn in buckets_arns]
                )
            ]
        )

        self.emr_role = iam.Role(
            self,
            f'{self.env}-emr-cluster-role',
            assumed_by=iam.ServicePrincipal('elasticmapreduce.amazonaws.com'),
            description='Role to allow EMR to process data',
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name('service-role/AmazonElasticMapReduceRole')
            ]
        )

        self.emr_role.attach_inline_policy(self.datalake_emr_policy)

        self.emr_ec2_role = iam.Role(
            self,
            f'{self.env}-emr-ec2-role',
            assumed_by=iam.ServicePrincipal('ec2.amazonaws.com'),
            description='Role to allow EMR to process data',
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name('service-role/AmazonElasticMapReduceforEC2Role')
            ]
        )

        self.emr_ec2_role.attach_inline_policy(self.datalake_emr_policy)

        self.emr_ec2_instance_profile = iam.CfnInstanceProfile(
            self,
            f'{self.env}-emr-instance_profile',
            instance_profile_name=f'{self.env}-emr-instance_profile',
            roles=[
                self.emr_ec2_role.role_name
            ]
        )

        self.cluster = emr.CfnCluster(
            self,
            f'{self.env}-emr-cluster',
            name=f'{self.env}-emr-cluster',
            instances=emr.CfnCluster.JobFlowInstancesConfigProperty(
                master_instance_group=emr.CfnCluster.InstanceGroupConfigProperty(
                    instance_count=1,
                    instance_type='m4.large',
                    market='ON_DEMAND',
                    name='Master'
                ),
                core_instance_group=emr.CfnCluster.InstanceGroupConfigProperty(
                    instance_count=2,
                    instance_type='m4.large',
                    market='ON_DEMAND',
                    name='Core'
                ),
                termination_protected=False,
                ec2_subnet_id=common.custom_vpc.private_subnets[0].subnet_id
            ),
            applications=[
                emr.CfnCluster.ApplicationProperty(name='Spark')
            ],
            log_uri=f's3://{self.logs_bucket.bucket_name}/logs',
            job_flow_role=self.emr_ec2_instance_profile.get_att('Arn').to_string(),
            service_role=self.emr_role.role_arn,
            release_label='emr-5.30.1',
            visible_to_all_users=True,
            configurations=[
                emr.CfnCluster.ConfigurationProperty(
                    classification='spark-hive-site',
                    configuration_properties={
                        "hive.metastore.client.factory.class": "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"
                    }
                )
            ]
        )
