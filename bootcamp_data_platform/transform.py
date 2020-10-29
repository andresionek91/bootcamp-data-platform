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
            bucket_name=f's3-belisco-{self.env}-emr-logs-bucket'
        )

        self.cluster = emr.CfnCluster(
            self,
            f'{self.env}-emr-cluster',
            name=f'{self.env}-emr-cluster',
            instances=[
                emr.CfnCluster.JobFlowInstancesConfigProperty(
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
                    termination_protected=True,
                    ec2_subnet_ids=common.custom_vpc.private_subnets
                )
            ],
            applications=[
                emr.CfnCluster.ApplicationProperty(name='Spark')
            ],
            log_uri=f's3://{self.logs_bucket.bucket_name}/logs',
            job_flow_role=,
            service_role=,
            release_label='emr-5.30.1',
            visible_to_all_users=True
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
 #
 #  emrRole:
 #    Type: AWS::IAM::Role
 #    Properties:
 #      AssumeRolePolicyDocument:
 #        Version: 2008-10-17
 #        Statement:
 #          - Sid: ''
 #            Effect: Allow
 #            Principal:
 #              Service: elasticmapreduce.amazonaws.com
 #            Action: 'sts:AssumeRole'
 #      Path: /
 #      ManagedPolicyArns:
 #        - 'arn:aws:iam::aws:policy/service-role/AmazonElasticMapReduceRole'
 #
 #  emrEc2Role:
 #    Type: AWS::IAM::Role
 #    Properties:
 #      AssumeRolePolicyDocument:
 #        Version: 2008-10-17
 #        Statement:
 #          - Sid: ''
 #            Effect: Allow
 #            Principal:
 #              Service: ec2.amazonaws.com
 #            Action: 'sts:AssumeRole'
 #      Path: /
 #      ManagedPolicyArns:
 #        - 'arn:aws:iam::aws:policy/service-role/AmazonElasticMapReduceforEC2Role'
 #
 #  emrEc2InstanceProfile:
 #    Type: AWS::IAM::InstanceProfile
 #    Properties:
 #      Path: /
 #      Roles:
 #        - !Ref emrEc2Role