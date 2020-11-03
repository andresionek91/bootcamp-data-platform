from aws_cdk import core
from data_lake import DataLake
from aws_cdk import (
    aws_redshift as redshift,
    aws_ec2 as ec2,
    aws_iam as iam
)
from common import Common


class SpectrumRole(iam.Role):

    def __init__(self, scope: core.Construct, data_lake: DataLake, **kwargs) -> None:
        self.environment = data_lake.env.value
        super().__init__(
            scope,
            id=f'iam-{self.environment}-redshift-spectrum-role',
            assumed_by=iam.ServicePrincipal('redshift.amazonaws.com'),
            description='Role to allow Redshift to access data lake using spectrum',
        )

        buckets_arns = [
            data_lake.data_lake_raw_bucket.bucket_arn,
            data_lake.data_lake_processed_bucket.bucket_arn,
            data_lake.data_lake_curated_bucket.bucket_arn
        ]

        policy = iam.Policy(
            scope,
            id=f'iam-{self.environment}-redshift-spectrum-policy',
            policy_name=f'iam-{self.environment}-redshift-spectrum-policy',
            statements=[
                iam.PolicyStatement(
                    actions=[
                        'glue:*',
                        'athena:*'
                    ],
                    resources=[
                        "*"
                    ]
                ),
                iam.PolicyStatement(
                    actions=[

                        's3:Get*',
                        's3:List*',
                        's3:Put*'
                    ],
                    resources=buckets_arns + [f'{arn}/*' for arn in buckets_arns]
                )
            ]
        )
        self.attach_inline_policy(policy)


class DataWarehouse(core.Stack):

    def __init__(self, scope: core.Construct, common: Common, data_lake: DataLake, **kwargs) -> None:
        self.env = common.env
        super().__init__(scope, id=f'{self.env}-data-warehouse', **kwargs)

        self.redshift_sg = ec2.SecurityGroup(
            self,
            f'orders-{self.env}-sg',
            vpc=self.custom_vpc,
            allow_all_outbound=True,
            security_group_name=f'orders-{self.env}-sg',
        )

        self.redshift_sg.add_ingress_rule(
            peer=ec2.Peer.ipv4('37.156.75.55/32'),
            connection=ec2.Port.tcp(5439)
        )

        for subnet in common.custom_vpc.private_subnets:
            self.redshift_sg.add_ingress_rule(
                peer=ec2.Peer.ipv4(subnet.ipv4_cidr_block),
                connection=ec2.Port.tcp(5439)
            )

        self.redshift_cluster = redshift.Cluster(
            self,
            f'belisco-{self.env}-redshift',
            cluster_name=f'belisco-{self.env}-redshift',
            vpc=common.custom_vpc,
            cluster_type=redshift.ClusterType.MULTI_NODE,
            node_type=redshift.NodeType.DC2_LARGE,
            default_database_name='dw',
            number_of_nodes=2,
            removal_policy=core.RemovalPolicy.DESTROY,
            master_user=redshift.Login(
                master_username='admin'
            ),
            publicly_accessible=True,
            roles=[
                SpectrumRole(
                    self,
                    data_lake
                )
            ],
            security_groups=common.orders_rds_sg,
            vpc_subnets=ec2.SubnetSelection(subnet_type=ec2.SubnetType.PUBLIC)
        )
