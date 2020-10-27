from enum import Enum

from aws_cdk import core
from aws_cdk import (
    aws_rds as rds,
    aws_ec2 as ec2
)


class Environment(Enum):
    PRODUCTION = 'production'
    STAGING = 'staging'
    DEV = 'dev'


class Common(core.Stack):

    def __init__(self, scope: core.Construct, environment: Environment, **kwargs) -> None:
        self.env = environment.value
        super().__init__(scope, id=f'{self.env}-common', **kwargs)

        self.custom_vpc = ec2.Vpc(
            self,
            f'vpc-{self.env}'
        )

        self.orders_rds_sg = ec2.SecurityGroup(
            self,
            f'orders-{self.env}-sg',
            vpc=self.custom_vpc,
            allow_all_outbound=True,
            security_group_name=f'orders-{self.env}-sg',
        )

        self.orders_rds_sg.add_ingress_rule(
            peer=ec2.Peer.ipv4('37.156.75.55/32'),
            connection=ec2.Port.tcp(5432)
        )

        for subnet in self.custom_vpc.private_subnets:
            self.orders_rds_sg.add_ingress_rule(
                peer=ec2.Peer.ipv4(subnet.ipv4_cidr_block),
                connection=ec2.Port.tcp(5432)
            )

        self.orders_rds = rds.DatabaseInstance(
            self,
            f'orders-{self.env}-rds',
            engine=rds.DatabaseInstanceEngine.POSTGRES,
            database_name='orders',
            instance_type=ec2.InstanceType('t3.micro'),
            vpc=self.custom_vpc,
            instance_identifier=f'rds-{self.env}-orders-db',
            port=5432,
            subnet_group=rds.SubnetGroup(
                self,
                f'rds-{self.env}-replication-subnet',
                description='place RDS on private subnet',
                vpc=self.custom_vpc
            ),
            security_groups=[
                self.orders_rds_sg
            ],
            removal_policy=core.RemovalPolicy.DESTROY,
            **kwargs
        )

