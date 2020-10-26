from enum import Enum

from aws_cdk import core
from aws_cdk import (
    aws_s3 as s3,
    aws_glue as glue,
    aws_iam as iam,
    aws_athena as athena
)


class Environment(Enum):
    PRODUCTION = 'production'
    STAGING = 'staging'
    DEV = 'dev'