#!/usr/bin/env python3
from enum import Enum
from aws_cdk import core
from aws_cdk import (
    aws_s3 as s3,
)


class Layer(Enum):
    RAW = 'raw'
    PROCESSED = 'processed'
    CURATED = 'curated'


class S3Defaults:

    @staticmethod
    def block_public_access():
        block_public_access = s3.BlockPublicAccess(
            block_public_acls=True,
            block_public_policy=True,
            ignore_public_acls=True,
            restrict_public_buckets=True
        )
        return block_public_access

    @staticmethod
    def encryption():
        encryption = s3.BucketEncryption(s3.BucketEncryption.S3_MANAGED)
        return encryption

    @staticmethod
    def lifecycle_rules(bucket):
        bucket.add_lifecycle_rule(
            abort_incomplete_multipart_upload_after=core.Duration.days(7),
            enabled=True
        )

        bucket.add_lifecycle_rule(
            noncurrent_version_transitions=[
                s3.NoncurrentVersionTransition(
                    storage_class=s3.StorageClass.INFREQUENT_ACCESS,
                    transition_after=core.Duration.days(30)
                ),
                s3.NoncurrentVersionTransition(
                    storage_class=s3.StorageClass.GLACIER,
                    transition_after=core.Duration.days(60)
                )
            ]
        )

        bucket.add_lifecycle_rule(
            noncurrent_version_expiration=core.Duration.days(360)
        )
