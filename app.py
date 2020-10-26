#!/usr/bin/env python3
from enum import Enum
from aws_cdk import core
from bootcamp_data_platform.data_lake import DataLake
from bootcamp_data_platform.ingestion import AtomicEventsRawIngestion


class Environment(Enum):
    PRODUCTION = 'production'
    STAGING = 'staging'
    DEV = 'dev'


app = core.App()
data_lake = DataLake(app, environment=Environment.PRODUCTION)
AtomicEventsRawIngestion(app, environment=Environment.PRODUCTION, data_lake=data_lake)
app.synth()
