#!/usr/bin/env python3
from enum import Enum
from aws_cdk import core
from bootcamp_data_platform.data_lake import DataLake
from bootcamp_data_platform.ingestion import AtomicEventsRawIngestion
from common import Environment
import os

environment = Environment[os.environ['ENVIRONMENT']]

app = core.App()
data_lake = DataLake(app, environment=environment)
AtomicEventsRawIngestion(app, environment=environment, data_lake=data_lake)
app.synth()
