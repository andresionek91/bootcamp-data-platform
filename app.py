#!/usr/bin/env python3
from aws_cdk import core
from bootcamp_data_platform.data_lake import DataLake
from bootcamp_data_platform.ingestion import RawIngestion
from bootcamp_data_platform.warehouse import DataWarehouse
from common import Common, Environment
from transform import EMRTransform
from catalog import GlueCatalog
import os

environment = Environment[os.environ['ENVIRONMENT']]

app = core.App()
common = Common(app, environment=environment)
data_lake = DataLake(app, environment=environment)
raw_ingestion = RawIngestion(app, data_lake=data_lake, common=common)
glue_catalog = GlueCatalog(app, data_lake=data_lake)
emr_transform = EMRTransform(app, data_lake=data_lake, common=common)
data_warehouse = DataWarehouse(app, data_lake=data_lake, common=common)
app.synth()
