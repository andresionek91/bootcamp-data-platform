#!/usr/bin/env python3

from aws_cdk import core

from bootcamp_data_platform.bootcamp_data_platform_stack import BootcampDataPlatformStack


app = core.App()
BootcampDataPlatformStack(app, "bootcamp-data-platform")

app.synth()
