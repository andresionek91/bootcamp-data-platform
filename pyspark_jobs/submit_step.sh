# execute by running ./submit_step.sh
aws s3 cp convert_to_parquet.py s3://s3-belisco-production-emr-logs-bucket/jobs/convert_to_parquet.py;
# Replace cluster id with your actual EMr cluster ID. Get it on EMR console after deploying the cluster.
aws emr add-steps --cluster-id j-3ECS1L31P638J --steps Type=Spark,Name="ParquetConversion",ActionOnFailure=CONTINUE,Args=[--deploy-mode,cluster,--master,yarn-cluster,--conf,spark.yarn.submit.waitAppCompletion=true,s3://s3-belisco-production-emr-logs-bucket/jobs/convert_to_parquet.py];
