# Redshift Spectrum

Para acessar as tabelas do Glue utilizando o Redshift Spectrum, rode a seguinte query:

```
CREATE EXTERNAL SCHEMA glue_data_lake_raw
FROM DATA CATALOG
DATABASE 'glue_belisco_production_data_lake_raw'
REGION 'eu-west-1'
iam_role 'arn:aws:iam::405179727091:role/production-data-warehouse-iamproductionredshiftspe-FHY1X6UIKCGT'
```

Busque o seu iam_role dentro do cloudformation / production-data-warehouse / Resources / iam role