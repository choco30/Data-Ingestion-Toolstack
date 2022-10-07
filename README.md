# Data-Ingestion-Toolstack
This repository contains code base for Extarcting data from the on prem databases and move them to the GCP Enviornment.The sql server to gcs data flow job 
is orchastrated through the extarction airflow dag.

The required dependency before running the job
1.)Installing apache_beam(2.41.0) package as pypi dpendency on airflow<br>
2.)Placing the java-jdk-8,mssql-jdbc-10.21 and ojdbc8 in a bucket.<br>
3.)The dependency installed at run time is downloaded from internet and defined in the setup.py file.


