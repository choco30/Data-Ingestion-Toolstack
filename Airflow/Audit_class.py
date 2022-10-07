from datetime import timedelta,datetime
from airflow import models
from airflow import DAG
from airflow.contrib.operators import gcs_to_bq
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.operators.python_operator import PythonOperator
import pandas as pd
import numpy as np
from airflow.models import Variable
from google.cloud import bigquery


class audit_class_bq_ds_auditcntrl:

	def audit_start(self,dag_name,task,table,status,load_date,record_count):
		bq_client=bigquery.Client()
		run_id_query = "select cast(coalesce(max(run_id),cast(0 as string)) as INT64) + 1 from `{0}.{1}.{2}`".format("corp-services-krnospbi-svc-dev","bq_ds_auditcntrl","KRONOSPBI_DAG_EXEC_STATUS")
		run_id_query_sel =bq_client.query(run_id_query)
		job_id=""
		for row in run_id_query_sel:
			job_id=str(row[0])
			break  
		insert_query="insert into `{0}.{1}.{2}` Values('{3}','{4}','{5}','{6}',{7},'{8}',current_datetime('UTC+5:30'),NULL,'{9}')".format('corp-services-krnospbi-svc-dev','bq_ds_auditcntrl','KRONOSPBI_DAG_EXEC_STATUS',job_id,dag_name,task,table,record_count,status,load_date)
		result_query=bq_client.query(insert_query)
		result_query.result()

	def audit_end_fail(self,dag_name,task,table,status,load_date):
		bq_client = bigquery.Client()
		run_id_query = "select cast(coalesce(max(run_id),cast(0 as string)) as INT64) from `{0}.{1}.{2}` where DAG_NAME='{3}' and TASK='{4}'and LOAD_DATE='{5}' and table='{6}' ".format("corp-services-krnospbi-svc-dev","bq_ds_auditcntrl","KRONOSPBI_DAG_EXEC_STATUS",dag_name,task,load_date,table)
		run_id_query_sel =bq_client.query(run_id_query)
		job_id=""
		for row in run_id_query_sel:
			job_id=str(row[0])
			break	
		update_query="update  `{0}.{1}.{2}` set END_DATE_TIME=current_datetime('UTC+5:30'),STATUS='{3}' where RUN_ID='{4}' and DAG_NAME='{5}' and TASK='{6}'and LOAD_DATE='{7}' and table='{8}'".format("corp-services-krnospbi-svc-dev","bq_ds_auditcntrl","KRONOSPBI_DAG_EXEC_STATUS",status,job_id,dag_name,task,load_date,table)
		result_query = bq_client.query(update_query)
		result_query.result()
	
	def audit_end_success(self,dag_name,task,table,status,load_date,record_count):
		bq_client = bigquery.Client()
		run_id_query = "select cast(coalesce(max(run_id),cast(0 as string)) as INT64) from `{0}.{1}.{2}` where DAG_NAME='{3}' and TASK='{4}'and LOAD_DATE='{5}' and table='{6}' ".format("corp-services-krnospbi-svc-dev","bq_ds_auditcntrl","KRONOSPBI_DAG_EXEC_STATUS",dag_name,task,load_date,table)
		run_id_query_sel =bq_client.query(run_id_query)
		job_id=""
		for row in run_id_query_sel:
			job_id=str(row[0])
			break
		update_query="update  `{0}.{1}.{2}` set END_DATE_TIME=current_datetime('UTC+5:30'),STATUS='{3}',RECORD_COUNT={9} where RUN_ID='{4}' and DAG_NAME='{5}' and TASK='{6}'and LOAD_DATE='{7}' and table='{8}'".format("corp-services-krnospbi-svc-dev","bq_ds_auditcntrl","KRONOSPBI_DAG_EXEC_STATUS",status,job_id,dag_name,task,load_date,table,record_count)
		result_query =bq_client.query(update_query)
		result_query.result()

