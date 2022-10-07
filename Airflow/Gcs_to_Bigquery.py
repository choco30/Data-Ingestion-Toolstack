from datetime import timedelta,datetime,date,timedelta
from airflow import models
from airflow import DAG
from airflow.contrib.operators import gcs_to_bq
from airflow.operators.python_operator import PythonOperator 
from airflow.models import Variable
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from google.cloud import bigquery
from audit_class import audit_class_bq_ds_auditcntrl



common_config=Variable.get("common_config", deserialize_json=True)
table_list=common_config["table_list"]
BQ_PROJECT=common_config['BQ_PROJECT']
BQ_landing_dataset=common_config['BQ_landing_dataset']
source_bucket=common_config["source_bucket"]
BQ_stg_dataset=common_config['BQ_stg_dataset']


DEFAULT_ARGS = {
    'depends_on_past': False,
    'start_date': datetime(2022,6,5),
    'catchup': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=1),
}

dag= DAG(
	'gcs_to_bq_kronos',
	catchup=False,
	default_args=DEFAULT_ARGS,
	schedule_interval=None
	)
    
def paramter_reciever(**kwargs):
    load_date=str(kwargs['dag_run'].conf["load_date"])
    print(load_date)
    return(str(load_date))

parameter_reciever_task=PythonOperator(
    task_id="parameter_reciever_task",
    python_callable=paramter_reciever,
    provide_context=True,
    dag=dag
    )
partition_date="{{task_instance.xcom_pull(task_ids='parameter_reciever_task')}}"
TriggerDag=TriggerDagRunOperator(
    task_id="trigger_Stg_dag",
    trigger_dag_id="Landing_To_STG_load",
    conf={"load_date":partition_date},
    dag=dag)
    
for table in table_list:
    file_object='01/DBO/source_csv/'+"{{task_instance.xcom_pull(task_ids='parameter_reciever_task')}}"+'/'+table+'.csv'
    file_schema='01/DBO/source_json/'+table+'.json'
    table_name="KRN_01_DBO_"+table
    def start_func(**kwargs):
        table=str(kwargs['table'])
        load_date=kwargs["load_date"]
        status=str(kwargs['status'])
        ti=kwargs['task_instance']
        audit_class_bq_ds_auditcntrl().audit_start("gcs_to_bq_kronos","LANDING",table,status,load_date,'NULL')
        
    def success_func(**kwargs):
        table=kwargs['table']
        load_date=kwargs["load_date"]
        status=kwargs['status']
        bq_client=bigquery.Client()
        query="select count(*) from `{0}.{1}.{2}`".format(BQ_PROJECT,BQ_landing_dataset,table)
        record_count_query =bq_client.query(query)
        record_count=0
        print("{{task_instance.xcom_pull(task_ids='parameter_reciever_task')}}",record_count,load_date)
        for row in record_count_query:
            record_count=row[0]
            break
        audit_class_bq_ds_auditcntrl().audit_end_success("gcs_to_bq_kronos","LANDING",table,status,load_date,record_count)
        
    def fail_func(**kwargs):
        table=kwargs['table']
        status=kwargs['status']
        load_date=kwargs["load_date"]
        ti=kwargs['task_instance']
        audit_class_bq_ds_auditcntrl().audit_end_fail("gcs_to_bq_kronos","LANDING",table,status,load_date)        
        
    Audit_START_Task=PythonOperator(
        task_id=table+"_Audit_START_Task",
        python_callable=start_func,
        op_kwargs={'table':table_name,'status':'STARTED','load_date':partition_date},
        provide_context=True,
        dag=dag)
        
    Audit_SUCCESS_Task=PythonOperator(
        task_id=table+"_Audit_SUCCESS_Task",
        python_callable=success_func,
        op_kwargs={'table':table_name,'status':"SUCCESS",'load_date':partition_date},
        provide_context=True,
        trigger_rule='all_success',
        dag=dag)
        
    Audit_FAIL_Task=PythonOperator(
        task_id=table+"_Audit_FAIL_Task",
        python_callable=fail_func,
        op_kwargs={'table':table_name,'status':"FAILED",'load_date':partition_date},
        provide_context=True,
        trigger_rule='all_failed',
        dag=dag)    
        
    load_task = gcs_to_bq.GoogleCloudStorageToBigQueryOperator(
        task_id=table+'_Load',
        bucket=source_bucket,
        source_objects=[file_object],
        destination_project_dataset_table=BQ_PROJECT+"."+BQ_landing_dataset+"."+table_name,
        skip_leading_rows = 1,
        schema_object = file_schema,
        allow_quoted_newlines=True,
        write_disposition = 'WRITE_TRUNCATE',
        dag=dag)
    
    parameter_reciever_task>>Audit_START_Task>>load_task>>[Audit_FAIL_Task,Audit_SUCCESS_Task,TriggerDag]
