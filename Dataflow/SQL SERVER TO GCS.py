#Java dk 8, mssql-jdbc-10.21 is required to setup and need to be paced into a gcs bucket which can be collectively installed as atar file at run time

#Jaydebeapi,pandas,gcsfs,sqlalchemy are the dependencied needs to be installed at run time on the worker nodes>the dependency are defined in aform of setup .py file which is passed on time of running the code

# The code has been successfully tested on apache beam version 2.39(by manually limiting the PROTOBUF dependency to 3.20) and 2.41


import apache_beam as beam
#import time
#import jaydebeapi 
import os
import argparse
#from google.cloud import bigquery
import logging
#import sys
#from google.cloud import storage as gstorage
import pandas as  pd
from oauth2client.client import GoogleCredentials
from datetime import datetime,date,timedelta
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions

class setenv(beam.DoFn): 
      def process(self,context,df_Bucket):
          import jaydebeapi
          import pandas as pd
          src1='gs://'+df_Bucket+'/JAVA_JDK_AND_JAR'
          os.system('gsutil cp '+src1+'/mssql-jdbc-10.2.1.jre8.jar /tmp/' +'&&'+ 'gsutil cp -r '+src1+'/jdk-8u202-linux-x64.tar.gz /tmp/')
          logging.info('Jar copied to Instance..')
          logging.info('Java Libraries copied to Instance..')
          os.system('mkdir -p /usr/lib/jvm  && tar zxvf /tmp/jdk-8u202-linux-x64.tar.gz -C /usr/lib/jvm  && update-alternatives --install "/usr/bin/java" "java" "/usr/lib/jvm/jdk1.8.0_202/bin/java" 1 && update-alternatives --config java')
          logging.info('Enviornment Variable set.')
          return list("1")
          

class readandwrite(beam.DoFn): 
      def process(self, context, conn_Detail, sql_readQuery, target_Bucket, src_TableName,Load_date):
          import jaydebeapi
          import pandas as pd
          from datetime import datetime,date,timedelta
          today_date=(datetime.now()+timedelta(hours=5,minutes=30)).strftime("%Y-%m-%d")
          
          DatabaseConn=conn_Detail.split("~|*")
          database_user=DatabaseConn[0]
          database_password=DatabaseConn[1]
          database_host=DatabaseConn[2]
          database_port=DatabaseConn[3]
          database_db=DatabaseConn[4]
          
          query = sql_readQuery
          
          jclassname = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
          url = ("jdbc:sqlserver://"+database_host+":"+database_port+";databaseName="+database_db+";encrypt=false")
          logging.info(url)
          jars = "/tmp/mssql-jdbc-10.2.1.jre8.jar"
          libs = None
          cnx = jaydebeapi.connect(jclassname,url,{'user':database_user,'password':database_password},jars=jars)  
          
          logging.info('Connection Successful..') 
          cursor = cnx.cursor()
          logging.info('Query submitted to SQL Server Database..')
          sql_query = pd.read_sql(query, cnx)
          
    
          logging.info('printing data')
          df = pd.DataFrame(sql_query,index=None)
          tgt_file_location = "gs://"+target_Bucket+"/01/DBO/source_csv/"+Load_date+"/"+src_TableName+".csv"
          df.to_csv(tgt_file_location, index=False, date_format='%Y-%m-%d %H:%M:%E*S')

   

def run():    
       
    try: 
        parser = argparse.ArgumentParser()
        parser.add_argument(
            '--sqlQuery',
            required=True,
            help= ('SQL Query to fetch data')
            )
        parser.add_argument(
            '--srcTableName',
            required=True,
            help= ('Source table name')
            )
        parser.add_argument(
            '--targetBucket',
            required=True,
            help= ('Bucket where src_data is dumped')
            )
        parser.add_argument(
            '--dfBucket',
            required=True,
            help= ('Bucket where JARS/JDK is present')
            )
        parser.add_argument(
            '--connDetail',
            required=True,
            help= ('Source Database Connection Detail')
            )
        parser.add_argument(
            '--Loaddate',
            required=True,
            help= ('LOad_date For partitioning Data in Gcs Bucket')
            )
               
        known_args, pipeline_args = parser.parse_known_args()
        
        global sql_readQuery
        sql_readQuery = known_args.sqlQuery
        global src_TableName
        src_TableName = known_args.srcTableName
        global target_Bucket
        target_Bucket = known_args.targetBucket
        global df_Bucket 
        df_Bucket = known_args.dfBucket
        global conn_Detail 
        conn_Detail = known_args.connDetail
        global Load_date 
        Load_date = known_args.Loaddate
        
        pipeline_options = PipelineOptions(pipeline_args)
        pcoll = beam.Pipeline(options=pipeline_options)
        
        logging.info("Pipeline Starts")
        dummy= pcoll | 'Initializing..' >> beam.Create(['1'])
        dummy_env = dummy | 'Setting up Instance..' >> beam.ParDo(setenv(),df_Bucket)
        readrecords=(dummy_env | 'Processing' >>  beam.ParDo(readandwrite(), conn_Detail, sql_readQuery, target_Bucket, src_TableName,Load_date))
        #Write_To_GCS = (readrecords | 'WriteToGCS' >>  beam.io.WriteToText("gs://gcp01-sb-krnospbi-dataflow-bucket/source_data/CMSPRINCIPALEMPLOYER.txt"))
        p=pcoll.run()
        logging.info('Job Run Successfully!')
        p.wait_until_finish()
    except:
        logging.exception('Failed to launch datapipeline')
        raise    


if __name__ == '__main__':
    #parser = argparse.ArgumentParser(description=__doc__ , formatter_class=argparse.RawDescriptionHelpFormatter)
     run()
