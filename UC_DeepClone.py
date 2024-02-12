# Databricks notebook source
import re, json,time,sys,traceback,os
from delta.tables import *
from pyspark.sql.functions import *
from datetime import datetime
from datetime import datetime,timedelta,date
from dateutil.relativedelta import relativedelta
from pytz import timezone
from delta.tables import *
from delta.exceptions import ConcurrentAppendException,ConcurrentWriteException,ProtocolChangedException,ConcurrentDeleteDeleteException,ConcurrentDeleteReadException
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import as_completed
from random import random
from threading import Lock
from time import sleep

# COMMAND ----------

spark.conf.set("spark.databricks.delta.retryWriteConflict.enabled",True)
spark.conf.set("spark.databricks.delta.retryWriteConflict.limit",20)  

# COMMAND ----------

spark.conf.get("spark.databricks.delta.retryWriteConflict.enabled")
spark.conf.get("spark.databricks.delta.retryWriteConflict.limit")  

# COMMAND ----------

# dbutils.widgets.text("InputParam", "{}","InputParam")
dbutils.widgets.text("ConfigParams", "{}","ConfigParams")
# dbutils.widgets.text("SubjectArea", "{}","SubjectArea")
dbutils.widgets.text("Schema", "","Schema")
dbutils.widgets.text("TablesToClone", "","TablesToClone")
dbutils.widgets.text("RunDate", "","RunDate")
# dbutils.widgets.text("SourceStorageAccount", "","SourceStorageAccount")
dbutils.widgets.text("TargetStorageContainer", "","TargetStorageContainer")
dbutils.widgets.text("TypeOfData", "","TypeOfData")

# COMMAND ----------

getArgument('ConfigParams')

# COMMAND ----------

# MAGIC %run /data_framework/release/di_framework/common_utility

# COMMAND ----------

import re
env_perm = re.sub("[^A-Z]", "", dbutils.secrets.get(scope = "cdo-datafdn-de-scope", key = "cdo-datafdn-environment") ,0,re.IGNORECASE)
set_job_view_permission(reader_list_group="cdo-"+env_perm+"-datafdn-de01-users")
set_job_manage_permission()
print(env_perm)

# COMMAND ----------

try:
  # fix_json_missing_quotes= re.sub(":\s*(,|})",":\"\"\\1",getArgument('InputParam'))
  # widgetParams=json.loads(fix_json_missing_quotes)
  ################ ConfigParams ################
  fix_json_missing_quotes_configparams= re.sub(":\s*(,|})",":\"\"\\1",getArgument('ConfigParams'))
  widgetParams_configparams=json.loads(fix_json_missing_quotes_configparams)
  print(widgetParams_configparams)
  Schema                   = getArgument('Schema')
  TablesToClone            = getArgument('TablesToClone')
  RunDate                  = getArgument('RunDate')
  # SourceStorageAccount     = getArgument('SourceStorageAccount')
  TargetStorageContainer   = getArgument('TargetStorageContainer')
  TypeOfData = getArgument('TypeOfData')

  auditsql_server   = widgetParams_configparams.get('sql_servername')
  auditsql_username = widgetParams_configparams.get('sql_server_username')
  auditsql_database = widgetParams_configparams.get('auditsql_database')
  ScopeName                = widgetParams_configparams.get('ScopeName')
  SPClientID               = widgetParams_configparams.get('kv_de_spn_client_id')
  SPDirectoryID            = widgetParams_configparams.get('kv_de_spn_tenant_id')
  KeyVaultSPSecret         = widgetParams_configparams.get('kv_de_spn_secret')
  pw=dbutils.secrets.get(scope=ScopeName,key=widgetParams_configparams.get('kv_sql_server_password'))
except Exception as e:
  raise e

# COMMAND ----------

if Schema=='frmwrkfdn':
  database_generation=['_bronze_raw_dev','_bronze_dev','_bronze_raw_dqerror_dev','_bronze_dqerror_dev']
else:
  database_generation=['_bronze_raw','_bronze','_bronze_dqerror','_bronze_raw_dqerror']
Security_Classification=['public','restricted','confidential','internal']

##ADD DF in the Business Group Where Condition

##SQL to get Distinct sector names for which Non-Enterprise DataAssets Exists
SQLquery_Get_SectorNames_AdlsContainerNames = f"""select sm.SectorName, dac.AdlsContainerName, da.BusinessGroupId from {auditsql_username}.DataAsset da join {auditsql_username}.DataAssetConfig dac on da.Id=dac.DataAssetId join {auditsql_username}.SectorMaster sm on da.SectorId=sm.Id join {auditsql_username}.BusinessGroup bg on da.BusinessGroupId=bg.id join {auditsql_username}.TargetMaster tm on da.TargetId=tm.Id where tm.TypeOfData='{TypeOfData}' and da.BusinessGroupId NOT IN (SELECT Id FROM {auditsql_username}.BusinessGroup WHERE Code='DF') group by sm.SectorName, dac.AdlsContainerName, da.BusinessGroupId"""

df_distinct_SectorNames_AdlsContainerNames  = spark.read.format("sqlserver").option("host", auditsql_server).option("user", auditsql_username).option("password", pw).option("database", auditsql_database).option("query", SQLquery_Get_SectorNames_AdlsContainerNames).load()

df_distinct_SectorNames_AdlsContainerNames.display()

distinct_sector_names_list = df_distinct_SectorNames_AdlsContainerNames.select("SectorName").distinct().rdd.flatMap(lambda x: x).collect()
distinct_adlscontainer_names_list = df_distinct_SectorNames_AdlsContainerNames.select("AdlsContainerName").distinct().rdd.flatMap(lambda x: x).collect()

SectorList = [SectorNames.replace('-','_').lower() for SectorNames in distinct_sector_names_list]
print("SectorList: {}".format(SectorList))
AdlsContainerNames = [AdlsContainer.replace('-','_') for AdlsContainer in distinct_adlscontainer_names_list]
print("AdlsContainerNames: {}".format(AdlsContainerNames))

DB_List=[]

#If Non-Enterprise Target Type 
for dbname in database_generation:
    for sector in SectorList:
      for adlsnames in AdlsContainerNames:
        DB_List.append('fdn'+'_'+sector+'_'+adlsnames+dbname)

# COMMAND ----------

def get_db_tbl_list(db):
  total_db_list= [x["databaseName"] for x in spark.sql("""SHOW databases""").select("databaseName").collect()]
  tbl_list = []
  if db in  total_db_list:
    tbl_obj_list=spark.sql(f'show tables from {db}').collect()
    if tbl_obj_list:
      for tbl_obj in tbl_obj_list:
        if "bronze" in db.lower():
          if (tbl_obj.tableName.lower().startswith("tbl_")) and (not tbl_obj.isTemporary):
            tbl_list.append(f"{db}.{tbl_obj.tableName}")
        elif not tbl_obj.isTemporary:
          tbl_list.append(f"{db}.{tbl_obj.tableName}")
    else:
      print(f"db present but no tables are there under database {db} ")
  else: 
    print(f"Database '{db}' not found")
  return tbl_list

def get_delta_tbl_detail(tbl):
  try:
    tbl_detail_df = spark.sql(f"DESCRIBE DETAIL {tbl}")
    tbl_detail_df_collect = tbl_detail_df.select(["location","format"]).collect()
    tbl_location,tbl_format = tbl_detail_df_collect[0]["location"],tbl_detail_df_collect[0]["format"]
    exception=''
  except Exception as e:
    exception=str(e)
    tbl_location=''
    tbl_format=''
  return tbl_location,tbl_format,exception

def deep_clone_tbl(tbl,src_loc,target_container, SPClientID, SPDirectoryID, ScopeName, KeyVaultSPSecret):
  source_container_name=src_loc.split('@')[0].split('//')[1]
  storage_account_name=src_loc.split('.')[0].split('@')[1]
  accessAdlsStorage(storage_account_name, SPClientID, SPDirectoryID, ScopeName, KeyVaultSPSecret)
  remaining_path=src_loc.split('@')[1].split('.net')[1]

  if DeltaTable.isDeltaTable(spark,"{}".format(src_loc)):
      mapping=True
      try:
        spark.conf.set("fs.azure.createRemoteFileSystemDuringInitialization", "true")
        dbutils.fs.mkdirs(f"abfss://{target_container}@{storage_account_name}.dfs.core.windows.net/")
        spark.conf.set("fs.azure.createRemoteFileSystemDuringInitialization", "false")
        dbutils.fs.ls(f"abfss://{target_container}@{storage_account_name}.dfs.core.windows.net/")
      except Exception as e:
        if 'java.io.FileNotFoundException' in str(e):
          raise Exception(f'The {target_container} Container is not present in the target side')  
      tgt_loc=f"abfss://{target_container}@{storage_account_name}.dfs.core.windows.net/{source_container_name}{remaining_path}"
      deep_clone_query=f'CREATE OR REPLACE TABLE delta.`{tgt_loc}` CLONE delta.`{src_loc}`'
  else:
    deep_clone_query=''
    mapping=False
    src_loc=''
    tgt_loc=''
  return source_container_name,mapping,deep_clone_query,src_loc,tgt_loc

def get_delta_db_detail(db):
    try:
      db_detail=spark.sql(f"desc database {db}") \
      .where(col("database_description_item")=='Location').collect()[0]['database_description_value']
      exception=''
    except Exception as e:
      exception=str(e)
      print(exception)
      db_detail=''
    return db_detail

# COMMAND ----------

db_count=len(DB_List)
for db in DB_List:
    try:
        tbl_list = get_db_tbl_list(db)
        db_loc=get_delta_db_detail(db)
        print(f"##########################DATABASE--{db_count}#############################")
        print("LOCATION: ",db_loc)
        print("LIST OF TABLES: ",tbl_list)
        for table in tbl_list:
            src_loc,src_tbl_format,exception = get_delta_tbl_detail(table)
            print("-----------------------------")
            print("TABLE LOCATION: ",src_loc)
        db_count-=1
    except Exception as e:  # Catch any errors during database retrieval
        print(f"Database '{db}' not found: {e}")
        print("=================================NOT FOUND=================================")
        continue  # Skip to the next database in the list
    print("==================================================================")

# COMMAND ----------

spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")

# COMMAND ----------

def run_deepclone_parallely(input_list):
    src_loc=''
    tgt_loc=''
    deep_clone_query=''
    src_count=None
    tgt_count=None
    repair=False
    db=input_list[0]
    db_loc=input_list[1]
    tbl=input_list[2]
    TargetStorageContainer=input_list[3]
    SPClientID=input_list[4]
    SPDirectoryID=input_list[5] 
    ScopeName=input_list[6]
    KeyVaultSPSecret=input_list[7]
    print(db_loc, tbl, TargetStorageContainer)
    start_time_two=datetime.now(timezone("US/Central"))

    try:
        src_loc,src_tbl_format,exception = get_delta_tbl_detail(tbl)
        if exception!='':
            raise Exception(exception)
        if src_tbl_format.strip().lower() == "delta":
            source_container_name,mapping,deep_clone_query,src_loc,tgt_loc = deep_clone_tbl(tbl,src_loc,TargetStorageContainer, SPClientID, SPDirectoryID, ScopeName, KeyVaultSPSecret)
            print("SOURCE LOCATION: {}".format(src_loc))
            print("TARGET LOCATION: {}".format(tgt_loc))
            print("CLONE QUERY: ",deep_clone_query)
            if not mapping: 
                raise Exception(f"No mapping found for the given table {tbl} please check the table Mapping.")
            if mapping:
                src_count_df=spark.read.format('delta').load(src_loc)
                src_count=src_count_df.select([src_count_df.columns[0]]).count()
                try:
                    print(f"Started Running DEEP CLONE for the table {tbl}")
                    deep_clone_df=spark.sql(deep_clone_query)
                    print(f"***DEEP CLONE PRECESS END***")
                except Exception as e:
                    if "Tried to clone a version of the table where files have been deleted manually or by VACUUM" in str(e):
                        print(f"FSCK REPAIR TABLE {tbl}")
                        spark.sql(f"FSCK REPAIR TABLE {tbl}")
                        repair=True
                        print(f"REPAIRED TABLE {tbl} successfully")
                    elif "Changing column mapping mode from" in str(e):
                        print(f"{str(e)}")
                        print("target location to be deleted is ",tgt_loc)
                        dbutils.fs.rm(tgt_loc,True)
                        repair=True
                        print(f"Deleted the underlying files successfully")
                    elif "java.util.concurrent.ExecutionException: java.io.FileNotFoundException" in str(e):
                        print(f"{str(e)}")
                        print("target location to be deleted is ",tgt_loc)
                        dbutils.fs.rm(tgt_loc,True)
                        repair=True
                        print(f"Deleted the underlying files  successfully") 
                    elif "A concurrent transaction has written new data since the current transaction read the table " in str(e):
                        print(f"{str(e)}")
                        print("target location to be deleted is ",tgt_loc)
                        dbutils.fs.rm(tgt_loc,True)
                        repair=True
                        print(f"Deleted the underlying files  successfully")     
                    else:
                        raise e   
                if repair:
                    print("1"*10,f"Trying To Run DEEP CLONE After Repairing the Delta Table {tbl}")     
                    deep_clone_df=spark.sql(deep_clone_query)
                    print("1"*10,"This time deepclone succeeded after repairing the delta table")
            tot_copied_files=deep_clone_df.collect()[0].num_copied_files
            print(f"Clone -Time Taken for  to process the {tbl} with files {tot_copied_files} is  {get_delta_time(start_time_two,datetime.now(timezone('US/Central')))}",end='\n')
            tgt_count_df=spark.read.format('delta').load(tgt_loc)
            tgt_count=tgt_count_df.select([tgt_count_df.columns[0]]).count()   
    except Exception as e:
        return (db,db_loc,tbl,src_loc,tgt_loc,deep_clone_query,src_count,tgt_count,str(e)),'uncloned'
    return (db,db_loc,tbl,src_loc,tgt_loc,deep_clone_query,src_count,tgt_count),'cloned'  

# callback that keeps track of the number of completed tasks
def completed_callback(future):
    sleep(0.5)
    global completed, lock
    with lock:
        completed += 1
        # check the number of remaining tasks
        size = TOTAL_TASKS - completed
        # report the total number of tasks that remain
        print(f'About {size} tables to be processed yet')

# COMMAND ----------

spark.read.format('delta').load('/FileStore/UCDeepClone/UnclonedTableListTESTV5').display()

# COMMAND ----------

uncloned_path_to_load='/FileStore/UCDeepClone/UnclonedTableListTESTV6' 
cloned_path_to_load='/FileStore/UCDeepClone/ClonedTableListTESTV6'
db_start_time_one=datetime.now(timezone("US/Central"))
start_time_one=datetime.now(timezone("US/Central"))

if DB_List != {}:
  print("List of relevant databases:",DB_List, end='\n')
  total_db_left_to_process=len(DB_List)
  print("Total Database to process are ",total_db_left_to_process)
  db_table_count=[]
  for db in DB_List:
      print("{} db's are left to process".format(total_db_left_to_process)) 
      db_loc=get_delta_db_detail(db)
      print("*" * 10,f"started processing the db called {db}","*" * 10,end='\n')
      if TablesToClone.lower()=='uncloned':
        if  DeltaTable.isDeltaTable(spark,"{}".format(uncloned_path_to_load)):
            print("*" * 10,f"started processing the failed/uncloned tables for the database {db}","*" * 10)
            print(uncloned_path_to_load)
            print(db)
            tbl_list_df=spark.read.format('delta').load(uncloned_path_to_load).filter(lower(col('DBName'))== db.lower()).where(f"""RunDate='{RunDate}'""")
            if tbl_list_df.count()>0:
              tbl_list_df.show()
            dataCollect=tbl_list_df.select('tableName').collect()
            tbl_list=[]
            for row in dataCollect:
                tbl_list.append(row['tableName'])
        else:
          print("no uncloned table list to process")
          tbl_list=[]
      else:
        tbl_list = get_db_tbl_list(db)
      db_table_count.append((db,len(tbl_list)))  
      print("-" * 10,f"Total tables under {db}  are:",len(tbl_list),"-" * 10,end='\n')
      uncloned_tbl_list = []
      cloned_tbl_list = []
      total_tables_left_to_process=len(tbl_list) 
      print(f"Tables are left to  process under {db} are ",total_tables_left_to_process)
      n_workers = 8 
      print("parallel execution tables count is ", n_workers)   
      TOTAL_TASKS=len(tbl_list)
      # lock for protecting the completed count
      lock = Lock()
      # the number of tasks that are completed
      completed = 0
      TOTAL_TASKS=len(tbl_list)
      with ThreadPoolExecutor(n_workers) as executor:
            futures = [executor.submit(run_deepclone_parallely, (db,db_loc,table,TargetStorageContainer,SPClientID,SPDirectoryID,ScopeName,KeyVaultSPSecret)) for table in tbl_list]
            for future in as_completed(futures):
                future.add_done_callback(completed_callback)
                result = future.result()
                if result[1]=='uncloned':
                    print(result)
                    uncloned_tbl_list.append(result[0])
                else:
                    cloned_tbl_list.append(result[0])             
      print("-" * 10,"count of cloned tables ------------------",len(cloned_tbl_list),"-" * 10)
      print("-" * 10,"count of uncloned tables ----------------",len(uncloned_tbl_list),"-" * 10)
      schema_ddl="DBName STRING,DBLocation STRING,tableName STRING,SourceTableLocation STRING,TargetTableLocation STRING,DeepCloneQuery STRING,SourceTableCount STRING,TargetTableCount STRING"
      cloned_df = spark.createDataFrame(data = cloned_tbl_list, schema = schema_ddl) \
                          .withColumn('RunTime',lit(start_time_one)).withColumn('RunDate',lit(RunDate))
      uncloned_df = spark.createDataFrame(data = uncloned_tbl_list, schema = schema_ddl+' ,ErrorReason STRING') \
                         .withColumn('RunTime',lit(start_time_one)).withColumn('RunDate',lit(RunDate))

      cloned_df_count=cloned_df.select('DBName').count()
      print('CLONED DF COUNT: {}'.format(cloned_df_count)) 
      uncloned_df_count=uncloned_df.select('DBName').count()
      print('UNCLONED DF COUNT: {}'.format(uncloned_df_count))                
      if cloned_df_count>0:
          print("-" * 10,f"started writing the clone table")
          CloneError = True
          while CloneError:
            try:
              if  DeltaTable.isDeltaTable(spark,"{}".format(cloned_path_to_load)):
                if TablesToClone.lower()=='uncloned':
                    src_cloned_df=spark.read.format('delta').load(cloned_path_to_load).where(col('DBName')==db)
                    cloned_df_updated=src_cloned_df.union(cloned_df)
                else:
                    cloned_df_updated=cloned_df
              else:
                cloned_df_updated=cloned_df  
              cloned_df_updated_count=cloned_df_updated.count()   
              cloned_df_updated.write.format("delta").partitionBy("DBName").option('replaceWhere',f"DBName=='{db}'") \
                       .mode("overwrite").save(cloned_path_to_load)
              CloneError = False
              print(f"Updated Deep clone count after this run for the database {db}",cloned_df_updated_count)
            except (ConcurrentAppendException,ConcurrentWriteException,ProtocolChangedException,ConcurrentDeleteDeleteException,ConcurrentDeleteReadException) as e:
              print("*"*10,"exception raised in clone due to "+e)
              pass                 
      if uncloned_df_count>0:
          print("-" * 10,f"started writing the unclone table")
          Unclone = True
          while Unclone:
            try:
              uncloned_df.write.format("delta").partitionBy("DBName") \
              .option('replaceWhere',f"DBName=='{db}'").mode("overwrite").save(uncloned_path_to_load)  
              Unclone = False
            except (ConcurrentAppendException,ConcurrentWriteException,ProtocolChangedException,ConcurrentDeleteDeleteException,ConcurrentDeleteReadException) as e:
              print("*"*10,"exception raised in unclone due to "+e)
              pass                   
      print("-" * 10,f"Deep Clone - Complete time for the {db} is  {get_delta_time(db_start_time_one,datetime.now(timezone('US/Central')))}",end='\n')
      print("*" * 10,f"Ended processing the db called {db} ",end='\n')
      total_db_left_to_process-=1 
  print("-" * 10,f"Deep Clone - Total Complete time  is  {get_delta_time(start_time_one,datetime.now(timezone('US/Central')))}",end='\n')   
else:
  print("-" * 10,"there is no subject area storage account mapping is available so decided to not to clone")

# COMMAND ----------

if DeltaTable.isDeltaTable(spark,"{}".format(cloned_path_to_load)):
  display(spark.read.format('delta').load(cloned_path_to_load).where(col('DBName').isin(DB_List)).where(f"""RunDate='{RunDate}' and RunTime=='{start_time_one}'"""))
else:
  print("delta table is not created yet")  

# COMMAND ----------

if DeltaTable.isDeltaTable(spark,"{}".format(uncloned_path_to_load)):
  display(spark.read.format('delta').load(uncloned_path_to_load).where(col('DBName').isin(DB_List)).where(f"""RunDate='{RunDate}' and RunTime=='{start_time_one}'"""))
else:
  print("delta table is not created yet")

# COMMAND ----------

df=spark.read.format('delta').load(cloned_path_to_load).where(col('DBName').isin(DB_List)).where(f"""RunDate='{RunDate}' and RunTime=='{start_time_one}'""")
tablelist=df.select("tableName","TargetTableLocation").collect()
dblist=df.select("DBName","DBLocation").dropDuplicates(["DBName"]).collect()
print(len(tablelist))
print(len(dblist))

# COMMAND ----------

# DBTITLE 1,Alter the tables 
failed_DDL=[]
successful_DDL=[]
total_tables_left=len(tablelist)
for  table in tablelist:
  tablename=table[0]
  tablepath=table[1]
  ALTER_STATMENT=f"""ALTER TABLE {tablename}  SET LOCATION '{tablepath}' """
  try:
    print(ALTER_STATMENT)
    #spark.sql(ALTER_STATMENT)
    successful_DDL.append((tablename,ALTER_STATMENT))
  except Exception as e:
    failed_DDL.append((tablename,ALTER_STATMENT,str(e)))
  total_tables_left -= 1  
  print("{}  tables  left to process".format(total_tables_left))
if len(failed_DDL)>0:
  print(failed_DDL)
else:
  print(successful_DDL)
print("DDL Statement executed for all the tables")    

# COMMAND ----------

len(failed_DDL)

# COMMAND ----------

# DBTITLE 1,Alter Database
failed_DDL=[]
successful_DDL=[]
total_db_left=len(dblist)
#ALTER the database location as well for which the tables are not present.
for db in dblist:
  dbname=db[0]
  srcdbpath=db[1]
  container_name=srcdbpath.split('@')[0].split('//')[1]
  storage_account_name=srcdbpath.split('.')[0].split('@')[1]
  source_container_name=srcdbpath.split('@')[0].split('//')[1]
  remaining_path=srcdbpath.split('@')[1].split('.net')[1]
  targetdbpath=f"abfss://{TargetStorageContainer}@{storage_account_name}.dfs.core.windows.net/{source_container_name}{remaining_path}"
  ALTER_STATMENT=f"""ALTER DATABASE {dbname} SET LOCATION '{targetdbpath}' """
  try:
    print(ALTER_STATMENT)
    #spark.sql(ALTER_STATMENT)
    successful_DDL.append((dbname,ALTER_STATMENT))
  except Exception as e:
    failed_DDL.append((dbname,ALTER_STATMENT,str(e)))
  total_db_left -= 1  
  print("{}  db's  left to process".format(total_db_left))
if len(failed_DDL)>0:
  print(failed_DDL)
else:
  print(successful_DDL)
print("DDL Statement executed for all the databases")    

# COMMAND ----------

spark.catalog.databaseExists("fdn_latam_df1_bronze_dev_df")

# COMMAND ----------

# DBTITLE 1,Change the location of Database with No Tables
db_count=len(DB_List)
for db in DB_List:
    try:
        tbl_list = get_db_tbl_list(db)
        db_loc=get_delta_db_detail(db)
        storage_account_name=db_loc.split('.')[0].split('@')[1]
        source_container_name=db_loc.split('@')[0].split('//')[1]
        remaining_path=db_loc.split('@')[1].split('.net')[1]
        print(f"##########################DATABASE--{db_count}#############################")
        print("LOCATION: ",db_loc)
        print("LIST OF TABLES: ",tbl_list)
        if spark.catalog.databaseExists(db):
            if len(tbl_list)>0:
                print('Location Already Altered')
            else:
                targetdbpath=f"abfss://{TargetStorageContainer}@{storage_account_name}.dfs.core.windows.net/{source_container_name}{remaining_path}"
                ALTER_STATMENT=f"""ALTER DATABASE {db} SET LOCATION '{targetdbpath}' """
                print(ALTER_STATMENT)
        else:
            raise Exception(f'{db} Not Found!')
        db_count-=1
    except Exception as e:  # Catch any errors during database retrieval
        print(f"Database '{db}' not found: {e}")
        print("=================================NOT FOUND=================================")
        continue  # Skip to the next database in the list
    print("==================================================================")

# COMMAND ----------

# alter_schema_ddl="tableName STRING,DDLStatement STRING,ErrorReason STRING"
# Failed_DF = spark.createDataFrame(data = failed_DDL, schema = alter_schema_ddl)
# display(Failed_DF)

# COMMAND ----------

# dbutils.fs.rm('/FileStore/DeepClone/UnclonedTableList/',True)

# COMMAND ----------

# dbutils.fs.rm('/FileStore/DeepClone/ClonedTableList/',True)

# COMMAND ----------

# %sql
# desc extended delta.`/FileStore/UCDeepClone/UnclonedTableListTEST1/`

# COMMAND ----------


