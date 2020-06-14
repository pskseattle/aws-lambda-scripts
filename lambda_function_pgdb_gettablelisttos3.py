import boto3
import os
import sys
import json
import cx_Oracle
import psycopg2
import pymysql
import boto3
import datetime


def lambda_handler(event, context):
    today=datetime.datetime.now();
    mytimestamp = today.strftime("%Y%m%d%H%M%S")
    srcType='postgres'
    dbUsername='postgres'
    srcPassword='postgres123'
    dbPort=5432
    dbHost='pgtst1.cpmpdensv4tc.us-east-2.rds.amazonaws.com'
    dbName="dvdrental"
    srcSchema="public"
    tablekeyword="%"
    rows = []
    filename= "tables.txt"
    bucket="rdstoolbox"
    s3_path= "rdstools_schema_snaps" + "/" + dbHost +  "/" + dbName + "/" + srcSchema + "/" + mytimestamp + "/" + filename
    lambda_path="/tmp/" + srcSchema + mytimestamp + filename
    #encoded_string = string.encode("utf-8")
    query = "select datname,pid,usename,application_name,state,wait_event from pg_stat_activity"
    #this is tested fine#srcTableListQuery = "select tablename   from pg_tables where upper(schemaname) like upper('" + srcSchema + "')  and upper(tablename)  like upper('" + tablekeyword + "') order by tablename"
    srcTableListQuery = "select json_agg(t) from (select schemaname,tablename,tableowner from pg_catalog.pg_tables where schemaname='public' order by tablename) as t"
    print('Checking connection ');
    if srcType == 'oracle':
        srcConnStr=dbUsername + '/' + srcPassword + '@' + dbHost + ":" + str(dbPort) + "/" + dbName
    if srcType == 'mysql':
        srcConnStr = {
            'user': dbUsername ,
            'password': srcPassword ,
            'host': dbHost,
            'database': dbName,
            'raise_on_warnings': True,
            'use_pure': False,
            }
    if srcType == 'postgres':
       srcConnStr = "user=" + dbUsername + " password=" + srcPassword + " host=" + dbHost +  " port=" +  str(dbPort) + " dbname=" + dbName

    if ( srcType == "postgres" ):
        try :
                print("connection string:" + srcConnStr)
                conn  = psycopg2.connect(srcConnStr)
                print("SUCCESS")
                with conn.cursor() as cur:
                    cur.execute(srcTableListQuery)
                    #cur.copy_expert(srcTableListQuery)
                    for row in cur:
                        rows.append(row)

                print(json.dumps(rows))
                try:
                    print("Now writing to file")
                    with open(lambda_path, 'w+') as file:
                        file.write(json.dumps(rows))
                        file.close()
                except:
                    print("Error writing to file")
                try:
                    print("Now writing to S3")
                    s3_client = boto3.client('s3')
                    s3_client.upload_file(lambda_path, bucket, s3_path)
                    print("Done writing to S3")
                except ClientError as error:
                    print("ERROR:Something went wrong: {} ".format(error))

        except psycopg2.Error as error:
            print("ERROR:Something went wrong: {} ".format(error))
            print(error.pgerror)
    return {
        'statusCode': 200,
        #'body': json.dumps('Hello from Lambda!')
        'body' : json.dumps(rows)
    }
