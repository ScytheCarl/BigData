#!/bin/bash

#The shell which can upload yesterday's log file to hdfs on certain time everyday

#get the date of yesterday
yesterday=$1
if [ "yesterday"="" ]
then
yesterday=`date +%Y_%m_%d --date="1 days ago"`
fi

#get the path of log files
logPath=/data/log/access_${yesterday}.log

#get hdfs log path
hdfsPath=log/${yesterday//_/}

#mkdir on hdfs
hdfs dfs -mkdir -p ${hdfsPath}

#upload
hdfs dfs -put ${logPath} ${hdfsPan}

#Do not forget to set the Crontab
