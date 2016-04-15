#!/bin/bash

#
# THis script pulls a tile set from HBase onto the local file system.
#

MAIN_CLASS=software.uncharted.graphing.tiling.TilesetPuller
MAIN_JAR=../lib/xdata-graph.jar
JOB_MASTER=yarn-client
HDFS_LOC=hdfs://uscc0-master0.uncharted.software
TABLE=$1
FILE=$2

spark-submit \
	--num-executors 1 \
	--driver-memory 2g \
	--executor-cores 1 \
    --conf spark.executor.extraClassPath=/opt/cloudera/parcels/CDH/lib/hbase/lib/htrace-core-3.1.0-incubating.jar \
    --driver-class-path /opt/cloudera/parcels/CDH/lib/hbase/lib/htrace-core-3.1.0-incubating.jar \
    --jars /opt/cloudera/parcels/CDH/lib/hbase/lib/htrace-core-3.1.0-incubating.jar \
	--class ${MAIN_CLASS} \
	--master ${JOB_MASTER} \
	${MAIN_JAR} \
	-table ${TABLE} \
	-file ${FILE} \
	-config /etc/hbase/conf/hbase-site.xml
