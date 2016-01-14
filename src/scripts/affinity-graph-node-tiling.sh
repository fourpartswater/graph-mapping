#!/bin/bash

MAIN_CLASS=software.uncharted.graphing.tiling.NodeTilingPipelineApp
MAIN_JAR=../lib/xdata-graph.jar
JOB_MASTER=yarn-client
HDFS_LOC=hdfs://uscc0-master0.uncharted.software

spark-submit \
	--num-executors 10 \
	--executor-memory 10g \
	--executor-cores 4 \
    --conf spark.executor.extraClassPath=/opt/cloudera/parcels/CDH/lib/hbase/lib/htrace-core-3.1.0-incubating.jar \
    --driver-class-path /opt/cloudera/parcels/CDH/lib/hbase/lib/htrace-core-3.1.0-incubating.jar \
    --jars /opt/cloudera/parcels/CDH/lib/hbase/lib/htrace-core-3.1.0-incubating.jar \
	--class ${MAIN_CLASS} \
	--master ${JOB_MASTER} \
	${MAIN_JAR} \
	-base ${HDFS_LOC}/user/nkronenfeld/affinity-graph/ag-layout \
	-levels 3,2,2,2,2,2 \
	-nodeColumns affinity-graph-node-tiling.bd affinity-graph-node-tiling.bd \
	-name affinity-graph-test


