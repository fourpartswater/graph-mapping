#!/bin/bash

export MAIN_JAR=../lib/xdata-graph.jar
export SOURCE=hdfs://uscc0-master0/user/nkronenfeld/affinity-graph

spark-submit --class software.uncharted.graphing.layout.ClusteredGraphLayoutApp \
	--num-executors 10 \
	--executor-memory 10g \
	--executor-cores 4 \
	${MAIN_JAR} \
	-source ${SOURCE}/ag-levels \
	-output ${SOURCE}/ag-layout \
	-maxLevel 6 \
	-spark yarn-client \
	-parts 80

