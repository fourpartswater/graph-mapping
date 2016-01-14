#!/bin/bash

export MEM=4g
export MAIN_JAR=../lib/xdata-graph.jar
export SOURCE=hdfs://uscc0-master0/xdata/data/SummerCamp2014/wdc-payleveldomain/pld-arc


spark-submit \
	--class software.uncharted.graphing.clustering.unithread.Community \
	--driver-memory ${MEM} \
	${MAIN_JAR} \
	affinity-graph.edges -m affinity-graph.metadata -l -1 -v

