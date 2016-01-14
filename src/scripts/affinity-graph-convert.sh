#!/bin/bash

export MEM=4g
export MAIN_JAR=../lib/xdata-graph.jar
export SOURCE=hdfs://uscc0-master0/xdata/data/SummerCamp2014/wdc-payleveldomain/pld-arc


spark-submit \
	--class software.uncharted.graphing.clustering.unithread.Convert \
	--driver-memory ${MEM} \
	${MAIN_JAR} \
	-ie affinity-graph.txt \
	-fe edge \
	-s 1 \
	-d 2 \
	-in affinity-graph.txt \
	-fn node \
	-cn \\t \
	-m 5 \
	-n 1 \
	-oe affinity-graph.edges \
	-om affinity-graph.metadata
