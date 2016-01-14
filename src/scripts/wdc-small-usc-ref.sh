#!/bin/bash

export MASTER=yarn-client
export CORES=1
export MEM=28g
export EXECUTORS=5
export MAIN_JAR=../lib/xdata-graph.jar
export SOURCE=hdfs://uscc0-master0/xdata/data/SummerCamp2014/wdc-payleveldomain/pld-arc


spark-submit \
    --master ${MASTER} \
    --num-executors ${EXECUTORS} \
    --executor-memory ${MEM} \
    --executor-cores ${CORES} \
    --class software.uncharted.graphing.clustering.usc.reference.LouvainSpark \
    ${MAIN_JAR} \
    -edgeFile ${SOURCE} \
    -edgeSeparator "[ 	]+" \
    -edgeSrcCol 0 \
    -edgeDstCol 1 \
    -partitions 5


