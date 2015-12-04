#!/bin/bash

export MASTER=yarn-client
export CORES=4
export MEM=10g
export EXECUTORS=4
export MAIN_JAR=../lib/xdata-graph.jar
export SOURCE=hdfs://uscc0-master0/xdata/data/affinity_graph/amazon_parsed_nocategory_edgeweights.txt

spark-submit \
    --master ${MASTER} \
    --num-executors ${EXECUTORS} \
    --executor-memory ${MEM} \
    --executor-cores ${CORES} \
    --class software.uncharted.graphing.clustering.usc.reference.LouvainSpark \
    ${MAIN_JAR} \
    -nodeFile ${SOURCE} \
    -nodeIdCol 1 \
    -edgeFile ${SOURCE} \
    -edgeSrcCol 1 \
    -edgeDstCol 2 \
    -edgeWeightCol 8 \
    -partitions 5

