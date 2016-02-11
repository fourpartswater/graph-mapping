#!/bin/bash

MAIN_JAR=../xdata-graph-0.1-SNAPSHOT/lib/xdata-graph.jar
MAIN_CLASS=software.uncharted.graphing.layout.ClusteredGraphLayoutApp
BASE_LOCATION=/user/nkronenfeld/graphing/timing

DATASET=$1


pushd ${DATASET}

echo
echo Running in `pwd`
echo Starting at `date`


MAX_LEVEL=`ls -d level_* | awk -F'_' '{print $2}' | sort -nr | head -n1`
MAX_SIZE=`du -s -BM level_0 | awk -F M '{print $1}'`
# one part per 16M data
PARTS=$(expr ${MAX_SIZE} / 8)
EXECUTORS=$(expr $(expr ${PARTS} + 7) / 8)

echo MAX_LEVEL: ${MAX_LEVEL}
echo MAX_SIZE: ${MAX_SIZE}
echo PARTS: ${PARTS}
echo EXECUTORS: ${EXECUTORS}

echo MAX_LEVEL: ${MAX_LEVEL} > layout.log
echo MAX_SIZE: ${MAX_SIZE} >> layout.log
echo PARTS: ${PARTS} >> layout.log
echo EXECUTORS: ${EXECUTORS} >> layout.log

TIMEA=$(date +%s)

echo Pushing to HDFS
hdfs dfs -rm -r -skipTrash ${BASE_LOCATION}/${DATASET}
hdfs dfs -mkdir ${BASE_LOCATION}/${DATASET}
hdfs dfs -mkdir ${BASE_LOCATION}/${DATASET}/clusters
hdfs dfs -put level_* ${BASE_LOCATION}/${DATASET}/clusters
hdfs dfs -rm ${BASE_LOCATION}/${DATASET}/clusters/level_*/stats

TIMEB=$(date +%s)

echo Starting layout run

echo spark-submit \
	--class ${MAIN_CLASS} \
	--num-executors ${EXECUTORS} \
	--executor-cores 4 \
	--executor-memory 10g \
	${MAIN_JAR} \
	-source ${BASE_LOCATION}/${DATASET}/clusters \
	-output ${BASE_LOCATION}/${DATASET}/layout \
	-maxLevel ${MAX_LEVEL} \
	-spark yarn-client \
	-parts ${PARTS} >> layout.log

spark-submit \
	--class ${MAIN_CLASS} \
	--num-executors ${EXECUTORS} \
	--executor-cores 4 \
	--executor-memory 10g \
	${MAIN_JAR} \
	-source ${BASE_LOCATION}/${DATASET}/clusters \
	-output ${BASE_LOCATION}/${DATASET}/layout \
	-maxLevel ${MAX_LEVEL} \
	-parts ${PARTS} |& tee -a layout.log

# Note: Took out -spark yarn-client.  Should be irrelevant, but noted just in case I'm wrong.

TIMEC=$(date +%s)

echo >> layout.log
echo >> layout.log
echo Time to upload to HDFS: $(( ${TIMEB} - ${TIMEA} )) seconds >> layout.log
echo Elapsed time for layout: $(( ${TIMEC} - ${TIMEB} )) seconds >> layout.log

echo
echo
echo Done at `date`
echo Time to upload to HDFS: $(( ${TIMEB} - ${TIMEA} )) seconds
echo Elapsed time for layout: $(( ${TIMEC} - ${TIMEB} )) seconds

popd

