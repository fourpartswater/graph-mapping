#!/bin/bash

MAIN_JAR=../xdata-graph-0.1-SNAPSHOT/lib/xdata-graph.jar
MAIN_CLASS=software.uncharted.graphing.tiling.EdgeTilingPipelineApp
BASE_LOCATION=/user/nkronenfeld/graphing/timing

DATASET=$1

pushd ${DATASET}

echo
echo Running in `pwd`
echo Starting at `date`

MAX_LEVEL=`hdfs dfs -ls ${BASE_LOCATION}/${DATASET}/layout | awk '{print $8}' | awk -F / '{print $NF}' | grep level_ | awk -F'_' '{print $2}' | sort -nr | head -n1`
PARTITIONS=`hdfs dfs -ls ${BASE_LOCATION}/${DATASET}/layout/level_0 | wc -l`
EXECUTORS=$(expr $(expr ${PARTITIONS} + 7) / 8)
LEVELS=
for level in `hdfs dfs -ls ${BASE_LOCATION}/${DATASET}/layout | awk '{print $8}' | awk -F '/' '{print $NF}' | grep level_ | awk -F '_' '{print $2}' | sort`
do
	if [ $level -eq 0 ]; then
		curLevels=3
	else
		curLevels=2
	fi
	if [ -z ${LEVELS} ]; then
		LEVELS=${curLevels}
	else
		LEVELS=${LEVELS},${curLevels}
	fi
done


echo MAX_LEVEL: ${MAX_LEVEL}
echo PARTITIONS: ${PARTITIONS}
echo EXECUTORS: ${EXECUTORS}
echo LEVELS: ${LEVELS}
echo OTHER_ARGS: ${OTHER_ARGS}

echo MAX_LEVEL: ${MAX_LEVEL} > inter-edge-tiling.log
echo PARTITIONS: ${PARTITIONS} >> inter-edge-tiling.log
echo EXECUTORS: ${EXECUTORS} >> inter-edge-tiling.log
echo LEVELS: ${LEVELS} >> inter-edge-tiling.log
echo OTHER_ARGS: ${OTHER_ARGS} >> inter-edge-tiling.log

echo
echo Removing old tile set
echo "disable 'graph-inter-edges-${DATASET}'" > clear-hbase-table
echo "drop 'graph-inter-edges-${DATASET}'" >> clear-hbase-table

hbase shell < clear-hbase-table

STARTTIME=$(date +%s)
echo Starting tiling

echo spark-submit \
	--num-executors ${EXECUTORS} \
	--executor-memory 10g \
	--executor-cores 4 \
    --conf spark.executor.extraClassPath=/opt/cloudera/parcels/CDH/lib/hbase/lib/htrace-core-3.1.0-incubating.jar \
    --driver-class-path /opt/cloudera/parcels/CDH/lib/hbase/lib/htrace-core-3.1.0-incubating.jar \
    --jars /opt/cloudera/parcels/CDH/lib/hbase/lib/htrace-core-3.1.0-incubating.jar \
	--class ${MAIN_CLASS} \
	${MAIN_JAR} \
	-base ${BASE_LOCATION}/${DATASET}/layout/ \
	-levels ${LEVELS} \
	-edgeColumns ../edge-tiling.bd \
	-name graph-inter-edges-${DATASET} \
	-minLength 4 \
	-maxLength 1024 \
	-edgeTest "^edge.*" \
	-edgeColumn edgeType \
	-edgeType inter >> inter-edge-tiling.log

spark-submit \
	--num-executors ${EXECUTORS} \
	--executor-memory 10g \
	--executor-cores 4 \
    --conf spark.executor.extraClassPath=/opt/cloudera/parcels/CDH/lib/hbase/lib/htrace-core-3.1.0-incubating.jar \
    --driver-class-path /opt/cloudera/parcels/CDH/lib/hbase/lib/htrace-core-3.1.0-incubating.jar \
    --jars /opt/cloudera/parcels/CDH/lib/hbase/lib/htrace-core-3.1.0-incubating.jar \
	--class ${MAIN_CLASS} \
	${MAIN_JAR} \
	-base ${BASE_LOCATION}/${DATASET}/layout/ \
	-levels ${LEVELS} \
	-edgeColumns ../edge-tiling.bd \
	-name graph-inter-edges-${DATASET} \
	-minLength 4 \
	-maxLength 1024 \
	-edgeTest "^edge.*" \
	-edgeColumn edgeType \
	-edgeType inter |& tee -a inter-edge-tiling.log

ENDTIME=$(date +%s)

echo >> inter-edge-tiling.log
echo >> inter-edge-tiling.log
echo Elapsed time: $(( ${ENDTIME} - ${STARTTIME} )) seconds >> inter-edge-tiling.log

echo
echo
echo Done at `date`
echo Elapsed time: $(( ${ENDTIME} - ${STARTTIME} )) seconds

popd
