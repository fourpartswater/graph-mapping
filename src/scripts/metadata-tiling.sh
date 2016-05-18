#!/bin/bash

MAIN_JAR=../xdata-graph-0.1-SNAPSHOT/lib/xdata-graph.jar
MAIN_CLASS=software.uncharted.graphing.salt.MetadataTilingPipeline
BASE_LOCATION=/user/nkronenfeld/graphing/timing

TOP_LEVEL=3
NEXT_LEVELS=2
DATASET=

while [ "$1" != "" ]; do
	case $1 in 
		-d | --dataset )
			shift
			DATASET=$1
			;;
		-1 | -t | --top )
			shift
			TOP_LEVEL=$1
			;;
		-n | -b | --next | --bottom )
			shift
			NEXT_LEVELS=$1
			;;
	esac
	shift
done

if [ "${DATASET}" == "" ]; then
	echo No dataset specified
	exit
fi

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
		curLevels=${TOP_LEVEL}
	else
		curLevels=${NEXT_LEVELS}
	fi
	if [ -z ${LEVELS} ]; then
		LEVELS=${curLevels}
	else
		LEVELS=${LEVELS},${curLevels}
	fi
done
DATATABLE="graph-metadata-${DATASET}-salt"

OTHER_ARGS=
case ${DATASET} in 

	analytics)
		OTHER_ARGS="${OTHER_ARGS} -analytic software.uncharted.graphing.analytics.SumAnalytic3"
		OTHER_ARGS="${OTHER_ARGS} -analytic software.uncharted.graphing.analytics.MeanAnalytic4"
		OTHER_ARGS="${OTHER_ARGS} -analytic software.uncharted.graphing.analytics.MinAnalytic5"
		;;
esac

echo MAX_LEVEL: ${MAX_LEVEL}
echo PARTITIONS: ${PARTITIONS}
echo EXECUTORS: ${EXECUTORS}
echo LEVELS: ${LEVELS}

echo MAX_LEVEL: ${MAX_LEVEL} > metadata-tiling.log
echo PARTITIONS: ${PARTITIONS} >> metadata-tiling.log
echo EXECUTORS: ${EXECUTORS} >> metadata-tiling.log
echo LEVELS: ${LEVELS} >> metadata-tiling.log

echo
echo Removing old tile set
echo "disable '${DATATABLE}'" > clear-hbase-table
echo "drop '${DATATABLE}'" >> clear-hbase-table

hbase shell < clear-hbase-table

# Extra jars needed by tiling processes
EXTRA_JARS=/opt/cloudera/parcels/CDH/lib/hbase/lib/htrace-core-3.2.0-incubating.jar:/opt/cloudera/parcels/CDH/lib/hbase/lib/hbase-client-1.0.0-cdh5.5.2.jar

STARTTIME=$(date +%s)
echo Starting tiling

echo spark-submit \
	--num-executors ${EXECUTORS} \
	--executor-memory 10g \
	--executor-cores 4 \
    --conf spark.executor.extraClassPath=${EXTRA_JARS} \
    --driver-class-path ${EXTRA_JARS} \
    --jars ${EXTRA_JARS} \
	--class ${MAIN_CLASS} \
	${MAIN_JAR} \
	-base ${BASE_LOCATION}/${DATASET}/layout/ \
	-levels ${LEVELS} \
	-name ${DATATABLE} \
	-hbaseConfig /etc/hbase/conf/hbase-site.xml \
	${OTHER_ARGS} >> metadata-tiling.log


spark-submit \
	--num-executors ${EXECUTORS} \
	--executor-memory 10g \
	--executor-cores 4 \
    --conf spark.executor.extraClassPath=${EXTRA_JARS} \
    --driver-class-path ${EXTRA_JARS} \
    --jars ${EXTRA_JARS} \
	--class ${MAIN_CLASS} \
	${MAIN_JAR} \
	-base ${BASE_LOCATION}/${DATASET}/layout/ \
	-levels ${LEVELS} \
	-name ${DATATABLE} \
	-hbaseConfig /etc/hbase/conf/hbase-site.xml \
	${OTHER_ARGS} |& tee -a metadata-tiling.log

ENDTIME=$(date +%s)

echo >> metadata-tiling.log
echo >> metadata-tiling.log
echo Elapsed time: $(( ${ENDTIME} - ${STARTTIME} )) seconds >> metadata-tiling.log

echo
echo
echo Done at `date`
echo Elapsed time: $(( ${ENDTIME} - ${STARTTIME} )) seconds

popd
