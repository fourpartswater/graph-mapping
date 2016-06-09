#!/bin/bash



# Include any needed common scripts                                                                                              
SOURCE_LOCATION=$( cd $( dirname ${BASH_SOURCE[0]} ) && pwd )
. ${SOURCE_LOCATION}/level-config.sh



# Set up default parameters
MAIN_JAR=../xdata-graph-0.1-SNAPSHOT/lib/xdata-graph.jar
TOP_LEVEL=3
NEXT_LEVELS=2

# Set up application-specific parameters
# MAIN_CLASS=software.uncharted.graphing.salt.NodeTilingPipeline
MAIN_CLASS=software.uncharted.graphing.config.ConfigurationTester
APPLICATION_NAME="Node tiling pipeline"

# Set up our dataset
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



# copy our output script into the dataset, if it's not already there
if [ ! -e ${DATASET}/output.conf ]; then
	cp ${SOURCE_LOCATION}/config/default-output.conf ${DATASET}/output.conf
fi

# move to where our dataset is stored
pushd ${DATASET}




echo
echo Running ${APPLICATION_NAME}
echo Running in `pwd`
echo Starting at `date`

# Determine parameters about our dataset
MAX_LEVEL=$(getMaxLevel ${DATASET})
PARTITIONS=$(getPartitions ${DATASET})
EXECUTORS=$(getExecutors ${DATASET})
LEVELS=($(hardCodedLevels ${DATASET} ${TOP_LEVEL} ${NEXT_LEVELS}))
DATATABLE="graph-nodes-${DATASET}-salt"

OTHER_ARGS=

case ${DATASET} in 

	analytics)
		ANALYTICS=
		OTHER_ARGS="${OTHER_ARGS} -Dgraph.analytics.0=software.uncharted.graphing.analytics.SumAnalytic3"
		OTHER_ARGS="${OTHER_ARGS} -Dgraph.analytics.1=software.uncharted.graphing.analytics.MeanAnalytic4"
		OTHER_ARGS="${OTHER_ARGS} -Dgraph.analytics.2=software.uncharted.graphing.analytics.MinAnalytic5"
		;;

esac

echo MAX_LEVEL: ${MAX_LEVEL}
echo PARTITIONS: ${PARTITIONS}
echo EXECUTORS: ${EXECUTORS}
echo LEVELS: ${LEVELS}
echo OTHER_ARGS: ${OTHER_ARGS}

echo MAX_LEVEL: ${MAX_LEVEL} > node-tiling.log
echo PARTITIONS: ${PARTITIONS} >> node-tiling.log
echo EXECUTORS: ${EXECUTORS} >> node-tiling.log
echo LEVELS: ${LEVELS} >> node-tiling.log
echo OTHER_ARGS: ${OTHER_ARGS} >> node-tiling.log

echo
echo Removing old tile set
echo "disable '${DATATABLE}'" > clear-hbase-table
echo "drop '${DATATABLE}'" >> clear-hbase-table

hbase shell < clear-hbase-table



# See if there is a local node configuration file
if [ -e node-tiling.conf ]; then
	CONFIGURATION=node-tiling.conf
else
	CONFIGURATION=
fi



# Extra jars needed by tiling processes
EXTRA_JARS=/opt/cloudera/parcels/CDH/lib/hbase/lib/htrace-core-3.2.0-incubating.jar:/opt/cloudera/parcels/CDH/lib/hbase/lib/hbase-client-1.0.0-cdh5.5.2.jar



# OK, that's all we need - start tiling.
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
	output.conf \
	${CONFIGURATION} \
	-Dtiling.source=$( relativeToSource ${DATASET} layout ) \
	$( getLevelConfig ${LEVELS[@]} ) \
	${OTHER_ARGS} >> node-tiling.log

spark-submit \
	--num-executors ${EXECUTORS} \
	--executor-memory 10g \
	--executor-cores 4 \
    --conf spark.executor.extraClassPath=${EXTRA_JARS} \
    --driver-class-path ${EXTRA_JARS} \
    --jars ${EXTRA_JARS} \
	--class ${MAIN_CLASS} \
	${MAIN_JAR} \
	output.conf \
	${CONFIGURATION} \
	-Dtiling.source=$( relativeToSource ${DATASET} layout ) \
	$( getLevelConfig ${LEVELS[@]} ) \
	${OTHER_ARGS} >> node-tiling.log



ENDTIME=$(date +%s)

echo >> node-tiling.log
echo >> node-tiling.log
echo Elapsed time: $(( ${ENDTIME} - ${STARTTIME} )) seconds >> node-tiling.log

echo
echo
echo Done at `date`
echo Elapsed time: $(( ${ENDTIME} - ${STARTTIME} )) seconds

popd
