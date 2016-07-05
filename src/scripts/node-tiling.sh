#!/bin/bash



# Include any needed common scripts
SOURCE_LOCATION=$( cd $( dirname ${BASH_SOURCE[0]} ) && pwd )
. ${SOURCE_LOCATION}/arg-parser.sh



# Set up default parameters
MAIN_JAR=../xdata-graph-0.1-SNAPSHOT/lib/xdata-graph.jar

# Set up application-specific parameters
# Switch main classes in order to debug configuration
MAIN_CLASS=software.uncharted.graphing.salt.NodeTilingPipeline
# MAIN_CLASS=software.uncharted.graphing.config.ConfigurationTester
APPLICATION_NAME="Node tiling pipeline"



# Parse input arguments
parseArguments graph-nodes- -salt "$@"



# copy our config files into the dataset, if they're not already there
OUTPUT_COPIED=$(checkConfigFile ${SOURCE_LOCATION}/config/default-output.conf ${DATASET}/output.conf)
TILING_COPIED=$(checkConfigFile ${SOURCE_LOCATION}/config/default-tiling.conf ${DATASET}/tiling.conf)
GRAPH__COPIED=$(checkConfigFile ${SOURCE_LOCATION}/config/default-graph.conf  ${DATASET}/graph.conf)



# Move to where our dataset is stored
pushd ${DATASET}




echo
echo Running ${APPLICATION_NAME}
echo Running in `pwd`
echo Starting at `date`

# Determine parameters about our dataset
MAX_LEVEL=$(getMaxLevel ${DATASET})
PARTITIONS=$(getPartitions ${DATASET})
EXECUTORS=$(getExecutors ${DATASET})

OTHER_ARGS=

case ${DATASET} in 

	analytics)
		OTHER_ARGS="${OTHER_ARGS} -Dgraph.analytics.0=software.uncharted.graphing.analytics.SumAnalytic3"
		OTHER_ARGS="${OTHER_ARGS} -Dgraph.analytics.1=software.uncharted.graphing.analytics.MeanAnalytic4"
		OTHER_ARGS="${OTHER_ARGS} -Dgraph.analytics.2=software.uncharted.graphing.analytics.MinAnalytic5"
		;;

esac

EXTRA_DRIVER_JAVA_OPTS=-Dtiling.source=$( relativeToSource ${DATASET} layout )
EXTRA_DRIVER_JAVA_OPTS="${EXTRA_DRIVER_JAVA_OPTS} $( getLevelConfig ${LEVELS[@]} )"
EXTRA_DRIVER_JAVA_OPTS=${EXTRA_DRIVER_JAVA_OPTS} ${OTHER_ARGS}

echo DATATABLE: ${DATATABLE}
echo MAX_LEVEL: ${MAX_LEVEL}
echo PARTITIONS: ${PARTITIONS}
echo EXECUTORS: ${EXECUTORS}
echo LEVELS: ${LEVELS[@]}
echo Extra java args: ${EXTRA_DRIVER_JAVA_OPTS}

echo DATATABLE: ${DATATABLE} > node-tiling.log
echo MAX_LEVEL: ${MAX_LEVEL} >> node-tiling.log
echo PARTITIONS: ${PARTITIONS} >> node-tiling.log
echo EXECUTORS: ${EXECUTORS} >> node-tiling.log
echo LEVELS: ${LEVELS[@]} >> node-tiling.log
echo Extra java args: ${EXTRA_DRIVER_JAVA_OPTS} >> node-tiling.log

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

echo Run command: >> node-tiling.log
echo spark-submit \
	--num-executors ${EXECUTORS} \
	--executor-memory 10g \
	--executor-cores 4 \
    --conf spark.executor.extraClassPath=${EXTRA_JARS} \
    --driver-class-path ${EXTRA_JARS} \
    --jars `echo ${EXTRA_JARS} | tr : ,` \
	--class ${MAIN_CLASS} \
	--conf "spark.driver.extraJavaOptions=${EXTRA_DRIVER_JAVA_OPTS}" \
	${MAIN_JAR} \
	output.conf tiling.conf graph.conf \
	${CONFIGURATION} \
	>> node-tiling.log
echo >> node-tiling.log
echo >> node-tiling.log

spark-submit \
	--num-executors ${EXECUTORS} \
	--executor-memory 10g \
	--executor-cores 4 \
    --conf spark.executor.extraClassPath=${EXTRA_JARS} \
    --driver-class-path ${EXTRA_JARS} \
    --jars `echo ${EXTRA_JARS} | tr : ,` \
	--class ${MAIN_CLASS} \
	--conf "spark.driver.extraJavaOptions=${EXTRA_DRIVER_JAVA_OPTS}" \
	${MAIN_JAR} \
	output.conf tiling.conf graph.conf \
	${CONFIGURATION} \
	|& tee -a node-tiling.log



ENDTIME=$(date +%s)



cleanupConfigFile ${OUTPUT_COPIED} output.conf
cleanupConfigFile ${TILING_COPIED} tiling.conf
cleanupConfigFile ${GRAPH__COPIED} graph.conf



echo >> node-tiling.log
echo >> node-tiling.log
echo Start time: ${STARTTIME} >> node-tiling.log
echo End time: ${ENDTIME} >> node-tiling.log
echo Elapsed time: $(( ${ENDTIME} - ${STARTTIME} )) seconds >> node-tiling.log

echo
echo
echo Done at `date`
echo Elapsed time: $(( ${ENDTIME} - ${STARTTIME} )) seconds

popd
