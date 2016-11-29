#!/bin/bash



# Include any needed common scripts
SOURCE_LOCATION=$( cd $( dirname ${BASH_SOURCE[0]} ) && pwd )
. ${SOURCE_LOCATION}/arg-parser.sh



# Set up default parameters
MAIN_JAR=../xdata-graph-0.1-SNAPSHOT/lib/xdata-graph.jar

# Set up application-specific parameters
# Switch main classes in order to debug configuration
MAIN_CLASS=software.uncharted.graphing.salt.EdgeTilingPipeline
# MAIN_CLASS=software.uncharted.graphing.config.ConfigurationTester
APPLICATION_NAME="Intra-community edge tiling pipeline"




# Parse input arguments
parseArguments graph-intra-edges- -salt "$@"


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

EXTRA_DRIVER_JAVA_OPTS="-Dgraph.edges.type=intra"
EXTRA_DRIVER_JAVA_OPTS="${EXTRA_DRIVER_JAVA_OPTS} -Dtiling.source=$( relativeToSource ${DATASET} layout )"
EXTRA_DRIVER_JAVA_OPTS="${EXTRA_DRIVER_JAVA_OPTS} $( getLevelConfig ${LEVELS[@]} )"
EXTRA_DRIVER_JAVA_OPTS="${EXTRA_DRIVER_JAVA_OPTS} ${OTHER_ARGS}"

echo DATATABLE: ${DATATABLE}
echo MAX_LEVEL: ${MAX_LEVEL}
echo PARTITIONS: ${PARTITIONS}
echo EXECUTORS: ${EXECUTORS}
echo LEVELS: ${LEVELS}
echo Extra java args: ${EXTRA_DRIVER_JAVA_OPTS}

echo DATATABLE: ${DATATABLE} > intra-edge-tiling.log
echo MAX_LEVEL: ${MAX_LEVEL} >> intra-edge-tiling.log
echo PARTITIONS: ${PARTITIONS} >> intra-edge-tiling.log
echo EXECUTORS: ${EXECUTORS} >> intra-edge-tiling.log
echo LEVELS: ${LEVELS} >> intra-edge-tiling.log
echo Extra java args: ${EXTRA_DRIVER_JAVA_OPTS} >> intra-edge-tiling.log

echo
echo Removing old tile set
echo "disable '${DATATABLE}'" > clear-hbase-table
echo "drop '${DATATABLE}'" >> clear-hbase-table

hbase shell < clear-hbase-table



# See if there is a local intra-community edge configuration file
if [ -e intra-edge-tiling.conf ]; then
	CONFIGURATION=intra-edge-tiling.conf
else
	CONFIGURATION=
fi



# Extra jars needed by tiling processes
EXTRA_JARS=/opt/cloudera/parcels/CDH/lib/hbase/lib/htrace-core-3.2.0-incubating.jar:/opt/cloudera/parcels/CDH/lib/hbase/lib/hbase-client-1.0.0-cdh5.5.2.jar



# OK, that's all we need - start tiling.
STARTTIME=$(date +%s)
echo Starting tiling

echo Run command: >> intra-edge-tiling.log
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
	>> intra-edge-tiling.log
echo >> intra-edge-tiling.log
echo >> intra-edge-tiling.log

if [ "${DEBUG}" != "true" ]; then
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
		|& tee -a intra-edge-tiling.log
fi



ENDTIME=$(date +%s)



cleanupConfigFile ${OUTPUT_COPIED} output.conf
cleanupConfigFile ${TILING_COPIED} tiling.conf
cleanupConfigFile ${GRAPH__COPIED} graph.conf



echo >> intra-edge-tiling.log
echo >> intra-edge-tiling.log
echo Start time: ${STARTTIME} >> intra-edge-tiling.log
echo End time: ${ENDTIME} >> intra-edge-tiling.log
echo Elapsed time: $(( ${ENDTIME} - ${STARTTIME} )) seconds >> intra-edge-tiling.log

echo
echo
echo Done at `date`
echo Elapsed time: $(( ${ENDTIME} - ${STARTTIME} )) seconds

popd
