#!/bin/bash

# Include any needed common scripts
SOURCE_LOCATION=$( cd $( dirname ${BASH_SOURCE[0]} ) && pwd )
. ${SOURCE_LOCATION}/arg-parser.sh

MEM=2g
MAIN_JAR=../../../build/distributions/xdata-graph-0.1-SNAPSHOT/lib/xdata-graph.jar
SCALA_JAR=/opt/scala-2.11.7/lib/scala-library.jar
MAIN_CLASS=software.uncharted.graphing.clustering.unithread.Convert

DATASET=patent-sample

# copy our config files into the dataset, if they're not already there
CONFIG_COPIED=$(checkConfigFile ${SOURCE_LOCATION}/config/default-layout.conf ${DATASET}/layout.conf)
OUTPUT_COPIED=$(checkConfigFile ${SOURCE_LOCATION}/config/default-output.conf ${DATASET}/output.conf)
TILING_COPIED=$(checkConfigFile ${SOURCE_LOCATION}/config/default-tiling.conf ${DATASET}/tiling.conf)
GRAPH__COPIED=$(checkConfigFile ${SOURCE_LOCATION}/config/default-graph.conf  ${DATASET}/graph.conf)

pushd ${DATASET}

echo Converting ${DATASET}
CONVERT_ARGS="-ie edges -ce \\t -s 0 -d 1 -oe edges.bin"
CONVERT_ARGS="${CONVERT_ARGS} -in nodes -cn \\t -n 1 -m 0 -om metadata.bin"

echo Removing existing output
rm edges.bin
rm metadata.bin

echo
echo Running in `pwd`
echo Starting at `date`
STARTTIME=$(date +%s)
java -cp ${MAIN_JAR}:${SCALA_JAR} -Xmx${MEM} ${MAIN_CLASS} ${CONVERT_ARGS} |& tee convert.log
ENDTIME=$(date +%s)

echo >> convert.log
echo >> convert.log
echo Elapsed time: $(( ${ENDTIME} - ${STARTTIME} )) seconds >> convert.log

echo Done at `date`
echo Elapsed time: $(( ${ENDTIME} - ${STARTTIME} )) seconds

MAIN_CLASS=software.uncharted.graphing.clustering.unithread.Community
ARGS="-i edges.bin -m metadata.bin -l -1 -v true -nd 10"

echo
echo Running in `pwd`
echo Starting at `date`
STARTTIME=$(date +%s)

echo Removing old results ...
rm -rf level_*

echo Clustering ...
java -cp ${MAIN_JAR}:${SCALA_JAR} -Xmx${MEM} ${MAIN_CLASS} ${ARGS} |& tee -a cluster.log
ENDTIME=$(date +%s)

echo >> cluster.log
echo >> cluster.log

echo Elapsed time: $(( ${ENDTIME} - ${STARTTIME} )) seconds >> cluster.log

echo Done at `date`
echo Elapsed time: $(( ${ENDTIME} - ${STARTTIME} )) seconds


MAIN_CLASS=software.uncharted.graphing.layout.ClusteredGraphLayoutApp
BASE_LOCATION=/user/${USER}/graphs

REMOVE_EXISTING=true

echo
echo Running layout in `pwd`
echo Starting at `date`

MAX_LEVEL=`ls -d level_* | awk -F'_' '{print $2}' | sort -nr | head -n1`
MAX_SIZE=5
PARTITIONS=1
EXECUTORS=1

echo MAX_LEVEL: ${MAX_LEVEL}
echo MAX_SIZE: ${MAX_SIZE}
echo PARTITIONS: ${PARTITIONS}
echo EXECUTORS: ${EXECUTORS}

echo MAX_LEVEL: ${MAX_LEVEL} > layout.log
echo MAX_SIZE: ${MAX_SIZE} >> layout.log
echo PARTITIONS: ${PARTITIONS} >> layout.log
echo EXECUTORS: ${EXECUTORS} >> layout.log

TIMEA=$(date +%s)

# Count the number of files that match the given name at the given location in HDFS
function countHDFSFiles {
	LOCATION=$1
	RESULTS=`hdfs dfs -ls -d ${LOCATION} | wc | awk '{print $1}'`
	if [ "" == "${RESULTS}" ]; then
		echo 0
	else
		echo ${RESULTS}
	fi
}

echo Checking file existence
COPY_LOCAL=0
if [ 0 -eq "$(countHDFSFiles ${BASE_LOCATION}/${DATASET})" ]; then
	hdfs dfs -mkdir ${BASE_LOCATION}/${DATASET}
	COPY_LOCAL=1
else
	LOCAL_CLUSTERS_EXIST=($(countHDFSFiles file:${PWD}/level_*))
	CLUSTERS_EXIST=($(countHDFSFiles ${BASE_LOCATION}/${DATASET}/clusters))
	LAYOUT_EXISTS=($(countHDFSFiles ${BASE_LOCATION}/${DATASET}/layout))

	if [ "${REMOVE_EXISTING}" == "true" -a 1 -eq "${CLUSTERS_EXIST}" -a 0 -lt "${LOCAL_CLUSTERS_EXIST}" ]; then
		hdfs dfs -rm -r ${BASE_LOCATION}/${DATASET}/clusters
		COPY_LOCAL=1
	elif [ 0 -eq "${CLUSTERS_EXIST}" -a 0 -lt "${LOCAL_CLUSTERS_EXIST}" ]; then
		COPY_LOCAL=1
	else
		COPY_LOCAL=0
	fi

	if [ 1 -eq "${LAYOUT_EXISTS}" ]; then
		hdfs dfs -rm -r ${BASE_LOCATION}/${DATASET}/layout
	fi
fi

if [ 1 -eq "${COPY_LOCAL}" ]; then
	echo Pushing to HDFS
	hdfs dfs -mkdir ${BASE_LOCATION}/${DATASET}/clusters
	hdfs dfs -put level_* ${BASE_LOCATION}/${DATASET}/clusters
	hdfs dfs -rm ${BASE_LOCATION}/${DATASET}/clusters/level_*/stats
fi

TIMEB=$(date +%s)

echo Starting layout run

# These variables need to be exported for the config file
export BASE_LOCATION
export DATASET
export MAX_LEVEL
export PARTITIONS

/opt/spark-2.0.1-bin-hadoop2.6/bin/spark-submit \
	--class ${MAIN_CLASS} \
	--num-executors ${EXECUTORS} \
	--executor-cores 4 \
	--executor-memory 10g \
	--master yarn \
	--deploy-mode client \
	${MAIN_JAR} \
	debug layout.conf |& tee -a layout.log

TIMEC=$(date +%s)

cleanupConfigFile ${CONFIG_COPIED} layout.conf

echo >> layout.log
echo >> layout.log
echo Time to upload to HDFS: $(( ${TIMEB} - ${TIMEA} )) seconds >> layout.log
echo Elapsed time for layout: $(( ${TIMEC} - ${TIMEB} )) seconds >> layout.log

echo
echo
echo Done at `date`
echo Time to upload to HDFS: $(( ${TIMEB} - ${TIMEA} )) seconds
echo Elapsed time for layout: $(( ${TIMEC} - ${TIMEB} )) seconds


# Set up application-specific parameters
MAIN_CLASS=software.uncharted.graphing.salt.NodeTilingPipeline
APPLICATION_NAME="Node tiling pipeline"

echo
echo Running ${APPLICATION_NAME}
echo Running in `pwd`
echo Starting at `date`
USER_LEVELS="2 2 2 2 2 2"
LEVELS=(${USER_LEVELS[*]})

DATATABLE=$DATASET
export DATATABLE

EXTRA_DRIVER_JAVA_OPTS=-Dtiling.source=$( relativeToSource ${DATASET} layout )
EXTRA_DRIVER_JAVA_OPTS="${EXTRA_DRIVER_JAVA_OPTS} $( getLevelConfig ${LEVELS[@]} )"

echo LEVELS: ${LEVELS[@]}
echo Extra java args: ${EXTRA_DRIVER_JAVA_OPTS}

echo DATATABLE: ${DATATABLE} > node-tiling.log
echo MAX_LEVEL: ${MAX_LEVEL} >> node-tiling.log
echo PARTITIONS: ${PARTITIONS} >> node-tiling.log
echo EXECUTORS: ${EXECUTORS} >> node-tiling.log
echo LEVELS: ${LEVELS[@]} >> node-tiling.log
echo Extra java args: ${EXTRA_DRIVER_JAVA_OPTS} >> node-tiling.log

# Extra jars needed by tiling processes
HBASE_VERSION=1.0.0-cdh5.5.2
HBASE_HOME=/opt/cloudera/parcels/CDH/lib/hbase/lib
EXTRA_JARS=${HBASE_HOME}/htrace-core-3.2.0-incubating.jar:${HBASE_HOME}/hbase-client-${HBASE_VERSION}.jar:${HBASE_HOME}/hbase-common-${HBASE_VERSION}.jar:${HBASE_HOME}/hbase-protocol-${HBASE_VERSION}.jar:${HBASE_HOME}/hbase-server-${HBASE_VERSION}.jar

# OK, that's all we need - start tiling.
STARTTIME=$(date +%s)
echo Starting tiling

/opt/spark-2.0.1-bin-hadoop2.6/bin/spark-submit \
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

echo >> node-tiling.log
echo >> node-tiling.log
echo Start time: ${STARTTIME} >> node-tiling.log
echo End time: ${ENDTIME} >> node-tiling.log
echo Elapsed time: $(( ${ENDTIME} - ${STARTTIME} )) seconds >> node-tiling.log

echo
echo
echo Done at `date`
echo Elapsed time: $(( ${ENDTIME} - ${STARTTIME} )) seconds


# Set up application-specific parameters
# Switch main classes in order to debug configuration
MAIN_CLASS=software.uncharted.graphing.salt.MetadataTilingPipeline
# MAIN_CLASS=software.uncharted.graphing.config.ConfigurationTester
APPLICATION_NAME="Graph metadata tiling pipeline"



echo
echo Running ${APPLICATION_NAME}
echo Running in `pwd`
echo Starting at `date`

echo DATATABLE: ${DATATABLE} > metadata-tiling.log
echo MAX_LEVEL: ${MAX_LEVEL} >> metadata-tiling.log
echo PARTITIONS: ${PARTITIONS} >> metadata-tiling.log
echo EXECUTORS: ${EXECUTORS} >> metadata-tiling.log
echo LEVELS: ${LEVELS} >> metadata-tiling.log
echo Extra java args: ${EXTRA_DRIVER_JAVA_OPTS} >> metadata-tiling.log

# OK, that's all we need - start tiling
STARTTIME=$(date +%s)
echo Starting tiling

/opt/spark-2.0.1-bin-hadoop2.6/bin/spark-submit \
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
	|& tee -a metadata-tiling.log



ENDTIME=$(date +%s)

echo >> metadata-tiling.log
echo >> metadata-tiling.log
echo Start time: ${STARTTIME} >> metadata-tiling.log
echo End time: ${ENDTIME} >> metadata-tiling.log
echo Elapsed time: $(( ${ENDTIME} - ${STARTTIME} )) seconds >> metadata-tiling.log

echo
echo
echo Done at `date`
echo Elapsed time: $(( ${ENDTIME} - ${STARTTIME} )) seconds


# Set up application-specific parameters
# Switch main classes in order to debug configuration
MAIN_CLASS=software.uncharted.graphing.salt.EdgeTilingPipeline
# MAIN_CLASS=software.uncharted.graphing.config.ConfigurationTester
APPLICATION_NAME="Inter-community edge tiling pipeline"

echo
echo Running ${APPLICATION_NAME}
echo Running in `pwd`
echo Starting at `date`

echo DATATABLE: ${DATATABLE} > inter-edge-tiling.log
echo MAX_LEVEL: ${MAX_LEVEL} >> inter-edge-tiling.log
echo PARTITIONS: ${PARTITIONS} >> inter-edge-tiling.log
echo EXECUTORS: ${EXECUTORS} >> inter-edge-tiling.log
echo LEVELS: ${LEVELS[@]} >> inter-edge-tiling.log
echo Extra java args: ${EXTRA_DRIVER_JAVA_OPTS} >> inter-edge-tiling.log

# OK, that's all we need - start tiling.
STARTTIME=$(date +%s)
echo Starting tiling

/opt/spark-2.0.1-bin-hadoop2.6/bin/spark-submit \
    --num-executors ${EXECUTORS} \
    --executor-memory 10g \
    --executor-cores 4 \
    --conf spark.executor.extraClassPath=${EXTRA_JARS} \
    --driver-class-path ${EXTRA_JARS} \
    --jars `echo ${EXTRA_JARS} | tr : ,` \
    --class ${MAIN_CLASS} \
    --conf "spark.driver.extraJavaOptions=-Dgraph.edges.type=inter ${EXTRA_DRIVER_JAVA_OPTS}" \
    ${MAIN_JAR} \
    output.conf tiling.conf graph.conf \
    ${CONFIGURATION} \
    |& tee -a inter-edge-tiling.log


ENDTIME=$(date +%s)

echo >> inter-edge-tiling.log
echo >> inter-edge-tiling.log
echo Start time: ${STARTTIME} >> inter-edge-tiling.log
echo End time: ${ENDTIME} >> inter-edge-tiling.log
echo Elapsed time: $(( ${ENDTIME} - ${STARTTIME} )) seconds >> inter-edge-tiling.log

echo
echo
echo Done at `date`
echo Elapsed time: $(( ${ENDTIME} - ${STARTTIME} )) seconds

# Set up application-specific parameters
# Switch main classes in order to debug configuration
MAIN_CLASS=software.uncharted.graphing.salt.EdgeTilingPipeline
# MAIN_CLASS=software.uncharted.graphing.config.ConfigurationTester
APPLICATION_NAME="Intra-community edge tiling pipeline"

echo
echo Running ${APPLICATION_NAME}
echo Running in `pwd`
echo Starting at `date`

echo DATATABLE: ${DATATABLE} > intra-edge-tiling.log
echo MAX_LEVEL: ${MAX_LEVEL} >> intra-edge-tiling.log
echo PARTITIONS: ${PARTITIONS} >> intra-edge-tiling.log
echo EXECUTORS: ${EXECUTORS} >> intra-edge-tiling.log
echo LEVELS: ${LEVELS} >> intra-edge-tiling.log
echo Extra java args: ${EXTRA_DRIVER_JAVA_OPTS} >> intra-edge-tiling.log

# OK, that's all we need - start tiling.
STARTTIME=$(date +%s)
echo Starting tiling

/opt/spark-2.0.1-bin-hadoop2.6/bin/spark-submit \
    --num-executors ${EXECUTORS} \
    --executor-memory 10g \
    --executor-cores 4 \
    --conf spark.executor.extraClassPath=${EXTRA_JARS} \
    --driver-class-path ${EXTRA_JARS} \
    --jars `echo ${EXTRA_JARS} | tr : ,` \
    --class ${MAIN_CLASS} \
    --conf "spark.driver.extraJavaOptions=-Dgraph.edges.type=intra ${EXTRA_DRIVER_JAVA_OPTS}" \
    ${MAIN_JAR} \
    output.conf tiling.conf graph.conf \
    ${CONFIGURATION} \
    |& tee -a intra-edge-tiling.log

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
