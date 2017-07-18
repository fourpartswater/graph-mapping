#!/bin/bash



# Include any needed common scripts
SOURCE_LOCATION=$( cd $( dirname ${BASH_SOURCE[0]} ) && pwd )
. ${SOURCE_LOCATION}/arg-parser.sh



# Set up default parameters
MAIN_JAR=../graph-mapping-0.1-SNAPSHOT/lib/graph-mapping.jar

# Set up application-specific parameters
# Switch main classes in order to debug configuration
MAIN_CLASS=software.uncharted.graphing.tiling.EdgeTilingPipeline
# MAIN_CLASS=software.uncharted.graphing.config.ConfigurationTester
APPLICATION_NAME="Inter-community edge tiling pipeline"



# Parse input arguments
parseArguments graph-inter-edges- -salt "$@"



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

EXTRA_DRIVER_JAVA_OPTS="-Dgraph.edges.type=inter"
EXTRA_DRIVER_JAVA_OPTS="${EXTRA_DRIVER_JAVA_OPTS} -Dtiling.source=$( relativeToSource ${DATASET} layout )"
EXTRA_DRIVER_JAVA_OPTS="${EXTRA_DRIVER_JAVA_OPTS} $( getLevelConfig ${LEVELS[@]} )"
EXTRA_DRIVER_JAVA_OPTS="${EXTRA_DRIVER_JAVA_OPTS} ${OTHER_ARGS}"

echo DATATABLE: ${DATATABLE}
echo MAX_LEVEL: ${MAX_LEVEL}
echo PARTITIONS: ${PARTITIONS}
echo EXECUTORS: ${EXECUTORS}
echo LEVELS: ${LEVELS[@]}
echo Extra java args: ${EXTRA_DRIVER_JAVA_OPTS}

echo DATATABLE: ${DATATABLE} > inter-edge-tiling.log
echo MAX_LEVEL: ${MAX_LEVEL} >> inter-edge-tiling.log
echo PARTITIONS: ${PARTITIONS} >> inter-edge-tiling.log
echo EXECUTORS: ${EXECUTORS} >> inter-edge-tiling.log
echo LEVELS: ${LEVELS[@]} >> inter-edge-tiling.log
echo Extra java args: ${EXTRA_DRIVER_JAVA_OPTS} >> inter-edge-tiling.log

# echo
# echo Removing old tile set
# echo "disable '${DATATABLE}'" > clear-hbase-table
# echo "drop '${DATATABLE}'" >> clear-hbase-table
#
# hbase shell < clear-hbase-table



# See if there is a local inter-community edge configuration file
if [ -e inter-edge-tiling.conf ]; then
	CONFIGURATION=inter-edge-tiling.conf
else
	CONFIGURATION=
fi



# Extra jars needed by tiling processes
HBASE_VERSION=1.0.0-cdh5.5.2
HBASE_HOME=/opt/cloudera/parcels/CDH/lib/hbase/lib
EXTRA_JARS=${HBASE_HOME}/htrace-core-3.2.0-incubating.jar:${HBASE_HOME}/hbase-client-${HBASE_VERSION}.jar:${HBASE_HOME}/hbase-common-${HBASE_VERSION}.jar:${HBASE_HOME}/hbase-protocol-${HBASE_VERSION}.jar:${HBASE_HOME}/hbase-server-${HBASE_VERSION}.jar



# OK, that's all we need - start tiling.
STARTTIME=$(date +%s)
echo Starting tiling

if [ "${CLUSTER}" == "true" ]; then
    echo Deploying in cluster mode

    EDJO=
    EDJO="${EDJO} -Ds3Output.awsAccessKey=${AWS_ACCESS_KEY}"
    EDJO="${EDJO} -Ds3Output.awsSecretKey=${AWS_SECRET_KEY}"
    EXTRA_DRIVER_JAVA_OPTS="${EXTRA_DRIVER_JAVA_OPTS} ${EDJO}"

    DEPLOY_MODE=cluster
    DISTRIBUTED_FILES="output.conf,tiling.conf,graph.conf,${CONFIGURATION}"
else
    echo Deploying in client mode
    DEPLOY_MODE=client
    DISTRIBUTED_FILES=""
fi

echo Run command: >> inter-edge-tiling.log
echo spark-submit \
    --num-executors ${EXECUTORS} \
    --executor-memory 10g \
    --executor-cores 4 \
    --conf spark.executor.extraClassPath=${EXTRA_JARS} \
    --driver-class-path ${EXTRA_JARS} \
    --jars `echo ${EXTRA_JARS} | tr : ,` \
    --class ${MAIN_CLASS} \
    --master yarn \
    --deploy-mode ${DEPLOY_MODE} \
    --conf spark.yarn.dist.files="${DISTRIBUTED_FILES}" \
    --conf "spark.driver.extraJavaOptions=${EXTRA_DRIVER_JAVA_OPTS}" \
    ${MAIN_JAR} \
    output.conf tiling.conf graph.conf \
    ${CONFIGURATION} \
    >> inter-edge-tiling.log
echo >> inter-edge-tiling.log
echo >> inter-edge-tiling.log

if [ "${DEBUG}" != "true" ]; then
    spark-submit \
        --num-executors ${EXECUTORS} \
        --executor-memory 10g \
        --executor-cores 4 \
        --conf spark.executor.extraClassPath=${EXTRA_JARS} \
        --driver-class-path ${EXTRA_JARS} \
        --jars `echo ${EXTRA_JARS} | tr : ,` \
        --class ${MAIN_CLASS} \
        --master yarn \
        --deploy-mode ${DEPLOY_MODE} \
        --conf spark.yarn.dist.files="${DISTRIBUTED_FILES}" \
        --conf "spark.driver.extraJavaOptions=${EXTRA_DRIVER_JAVA_OPTS}" \
        ${MAIN_JAR} \
        output.conf tiling.conf graph.conf \
        ${CONFIGURATION} \
        |& tee -a inter-edge-tiling.log
fi


ENDTIME=$(date +%s)



cleanupConfigFile ${OUTPUT_COPIED} output.conf
cleanupConfigFile ${TILING_COPIED} tiling.conf
cleanupConfigFile ${GRAPH__COPIED} graph.conf



echo >> inter-edge-tiling.log
echo >> inter-edge-tiling.log
echo Start time: ${STARTTIME} >> inter-edge-tiling.log
echo End time: ${ENDTIME} >> inter-edge-tiling.log
echo Elapsed time: $(( ${ENDTIME} - ${STARTTIME} )) seconds >> inter-edge-tiling.log

echo
echo
echo Done at `date`
echo Elapsed time: $(( ${ENDTIME} - ${STARTTIME} )) seconds

popd
