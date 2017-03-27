#!/bin/bash



# Include any needed common scripts
SOURCE_LOCATION=$( cd $( dirname ${BASH_SOURCE[0]} ) && pwd )
. ${SOURCE_LOCATION}/arg-parser.sh



MAIN_JAR=../xdata-graph-0.1-SNAPSHOT/lib/xdata-graph.jar
MAIN_CLASS=software.uncharted.graphing.layout.ClusteredGraphLayoutApp
BASE_LOCATION=/user/${USER}/graphs



DATASET=
REMOVE_EXISTING=false

while [ "$1" != "" ]; do
	case $1 in
		-d | --dataset )
			shift
			DATASET=$1
			;;
        -cluster )
            export CLUSTER=true
            ;;
		-r | --refresh )
			REMOVE_EXISTING=true
	esac
	shift
done

if [ "${DATASET}" == "" ]; then
	echo No dataset specified
	exit
fi



# copy our config files into the dataset, if they're not already there
CONFIG_COPIED=$(checkConfigFile ${SOURCE_LOCATION}/config/default-layout.conf ${DATASET}/layout.conf)



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

echo Checking base location
if [ 0 -eq "$(countHDFSFiles ${BASE_LOCATION})" ]; then
	echo Base location ${BASE_LOCATION} does not exists!
	exit
fi

echo Checking dataset
if [ "${DATASET}" == "" ]; then
	echo No dataset specified
	exit
fi

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
export PARTS

EXTRA_DRIVER_JAVA_OPTS=""
if [ "${CLUSTER}" == "true" ]; then
    echo Deploying in cluster mode

    DEPLOY_MODE=cluster
    DISTRIBUTED_FILES="layout.conf"
else
    echo Deploying in client mode
    DEPLOY_MODE=client
    DISTRIBUTED_FILES=""
fi

echo spark-submit \
    --num-executors ${EXECUTORS} \
    --executor-memory 10g \
    --executor-cores 4 \
    --class ${MAIN_CLASS} \
    --master yarn \
    --deploy-mode ${DEPLOY_MODE} \
    --conf spark.yarn.dist.files="${DISTRIBUTED_FILES}" \
    --conf "spark.driver.extraJavaOptions=${EXTRA_DRIVER_JAVA_OPTS}" \
    ${MAIN_JAR} \
    debug layout.conf >> layout.log

spark-submit \
    --num-executors ${EXECUTORS} \
    --executor-memory 10g \
    --executor-cores 4 \
    --class ${MAIN_CLASS} \
    --master yarn \
    --deploy-mode ${DEPLOY_MODE} \
    --conf spark.yarn.dist.files="${DISTRIBUTED_FILES}" \
    --conf "spark.driver.extraJavaOptions=${EXTRA_DRIVER_JAVA_OPTS}" \
    ${MAIN_JAR} \
    debug layout.conf >> layout.log

# Note: Took out -spark yarn-client.  Should be irrelevant, but noted just in case I'm wrong.

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

popd
