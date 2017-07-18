#!/bin/bash

MEM=4g
MAIN_JAR=../graph-mapping-0.1-SNAPSHOT/lib/graph-mapping.jar
SCALA_JAR=/opt/scala-2.11.7/lib/scala-library.jar
MAIN_CLASS=software.uncharted.graphing.clustering.unithread.Community



DATASET=

while [ "$1" != "" ]; do
	case $1 in
		-d | --dataset )
			shift
			DATASET=$1
			;;
	esac
	shift
done

if [ "${DATASET}" == "" ]; then
	echo No dataset specified
	exit
fi

pushd ${DATASET}



if [ -e metadata.bin ]
then
	ARGS="-i edges.bin -m metadata.bin -l -1 -v true"
else
	ARGS="-i edges.bin -l -1 -v true"
fi

if [ -e weights.bin ]
then
        ARGS="${ARGS} -w weights.bin"
fi

case ${DATASET} in

    example)
            ARGS="${ARGS} -nd 10"
            ARGS="${ARGS} -a software.uncharted.graphing.analytics.BucketAnalytic config/grant-analytics.conf"
            ;;


esac



echo
echo Running in `pwd`
echo Starting at `date`
STARTTIME=$(date +%s)

echo Removing old results ...
rm -rf level_*

echo Clustering ...
echo java -cp ${MAIN_JAR}:${SCALA_JAR} -Xmx${MEM} ${MAIN_CLASS} ${ARGS}
echo java -cp ${MAIN_JAR}:${SCALA_JAR} -Xmx${MEM} ${MAIN_CLASS} ${ARGS} > cluster.log
java -cp ${MAIN_JAR}:${SCALA_JAR} -Xmx${MEM} ${MAIN_CLASS} ${ARGS} |& tee -a cluster.log
ENDTIME=$(date +%s)

echo >> cluster.log
echo >> cluster.log

echo Elapsed time: $(( ${ENDTIME} - ${STARTTIME} )) seconds >> cluster.log

echo Done at `date`
echo Elapsed time: $(( ${ENDTIME} - ${STARTTIME} )) seconds

popd
