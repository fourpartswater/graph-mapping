#!/bin/bash

MEM=12g
MAIN_JAR=../../../build/distributions/xdata-graph-0.1-SNAPSHOT/lib/xdata-graph.jar
SCALA_JAR=/opt/scala-2.11.7/lib/scala-library.jar
MAIN_CLASS=software.uncharted.graphing.clustering.unithread.Convert



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



case ${DATASET} in

    example)
        echo Converting ${DATASET}
        CONVERT_ARGS="-ie edges -ce \\t -s 0 -d 1 -oe edges.bin"
        CONVERT_ARGS="${CONVERT_ARGS} -in nodes -cn \\t -n 1 -m 0 -om metadata.bin"
        CONVERT_ARGS="${CONVERT_ARGS} -anc software.uncharted.graphing.analytics.BucketAnalytic:../config/grant-analytics.conf"
        MEM=64g
        ;;


esac



rm edges.bin
rm metadata.bin

echo
echo Running in `pwd`
echo Starting at `date`
STARTTIME=$(date +%s)
echo java -cp ${MAIN_JAR}:${SCALA_JAR} -Xmx${MEM} ${MAIN_CLASS} ${CONVERT_ARGS} |& tee convert.log
echo
java -cp ${MAIN_JAR}:${SCALA_JAR} -Xmx${MEM} ${MAIN_CLASS} ${CONVERT_ARGS} |& tee convert.log
ENDTIME=$(date +%s)

echo >> convert.log
echo >> convert.log
echo Elapsed time: $(( ${ENDTIME} - ${STARTTIME} )) seconds >> convert.log

echo Done at `date`
echo Elapsed time: $(( ${ENDTIME} - ${STARTTIME} )) seconds

popd
