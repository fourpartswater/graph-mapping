#!/bin/bash

MEM=4g
MAIN_JAR=../xdata-graph-0.1-SNAPSHOT/lib/xdata-graph.jar
SCALA_JAR=/opt/scala-2.10.4/lib/scala-library.jar
MAIN_CLASS=software.uncharted.graphing.clustering.unithread.Community

DATASET=$1


pushd ${DATASET}

if [ -e metadata.bin ]
then
	ARGS="edges.bin -m metadata.bin -l -1 -v"
else
	ARGS="edges.bin -l -1 -v"
fi

if [ "${DATASET}" = "affinity-nd" ]
then
	ARGS="${ARGS} -nd 10"
fi

if [ "${DATASET}" = "affinity-cs" ]
then
	ARGS="${ARGS} -cs 20"
fi


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
