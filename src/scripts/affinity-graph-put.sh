#!/bin/bash

export BASE=/user/nkronenfeld/affinity-graph

hdfs dfs -rm -r -skipTrash ${BASE}/ag-layout
hdfs dfs -rm -r -skipTrash ${BASE}/ag-levels
hdfs dfs -mkdir ${BASE}/ag-levels
hdfs dfs -put level_* ${BASE}/ag-levels

