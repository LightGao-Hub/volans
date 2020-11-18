#!/bin/bash

# $1 savepoint 路径， $2 flink作业id  $3 yarn 作业id
flink cancel \
-m yarn-cluster \
-s $1 $2 \
-yid $3

yarn application -kill $3