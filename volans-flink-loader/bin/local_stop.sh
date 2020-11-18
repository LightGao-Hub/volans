#!/bin/bash
DIR=$(cd `dirname $0`; cd ..; pwd)
cd $DIR
ROOT_HOME=$DIR;export ROOT_HOME

# $1 flink作业id
flink cancel \
-m localhost:8081 \
-s $ROOT_HOME/savapoints $1