#!/bin/bash
DIR=$(cd `dirname $0`; cd ..; pwd)
cd $DIR
ROOT_HOME=$DIR;export ROOT_HOME
LIB_HOME=$DIR/lib;export LIB_HOME

extLibInfo=`find $LIB_HOME -type f -name *.jar | awk '{print "-C file://"$1}' | xargs`

# $1 保存点路径
flink run \
$extLibInfo \
-yt $LIB_HOME \
-s $1 \
-m yarn-cluster \
-p 3 \
-yjm 1024m \
-ytm 1024m \
-ys 1 \
-c com.haizhi.volans.loader.scala.StartFlinkLoader \
$LIB_HOME/volans-flink-loader-1.0.0.jar \
-input hdfs://localhost:9000/volans/参数配置.json
