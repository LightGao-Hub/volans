#!/bin/bash
DIR=$(cd `dirname $0`; cd ..; pwd)
cd $DIR
ROOT_HOME=$DIR;export ROOT_HOME
LIB_HOME=$DIR/lib;export LIB_HOME

<<notes
处理hive-storage、hive-exec jar包冲突问题
将hive-storage提取出来放在extLibInfo前面，本地程序默认先加载storage包
notes
extLibInfo=`find $LIB_HOME -type f -name *.jar | grep -v "hive-storage" | awk '{print "-C file://"$1}' | xargs`
hiveStorageJar=`find $LIB_HOME -type f -name *hive-storage* | xargs -I {} echo "-C file://"{}" "`
extLibInfo="$hiveStorageJar$extLibInfo"

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
-input $2
