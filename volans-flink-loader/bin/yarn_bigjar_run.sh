#!/bin/bash
flink run -m yarn-cluster -p 3 -d \
-yjm 1024m -ytm 1524m  -ys 3  \
-c com.haizhi.volans.loader.scala.StartFlinkLoader \
volans-flink-loader-*.jar \
-input $1
