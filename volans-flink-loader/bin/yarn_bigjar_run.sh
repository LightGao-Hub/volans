#!/bin/bash
flink run -m yarn-cluster -p 3 -d \
-yjm 1024m -ytm 1524m  -ys 3  \
-c com.haizhi.volans.loader.scala.StartFlinkLoader \
volans-flink-loader-1.0.0.jar \
-input $1
