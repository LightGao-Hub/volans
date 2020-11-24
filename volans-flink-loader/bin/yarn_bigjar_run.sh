#!/bin/bash
flink run -m yarn-cluster -p 3 \
-yjm 1024m -ytm 1524m  -ys 3  \
-c com.haizhi.volans.loader.scala.StartFlinkLoader \
volans-flink-loader-1.0.0.jar \
-input hdfs://localhost:9000/tmp/volans/参数配置.json
