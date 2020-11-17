#启动命令
nohup flink run -m yarn-cluster -p 3 -yjm 1024m -ytm 1024m -ys 1 -yn 3 flink_Interface_warning-1.0.jar >> /datalog/flink_Interface_warning/log.log 2>&1 &
flink run -m yarn-cluster volans-flink-loader-1.0.0.jar /Users/hzxt/project/IDEA_HZXT/volans-flink/volans/volans-flink-loader/src/main/resources/参数配置.json

#停止命令，通过保存点停止作业 : 需要yarn_job_id  还需要flink作业id
flink cancel -m yarn-cluster -s hdfs://nameservice1/flink/savepoints 65a8b1aa3443a17454261a593351af57  -yid application_1586509094298_12112
yarn application -kill application_1586509094298_12134

#通过保存点路径恢复作业
flink run -s hdfs://nameservice1/flink/savepoints/savepoint-65a8b1-f12e504bd2db -m yarn-cluster -p 3 -yjm 1024m -ytm 1024m -ys 1 -yn 3 flink_Interface_warning-1.0.jar