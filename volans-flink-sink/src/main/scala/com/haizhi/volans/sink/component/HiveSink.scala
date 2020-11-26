package com.haizhi.volans.sink.component

import java.io.OutputStream
import java.nio.charset.Charset
import java.util.Properties

import com.haizhi.volans.sink.combiner.{CombineFileJob, CommitFileJob}
import com.haizhi.volans.sink.config.constant.{HiveStoreType, StoreType}
import com.haizhi.volans.sink.config.schema.SchemaVo
import com.haizhi.volans.sink.config.store.StoreHiveConfig
import com.haizhi.volans.sink.server.HiveDao
import com.haizhi.volans.sink.util.AvroUtils
import com.haizhi.volans.sink.writer.orc.OrcWriters
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.flink.api.common.serialization.{BulkWriter, SimpleStringEncoder}
import org.apache.flink.core.fs.Path
import org.apache.flink.formats.parquet.avro.ParquetAvroWriters
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.BasePathBucketAssigner
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy
import org.apache.flink.streaming.api.functions.sink.filesystem.{BucketAssigner, StreamingFileSink}
import org.apache.hadoop.hive.metastore.api.Table
import org.slf4j.LoggerFactory

/**
 * Author pengxb
 * Date 2020/11/16
 */
class HiveSink(override var storeType: StoreType,
               var storeConfig: StoreHiveConfig,
               var schemaVo: SchemaVo)
  extends Sink with Serializable {

  private val logger = LoggerFactory.getLogger(classOf[HiveSink])
  override var uid: String = "Hive"
  private var hiveDao: HiveDao = _
  private var table: Table = _
  // Avro Schema
  private var avroSchemaStr: String = _

  /**
   * 初始化
   */
  def init(): Unit = {
    hiveDao = new HiveDao()
    table = hiveDao.getTable(storeConfig.database, storeConfig.table)
    // 获取包括partitionKey在内的Hive表字段schema信息
    val fieldSchemaWithPartitionKeyList = hiveDao.getAllFieldSchema(table)
    logger.info(s"hive fieldSchema list: $fieldSchemaWithPartitionKeyList")
    // Hive表字段schema转Avro Schema
    avroSchemaStr = AvroUtils.convertHiveFieldToAvro(fieldSchemaWithPartitionKeyList)
    config.setProperty("avroSchema", avroSchemaStr)
    logger.info(s"convert to avro schema: $avroSchemaStr")
  }

  /**
   * 构建HiveSink
   *
   * @return
   */
  private def buildHiveSink: StreamingFileSink[GenericRecord] = {
    val tableStoreType = hiveDao.getStoreType(table)
    logger.info(s"table stored type: $tableStoreType")
    // 检查storeType，目前支持的存储格式为：textFile、orc、parquet
    if (!HiveStoreType.ORC.equals(tableStoreType) && !HiveStoreType.PARQUET.eq(tableStoreType)
      && !HiveStoreType.TEXTFILE.equals(tableStoreType)) {
      throw new Exception(s"不支持存储格式: [$tableStoreType], 目前仅支持的存储格式为：textFile、orc、parquet")
    }

    // 分区字段
    val partitionKeys = hiveDao.getPartitionKeys(table)
    // 非分区字段
    val fieldSchema = hiveDao.getFieldSchema(table)
    // 字段分隔符
    val fieldDelimited = hiveDao.getFieldDelimited(table)

    // 自定义桶分配器
    val bucketAssigner: BasePathBucketAssigner[GenericRecord] = new BasePathBucketAssigner[GenericRecord]() {
      override def getBucketId(element: GenericRecord, context: BucketAssigner.Context): String = {
        val bucketIdBuffer = new StringBuilder()
        // 如果是分区表，则拼接分区字段作为桶目录
        if (partitionKeys != null && partitionKeys.size > 0) {
          for (partitionKey <- partitionKeys) {
            bucketIdBuffer.append(partitionKey)
              .append("=")
              .append(element.get(partitionKey))
              .append(java.io.File.separator)
          }
          if (bucketIdBuffer.size > 0) {
            bucketIdBuffer.deleteCharAt(bucketIdBuffer.size - 1)
          }
        }
        bucketIdBuffer.toString()
      }
    }

    // 工作目录
    val workDir = new Path(storeConfig.rollingPolicy.workDir)
    //    if (!storeConfig.rollingPolicy.rolloverEnable) {
    //      // 如果不开启文件滚动策略，默认工作目录为表数据存储路径
    //      workDir = new Path(table.getSd.getLocation)
    //    }

    var streamSink: StreamingFileSink[GenericRecord] = null

    // orc、parquet
    if (HiveStoreType.ORC.equals(tableStoreType) || HiveStoreType.PARQUET.equals(tableStoreType)) {
      val writerFactory: BulkWriter.Factory[GenericRecord] =
        if (HiveStoreType.ORC.equals(tableStoreType)) {
          OrcWriters.forGenericRecord(avroSchemaStr, new Properties())
        } else {
          ParquetAvroWriters.forGenericRecord(new Schema.Parser().parse(avroSchemaStr))
        }
      streamSink = StreamingFileSink
        .forBulkFormat(workDir, writerFactory)
        .withBucketAssigner(bucketAssigner)
        .withRollingPolicy(OnCheckpointRollingPolicy.build())
        .build()
    } else {
      // textFile
      streamSink = StreamingFileSink.forRowFormat(workDir,
        new SimpleStringEncoder[GenericRecord]("UTF-8") {
          override def encode(element: GenericRecord, stream: OutputStream): Unit = {
            val buffer = new StringBuilder()
            for (fieldSchema <- fieldSchema) {
              buffer.append(element.get(fieldSchema._1))
                .append(fieldDelimited)
            }
            if (buffer.size > 0) {
              buffer.deleteCharAt(buffer.size - 1)
            }
            val charset = Charset.forName("UTF-8")
            stream.write(buffer.toString.getBytes(charset))
            stream.write('\n')
          }
        })
        .withBucketAssigner(bucketAssigner)
        .withRollingPolicy(OnCheckpointRollingPolicy.build())
        .build()
    }

    if (storeConfig.rollingPolicy.rolloverEnable) {
      // 启动文件合并线程
      CombineFileJob.init(storeConfig)
      CombineFileJob.start()
    } else {
      // 启动文件提交线程，不合并文件
      CommitFileJob.init(storeConfig)
      CommitFileJob.start()
    }
    streamSink
  }

  override def build[T](v: T): RichSinkFunction[T] = {
    this.buildHiveSink.asInstanceOf[RichSinkFunction[T]]
  }

  def getAvroSchema(): String = {
    avroSchemaStr
  }

}
