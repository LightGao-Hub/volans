package com.haizhi.volans.sink.component

import java.io.OutputStream
import java.nio.charset.Charset
import java.util.Properties

import com.haizhi.volans.sink.config.constant.{HiveStoreType, StoreType}
import com.haizhi.volans.sink.config.schema.SchemaVo
import com.haizhi.volans.sink.config.store.StoreHiveConfig
import com.haizhi.volans.sink.server.HiveDao
import com.haizhi.volans.sink.util.{AvroUtils, HiveUtils, OrcUtils}
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.flink.api.common.serialization.{BulkWriter, SimpleStringEncoder}
import org.apache.flink.core.fs.Path
import org.apache.flink.formats.parquet.avro.ParquetAvroWriters
import org.apache.flink.orc.OrcSplitReaderUtil
import org.apache.flink.orc.vector.{RowDataVectorizer, Vectorizer}
import org.apache.flink.orc.writer.OrcBulkWriterFactory
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.BasePathBucketAssigner
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy
import org.apache.flink.streaming.api.functions.sink.filesystem.{BucketAssigner, StreamingFileSink}
import org.apache.flink.table.data.RowData
import org.apache.flink.table.types.logical.{LogicalType, RowType}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hive.metastore.api.Table
import org.apache.orc.TypeDescription
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
  // 表存储类型
  private var tableStoredType: String = _
  // 表字段schema -> List[(field,type)]
  private var fieldSchemaList: List[(String, String)] = _
  // Avro Schema
  private var avroSchemaStr: String = _
  // Vectorizer
  private var vectorizer: Vectorizer[RowData] = _
  // 是否需要处理文件
  private var needHandleFile: Boolean = true

  /**
   * 初始化
   */
  def init(): Unit = {
    this.hiveDao = new HiveDao()
    this.table = hiveDao.getTable(storeConfig.database, storeConfig.table)
    // 获取包括partitionKey在内的Hive表字段schema信息
    this.fieldSchemaList = HiveUtils.getAllFieldSchema(table)
    // config.put("fieldSchemaList", fieldSchemaList)
    logger.info(s"hive fieldSchema list: $fieldSchemaList")
    // 如果未开启文件合并功能且不是分区表，则不需要启动CombinerFileJob
    if (!storeConfig.rollingPolicy.rolloverEnable && HiveUtils.getPartitionKeys(table).size == 0) {
      needHandleFile = false
    }
    this.tableStoredType = HiveUtils.getTableStoredType(table)
    // 检查storeType，目前支持的存储格式为：textFile、orc、parquet
    if (!HiveStoreType.ORC.equals(tableStoredType) && !HiveStoreType.PARQUET.eq(tableStoredType)
      && !HiveStoreType.TEXTFILE.equals(tableStoredType)) {
      throw new Exception(s"不支持存储格式: [$tableStoredType], 目前仅支持的存储格式为：textFile、orc、parquet")
    }
    if (HiveStoreType.ORC.equals(tableStoredType)) {
      // Hive表字段schema转LogicalType
      val logicalTypes: Array[LogicalType] = OrcUtils.convertHiveFieldToLogicalType(fieldSchemaList)
      val fieldNames = fieldSchemaList.map(_._1).toArray
      val typeDescription: TypeDescription = OrcSplitReaderUtil.logicalTypeToOrcType(
        RowType.of(logicalTypes, fieldNames))
      this.vectorizer = new RowDataVectorizer(typeDescription.toString, logicalTypes)
      logger.info(s"convert to orc schema: $typeDescription")
    } else {
      // Hive表字段schema转Avro Schema
      avroSchemaStr = AvroUtils.convertHiveFieldToAvro(fieldSchemaList)
      // config.put("avroSchema", avroSchemaStr)
      logger.info(s"convert to avro schema: $avroSchemaStr")
    }
  }

  /**
   * 构建HiveSink(text,parquet)，数据源格式统一为GenericRecord
   *
   * @return
   */
  private def buildHiveSink: StreamingFileSink[GenericRecord] = {
    // 分区字段
    val partitionKeys = HiveUtils.getPartitionKeys(table)
    // 非分区字段
    val fieldSchema = HiveUtils.getFieldSchema(table)
    // 字段分隔符
    val fieldDelimited = HiveUtils.getFieldDelimited(table)
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

    var streamSink: StreamingFileSink[GenericRecord] = null
    // 工作目录
    val workDir = new Path(HiveUtils.getTableLocation(table))
    // parquet
    if (HiveStoreType.PARQUET.equals(tableStoredType)) {
      val writerFactory: BulkWriter.Factory[GenericRecord] =
        ParquetAvroWriters.forGenericRecord(new Schema.Parser().parse(this.avroSchemaStr))
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
    streamSink
  }

  /**
   * 构建HiveSink(orc)，数据源格式为RowData
   *
   * @return
   */
  def buildHiveOrcSink: StreamingFileSink[RowData] = {
    // 分区字段
    val partitionSchema = HiveUtils.getPartitionSchema(table)
    // 自定义桶分配器
    val bucketAssigner: BasePathBucketAssigner[RowData] = new BasePathBucketAssigner[RowData]() {
      override def getBucketId(element: RowData, context: BucketAssigner.Context): String = {
        val bucketIdBuffer = new StringBuilder()
        // 如果是分区表，则拼接分区字段作为桶目录
        if (partitionSchema != null && partitionSchema.size > 0) {
          // 分区字段元素在RowData数组中的起点位置
          var startPos = element.getArity - partitionSchema.size
          for (i <- 0 until partitionSchema.size) {
            val partitionkey = partitionSchema(i)._1
            val fieldType = partitionSchema(i)._2
            bucketIdBuffer.append(partitionkey)
              .append("=")
              .append(OrcUtils.getRowDataValue(element, fieldType, startPos))
              .append(java.io.File.separator)
            startPos += 1
          }
          if (bucketIdBuffer.size > 0) {
            bucketIdBuffer.deleteCharAt(bucketIdBuffer.size - 1)
          }
        }
        bucketIdBuffer.toString()
      }
    }

    // 工作目录
    val workDir = new Path(HiveUtils.getTableLocation(table))

    val writerFactory: OrcBulkWriterFactory[RowData] = new OrcBulkWriterFactory(this.vectorizer, new Properties(), new Configuration())
    StreamingFileSink
      .forBulkFormat(workDir, writerFactory)
      .withBucketAssigner(bucketAssigner)
      .withRollingPolicy(OnCheckpointRollingPolicy.build())
      .build()
  }

  override def build[T](v: T): RichSinkFunction[T] = {
    if (HiveStoreType.ORC.equals(tableStoredType)) {
      this.buildHiveOrcSink.asInstanceOf[RichSinkFunction[T]]
    } else {
      this.buildHiveSink.asInstanceOf[RichSinkFunction[T]]
    }
  }

  def getAvroSchema(): String = {
    this.avroSchemaStr
  }

  def getFieldSchemaList(): List[(String, String)] = {
    this.fieldSchemaList
  }

  def getTableStoredType(): String = {
    this.tableStoredType
  }

  /**
   * 是否需要启动CombinerFileJob（合并文件、提交分区）
   *
   * @return
   */
  def needCombinerFileJob: Boolean = {
    this.needHandleFile
  }

}
