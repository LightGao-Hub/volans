package com.haizhi.volans.sink.util

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream}

import com.haizhi.volans.sink.config.constant.{AvroFieldType, HiveFieldType}
import com.haizhi.volans.sink.server.HiveDao
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericDatumReader, GenericDatumWriter, GenericRecord}
import org.apache.avro.io.{DatumReader, Decoder, DecoderFactory, Encoder, EncoderFactory}

/**
 * Author pengxb
 * Date 2020/11/24
 */
object AvroUtils {

  /**
   * Hive字段转Avro schema
   *
   * @param fieldSchemaList
   * @return
   */
  def convertHiveFieldToAvro(fieldSchemaList: List[(String, String)]): String = {
    val buffer = new StringBuilder
    for (fieldSchema <- fieldSchemaList) {
      val field = fieldSchema._1
      val fieldType = fieldSchema._2
      buffer.append("{\"name\":\"")
        .append(field)
        .append("\",\"type\":\"")
        .append(hiveFieldTypeToAvro(fieldType))
        .append("\"},")
    }
    if (buffer.size > 0) {
      buffer.deleteCharAt(buffer.size - 1)
    }
    s"""
       | {
       |  "name": "GenericRecord",
       |  "type": "record",
       |  "namespace": "com.haizhi.volans.sink",
       |  "fields": [
       |    ${buffer.toString}
       |  ]
       | }
       |""".stripMargin
  }

  /**
   * Hive数据类型转Avro类型
   *
   * @param fieldType
   * @return
   */
  def hiveFieldTypeToAvro(fieldType: String): String = {
    fieldType.toLowerCase match {
      case HiveFieldType.TINGINT | HiveFieldType.SMALLINT | HiveFieldType.INT => AvroFieldType.INT
      case HiveFieldType.BIGINT => AvroFieldType.LONG
      case HiveFieldType.FLOAT => AvroFieldType.FLOAT
      case HiveFieldType.DOUBLE => AvroFieldType.DOUBLE
      case HiveFieldType.BOOLEAN => AvroFieldType.BOOLEAN
      case HiveFieldType.STRING => AvroFieldType.STRING
      case HiveFieldType.BINARY => AvroFieldType.BYTES
      case HiveFieldType.MAP => AvroFieldType.MAP
      case HiveFieldType.ARRAY => AvroFieldType.ARRAY
      case HiveFieldType.STRUCT => AvroFieldType.RECORD
      case _ => AvroFieldType.STRING
    }
  }

  /**
   * JSON转Avro
   *
   * @param jsonStr
   * @param schemastr
   * @return
   */
  def fromJsonToAvro(jsonStr: String, schemastr: String): Array[Byte] = {
    val schema: Schema = new Schema.Parser().parse(schemastr)
    fromJsonToAvro(jsonStr, schema)
  }

  /**
   * JSON转Avro
   *
   * @param jsonStr
   * @param schema
   * @return
   */
  def fromJsonToAvro(jsonStr: String, schema: Schema): Array[Byte] = {
    val input = new ByteArrayInputStream(jsonStr.getBytes())
    val dis: DataInputStream = new DataInputStream(input);
    val decoder: Decoder = DecoderFactory.get().jsonDecoder(schema, dis)
    val reader: DatumReader[Object] = new GenericDatumReader[Object](schema)
    val datum: Object = reader.read(null, decoder);
    val writer: GenericDatumWriter[Object] = new GenericDatumWriter[Object](schema)
    val outputStream: ByteArrayOutputStream = new ByteArrayOutputStream()
    val encoder: Encoder = EncoderFactory.get().binaryEncoder(outputStream, null);
    writer.write(datum, encoder)
    encoder.flush();
    outputStream.toByteArray();
  }

  def main(args: Array[String]): Unit = {

//    val a = classOf[GenericRecord].newInstance()
//    println(a)
//    sys.exit(0)
    val hiveDao = new HiveDao()
    val table = hiveDao.getTable("bigdata_test", "person_orc")
    val fieldSchemaList = hiveDao.getAllFieldSchema(table)
    val shcema = convertHiveFieldToAvro(fieldSchemaList)
    println(shcema)
    hiveDao.shutdown()
  }

}
