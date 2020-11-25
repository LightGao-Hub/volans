package com.haizhi.volans.sink.func

import com.haizhi.volans.sink.util.AvroUtils
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericDatumReader, GenericRecord}
import org.apache.avro.io.{DatumReader, Decoder, DecoderFactory}
import org.apache.flink.api.common.functions.MapFunction

/**
 * Author pengxb
 * Date 2020/11/24
 */
class AvroConvertMapFunction(val schemaStr: String)
  extends MapFunction[String, GenericRecord] {

  override def map(tuple: String): GenericRecord = {
    // 构建schema
    val schema: Schema = new Schema.Parser().parse(schemaStr)
    val avroByteArray = AvroUtils.fromJsonToAvro(tuple, schema)
    val reader: DatumReader[GenericRecord] = new GenericDatumReader[GenericRecord](schema)
    val decoder: Decoder = DecoderFactory.get().binaryDecoder(avroByteArray, null)
    reader.read(null, decoder)
  }

}
