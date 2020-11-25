package com.haizhi.volans.sink.writer.orc;

import org.apache.avro.generic.GenericRecord;

import java.io.Serializable;
import java.util.Properties;

public class OrcWriters implements Serializable {

    public static <T extends GenericRecord> GenericRecordOrcWriterFactory<T> forGenericRecord(String avroSchemaString, Properties props) {
        GenericRecordHiveOrcBuilder builder = new GenericRecordHiveOrcBuilder(avroSchemaString, props);
        return new GenericRecordOrcWriterFactory<T>(builder);
    }
}
