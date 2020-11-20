package com.haizhi.volans.sink.utils;

import com.google.gson.reflect.TypeToken;
import com.haizhi.volans.common.flink.base.scala.util.JSONUtils;
import com.haizhi.volans.sink.config.constant.JavaFieldType;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.*;
import scala.collection.mutable.WrappedArray;

import java.util.Date;

/**
 * Create by zhoumingbing on 2020-09-23
 */
public class HbaseSerializer {

    private HbaseSerializer() {
    }

    public static <T> byte[] serialize(T t, JavaFieldType fieldType) {
        return getByteByBean(t, fieldType);
    }

    public static Object deserialize(byte[] bytes, JavaFieldType fieldType) {
        return getBeanByByte(bytes, fieldType);
    }

    private static Object getBeanByByte(byte[] bytes, JavaFieldType fieldType) {
        switch (fieldType) {
            case STRING:
            case UNKNOWN:
                return Bytes.toString(bytes);
            case LONG:
            case DATETIME:
                return Bytes.toLong(bytes);
            case DOUBLE:
                return Bytes.toDouble(bytes);
            default:
                throw new UnsupportedOperationException("fieldType:" + fieldType + " unsupported deserialize from byte");
        }
    }

    private static <T> byte[] getByteByBean(T t, JavaFieldType fieldType) {
        switch (fieldType) {
            case STRING:
            case UNKNOWN:
                return Bytes.toBytes(String.valueOf(t));
            case LONG:
                return Bytes.toBytes(Long.parseLong(String.valueOf(t)));
            case DOUBLE:
                return Bytes.toBytes(Double.parseDouble(String.valueOf(t)));
            case DATETIME:
                long value;
                if (t instanceof Date) {
                    value = ((Date) t).getTime();
                } else if (t instanceof Long) {
                    value = (Long) t;
                } else {
                    value = Long.parseLong(t.toString());
                }
                return Bytes.toBytes(value);
            case ARRAY:
                return getArrayByte(t, JavaFieldType.STRING);
            default:
                throw new UnsupportedOperationException("fieldType:" + fieldType + " unsupported to bytes ");
        }
    }

    private static <T> byte[] getArrayByte(T t, JavaFieldType fieldType) {
        Object[] objects;
        if (t.getClass().isArray()) {
            objects = (Object[]) t;
        }else if(t instanceof WrappedArray){
            objects = (Object[]) ((WrappedArray)t).array();
        } else {
            objects = JSONUtils.fromJson(t.toString(), TypeToken.get(getBeanClass(fieldType)).getType());
        }
        return WritableUtils.toByteArray(getArrayWritable(objects, fieldType));
    }

    private static ArrayWritable getArrayWritable(Object[] objects, JavaFieldType fieldType) {
        Writable[] writers = new Writable[objects.length];
        Class clazz;
        switch (fieldType) {
            case STRING:
                for (int i = 0; i < objects.length; i++) {
                    writers[i] = new Text(objects[i].toString());
                }
                clazz = Text.class;
                break;
            case LONG:
                for (int i = 0; i < objects.length; i++) {
                    writers[i] = new LongWritable(Long.parseLong(objects[i].toString()));
                }
                clazz = LongWritable.class;
                break;
            case DOUBLE:
                for (int i = 0; i < objects.length; i++) {
                    writers[i] = new DoubleWritable(Double.parseDouble(objects[i].toString()));
                }
                clazz = DoubleWritable.class;
                break;
            case DATETIME:
                for (int i = 0; i < objects.length; i++) {
                    long value;
                    Object t = objects[i];
                    if (t instanceof Date) {
                        value = ((Date) t).getTime();
                    } else if (t instanceof Long) {
                        value = (Long) t;
                    } else {
                        value = Long.parseLong(t.toString());
                    }
                    writers[i] = new LongWritable(value);
                }
                clazz = LongWritable.class;
                break;
            default:
                throw new UnsupportedOperationException("fieldType:" + fieldType + " unsupported to array bytes ");
        }
        return new ArrayWritable(clazz, writers);
    }

    private static Class getBeanClass(JavaFieldType fieldType) {
        switch (fieldType) {
            case STRING:
                return String.class;
            case LONG:
            case DATETIME:
                return Long.class;
            case DOUBLE:
                return Double.class;
            default:
                throw new UnsupportedOperationException("fieldType:" + fieldType + "unsupported get bean class");
        }
    }
}
