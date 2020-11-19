package com.haizhi.volans.sink.constant;

import org.apache.commons.lang3.EnumUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Create by zhoumingbing on 2020-08-06
 */
public enum StoreType {
    HDFS,
    HIVE,
    ATLAS,
    GDB,
    ES,
    HBASE,
    JANUS
    ;

    private static final Map<String, StoreType> formatTypeMap = new HashMap<>();

    static {
        List<StoreType> enumList = EnumUtils.getEnumList(StoreType.class);
        for (StoreType formatType : enumList) {
            formatTypeMap.put(formatType.name(), formatType);
        }
    }

    public static StoreType findStoreType(String name) {
        return formatTypeMap.get(StringUtils.upperCase(name));
    }
}
