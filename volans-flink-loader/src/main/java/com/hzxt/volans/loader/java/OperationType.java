package com.hzxt.volans.loader.java;

import org.apache.commons.lang3.EnumUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * GL
 */
public enum OperationType {
    ADD,
    UPDATE,
    DELETE,
    UPSERT;

    private static final Map<String, OperationType> formatTypeMap = new HashMap<>();

    static {
        List<OperationType> enumList = EnumUtils.getEnumList(OperationType.class);
        for (OperationType formatType : enumList) {
            formatTypeMap.put(formatType.name(), formatType);
        }
    }

    public static OperationType findStoreType(String name) {
        return formatTypeMap.get(StringUtils.upperCase(name));
    }
}
