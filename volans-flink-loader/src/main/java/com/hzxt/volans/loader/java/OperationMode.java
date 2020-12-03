package com.hzxt.volans.loader.java;

import org.apache.commons.lang3.EnumUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * GL
 */
public enum OperationMode {
    MIX,
    DELETE,
    UPSERT;

    private static final Map<String, OperationMode> formatTypeMap = new HashMap<>();

    static {
        List<OperationMode> enumList = EnumUtils.getEnumList(OperationMode.class);
        for (OperationMode formatType : enumList) {
            formatTypeMap.put(formatType.name(), formatType);
        }
    }

    public static OperationMode findStoreType(String name) {
        return formatTypeMap.get(StringUtils.upperCase(name));
    }
}
