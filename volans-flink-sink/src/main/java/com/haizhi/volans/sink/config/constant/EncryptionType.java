package com.haizhi.volans.sink.config.constant;

import org.apache.commons.lang3.EnumUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Create by zhoumingbing on 2020-09-21
 */
public enum EncryptionType {

    AES,
    ORIGINAL,
    ;

    private static final Map<String, EncryptionType> encryptionTypeMap = new ConcurrentHashMap<>();

    static {
        List<EncryptionType> enumList = EnumUtils.getEnumList(EncryptionType.class);
        for (EncryptionType encryptionType : enumList) {
            encryptionTypeMap.put(encryptionType.name(), encryptionType);
        }
    }

    public static EncryptionType getEncryptionType(String name) {
        if (StringUtils.isBlank(name)) {
            return ORIGINAL;
        }
        EncryptionType encryptionType = encryptionTypeMap.get(name);
        Objects.requireNonNull(encryptionType, "can't find encryption by " + name);
        return encryptionType;
    }
}
