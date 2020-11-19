package com.haizhi.volans.sink.constant;

import com.haizhi.volans.sink.utils.DateUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.EnumSet;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Create by zhoumingbing on 2020-09-24
 */
public enum  JavaFieldType {
    STRING("字符串"),
    LONG("整数"),
    DOUBLE("浮点数"),
    DATETIME("日期"),
    ARRAY("数组"),
    UNKNOWN("UNKNOWN");

    private static final Map<String, JavaFieldType> codeLookup = new ConcurrentHashMap(6);
    private String label;

    private JavaFieldType(String label) {
        this.label = label;
    }

    public String label() {
        return this.label;
    }

    public boolean isNumber() {
        return this.equals(DOUBLE) || this.equals(LONG);
    }

    public boolean isText() {
        return this.equals(STRING) || this.equals(DATETIME);
    }

    public boolean isDate() {
        return this.equals(DATETIME);
    }

    public static JavaFieldType fromCode(String code) {
        if (code == null) {
            return UNKNOWN;
        } else {
            JavaFieldType data = (JavaFieldType)codeLookup.get(code.toLowerCase());
            return data == null ? UNKNOWN : data;
        }
    }

    public static JavaFieldType getFieldType(String value) {
        try {
            return StringUtils.isEmpty(value) ? STRING : (DateUtils.isDate(value) ? DATETIME : (StringUtils.isNumeric(value) ? LONG : (StringUtils.isNumeric(value.replace(".", "").replaceAll(",", "")) ? DOUBLE : STRING)));
        } catch (Exception var2) {
            return STRING;
        }
    }

    static {
        Iterator iterator = EnumSet.allOf(JavaFieldType.class).iterator();

        while(iterator.hasNext()) {
            JavaFieldType type = (JavaFieldType)iterator.next();
            codeLookup.put(type.name().toLowerCase(), type);
        }

    }
}
