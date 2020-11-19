package com.haizhi.volans.sink.utils;

import com.haizhi.volans.sink.constant.EncryptionType;
import org.apache.commons.lang3.StringUtils;

/**
 * Create by zhoumingbing on 2020-09-21
 */
public class EncryptDecryptHelper {

    public static String encrypt(String content, String encryptionType) throws Exception {
        EncryptionType type = EncryptionType.getEncryptionType(encryptionType);
        return encrypt(content, type);
    }

    public static String encrypt(String content, EncryptionType encryptionType) throws Exception {
        if (StringUtils.isBlank(content)) {
            return content;
        }
        if (encryptionType == EncryptionType.AES) {
            return AESUtils.encrypt(content);
        }
        return content;
    }

    public static String decrypt(String content, String encryptionType) throws Exception {
        EncryptionType type = EncryptionType.getEncryptionType(encryptionType);
        return decrypt(content, type);
    }

    public static String decrypt(String content, EncryptionType encryptionType) throws Exception {
        if (StringUtils.isBlank(content)) {
            return content;
        }
        if (encryptionType == EncryptionType.AES) {
            return AESUtils.decrypt(content);
        }
        return content;
    }
}
