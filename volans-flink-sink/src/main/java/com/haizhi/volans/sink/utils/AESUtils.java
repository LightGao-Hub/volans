package com.haizhi.volans.sink.utils;

import javax.crypto.Cipher;
import javax.crypto.KeyGenerator;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.util.Base64;

/**
 * Create by zhoumingbing on 2020-09-21
 */
public class AESUtils {

    private static final String KEY = "hzAtlasProdution";
    private static final String IV = "hzAtlasProdution";
    private static final String ENCODE = "UTF-8";
    private static final String ALGORITHM = "AES";
    private static final String ALGORITHMSTR = "AES/CBC/NoPadding";

    private AESUtils() {
    }

    public static String encrypt(String content, String encryptKey) throws Exception {
        KeyGenerator kgen = KeyGenerator.getInstance("AES");
        kgen.init(128);
        String ivString = "hzAtlasProdution";
        byte[] iv = ivString.getBytes("UTF-8");
        Cipher cipher = Cipher.getInstance("AES/CBC/NoPadding");
        IvParameterSpec ivSpec = new IvParameterSpec(iv);
        cipher.init(1, new SecretKeySpec(encryptKey.getBytes(), "AES"), ivSpec);
        int blockSize = cipher.getBlockSize();
        byte[] dataBytes = content.getBytes("UTF-8");
        int length = dataBytes.length;
        if (length % blockSize != 0) {
            length += blockSize - length % blockSize;
        }

        byte[] plaintext = new byte[length];
        System.arraycopy(dataBytes, 0, plaintext, 0, dataBytes.length);
        byte[] b = cipher.doFinal(plaintext);
        return Base64.getEncoder().encodeToString(b);
    }

    public static String decrypt(String encryptStr, String decryptKey) throws Exception {
        KeyGenerator kgen = KeyGenerator.getInstance("AES");
        kgen.init(128);
        String ivString = "hzAtlasProdution";
        byte[] iv = ivString.getBytes("UTF-8");
        Cipher cipher = Cipher.getInstance("AES/CBC/NoPadding");
        IvParameterSpec ivSpec = new IvParameterSpec(iv);
        cipher.init(2, new SecretKeySpec(decryptKey.getBytes(), "AES"), ivSpec);
        byte[] encryptBytes = Base64.getDecoder().decode(encryptStr);
        byte[] decryptBytes = cipher.doFinal(encryptBytes);
        return byteToStr(decryptBytes);
    }

    private static String byteToStr(byte[] buffer) {
        try {
            int length = 0;

            for (int i = 0; i < buffer.length; ++i) {
                if (buffer[i] == 0) {
                    length = i;
                    break;
                }
            }

            if (length == 0 && buffer.length > 0) {
                length = buffer.length;
            }

            return new String(buffer, 0, length, "UTF-8");
        } catch (Exception var3) {
            return "";
        }
    }

    public static String encrypt(String content) throws Exception {
        return encrypt(content, "hzAtlasProdution");
    }

    public static String decrypt(String encryptStr) throws Exception {
        return decrypt(encryptStr, "hzAtlasProdution");
    }

}
