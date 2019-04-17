package com.lickey.starter.rocketmq.utils;

import com.alibaba.fastjson.JSON;

import java.nio.charset.Charset;

/**
 * @author : lvqi
 * @date : 2019/4/10 21:59
 * @description : json序列化
 */
public class FastJson2JsonSerializer {
    private static final Charset DEFAULT_CHARSET = Charset.forName("UTF-8");

    public static byte[] serialize(Object t) {
        if (t == null) {
            return new byte[0];
        }
        return JSON.toJSONBytes(t);
    }

    public static String deserialize(byte[] bytes) {
        if (bytes == null || bytes.length <= 0) {
            return null;
        }
        return new String(bytes, DEFAULT_CHARSET);
    }
}
