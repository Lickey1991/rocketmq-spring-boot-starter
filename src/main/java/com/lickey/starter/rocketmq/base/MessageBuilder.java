package com.lickey.starter.rocketmq.base;

import com.lickey.starter.rocketmq.annotation.MQKey;
import com.lickey.starter.rocketmq.enums.DelayTimeLevel;
import com.lickey.starter.rocketmq.utils.FastJson2JsonSerializer;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.message.Message;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;

@Data
@Slf4j
public class MessageBuilder {


    private static final String[] DELAY_ARRAY = "1s 5s 10s 30s 1m 2m 3m 4m 5m 6m 7m 8m 9m 10m 20m 30m 1h 2h".split(" ");

    private String topic;
    private String tag;
    private String key;
    private Object messageBody;
    private Integer delayTimeLevel;

    public static MessageBuilder of(String topic, String tag) {
        MessageBuilder builder = new MessageBuilder();
        builder.setTopic(topic);
        builder.setTag(tag);
        return builder;
    }

    public static MessageBuilder of(Object messageBody) {
        MessageBuilder builder = new MessageBuilder();
        builder.setMessageBody(messageBody);
        return builder;
    }

    public MessageBuilder topic(String topic) {
        this.topic = topic;
        return this;
    }

    public MessageBuilder tag(String tag) {
        this.tag = tag;
        return this;
    }

    public MessageBuilder key(String key) {
        this.key = key;
        return this;
    }

    public MessageBuilder delayTimeLevel(DelayTimeLevel delayTimeLevel) {
        this.delayTimeLevel = delayTimeLevel.getLevel();
        return this;
    }

    public Message build() {
        StringBuilder messageKey= new StringBuilder(StringUtils.isEmpty(key) ? "" : key);
        try {
            Field[] fields = messageBody.getClass().getDeclaredFields();
            for (Field field : fields) {
                Annotation[] allFAnnos= field.getAnnotations();
                if(allFAnnos.length > 0) {
                    for (int i = 0; i < allFAnnos.length; i++) {
                        if(allFAnnos[i].annotationType().equals(MQKey.class)) {
                            field.setAccessible(true);
                            MQKey mqKey = MQKey.class.cast(allFAnnos[i]);
                            messageKey.append(StringUtils.SPACE).append(StringUtils.isEmpty(mqKey.prefix()) ? field.get(messageBody).toString() : (mqKey.prefix() + ":" + field.get(messageBody).toString()));
                        }
                    }
                }
            }
        } catch (Exception e) {
            log.error("parse key error : {}" , e.getMessage());
        }

        if(StringUtils.isEmpty(topic)) {
            if(StringUtils.isEmpty(getTopic())) {
                throw new RuntimeException("no topic defined to send this message");
            }
        }
        Message message = new Message(topic, FastJson2JsonSerializer.serialize(messageBody));
        if (!StringUtils.isEmpty(tag)) {
            message.setTags(tag);
        }
        if(StringUtils.isNotEmpty(messageKey.toString())) {
            message.setKeys(messageKey.toString());
        }
        if(delayTimeLevel != null && delayTimeLevel > 0 && delayTimeLevel <= DELAY_ARRAY.length) {
            message.setDelayTimeLevel(delayTimeLevel);
        }
        return message;
    }



}
