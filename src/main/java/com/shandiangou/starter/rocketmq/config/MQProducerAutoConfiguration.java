package com.shandiangou.starter.rocketmq.config;

import com.shandiangou.starter.rocketmq.annotation.MQProducer;
import com.shandiangou.starter.rocketmq.annotation.MQTransactionProducer;
import com.shandiangou.starter.rocketmq.base.AbstractMQProducer;
import com.shandiangou.starter.rocketmq.base.AbstractMQTransactionProducer;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.TransactionMQProducer;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

import javax.annotation.PostConstruct;
import java.util.Map;
import java.util.concurrent.*;

/**
 * 自动装配消息生产者
 */
@Slf4j
@Configuration
@ConditionalOnBean(MQBaseAutoConfiguration.class)
public class MQProducerAutoConfiguration extends MQBaseAutoConfiguration {

    @Setter
    private static DefaultMQProducer producer;

    @PostConstruct
    public void exposeProducer() throws Exception {
        Map<String, Object> beans = applicationContext.getBeansWithAnnotation(MQProducer.class);
        //对于仅仅只存在消息消费者的项目，无需构建生产者
        if(CollectionUtils.isEmpty(beans)){
            return;
        }
        Environment environment = applicationContext.getEnvironment();
        for (Map.Entry<String, Object> entry : beans.entrySet()) {
            Object producer = entry.getValue();
            if (producer != null) {
                AbstractMQProducer beanObj = (AbstractMQProducer) producer;
                MQProducer annotation = beanObj.getClass().getAnnotation(MQProducer.class);
                String producerGroup = null;
                //优先去注解
                if (!StringUtils.isEmpty(annotation.producerGroup())) {
                    producerGroup = environment.resolvePlaceholders(annotation.producerGroup());
                }
                if (StringUtils.isEmpty(annotation.producerGroup())) {
                    producerGroup = mqProperties.getProducerGroup();
                }
                Assert.notNull(producerGroup, "producer group must be defined");
                Assert.notNull(mqProperties.getNameServerAddress(), "name server address must be defined");
                DefaultMQProducer defaultMQProducer = new DefaultMQProducer(producerGroup);
                defaultMQProducer.setNamesrvAddr(mqProperties.getNameServerAddress());
                defaultMQProducer.setSendMsgTimeout(mqProperties.getSendMsgTimeout());
                defaultMQProducer.setSendMessageWithVIPChannel(mqProperties.getVipChannelEnabled());
                defaultMQProducer.start();
                beanObj.setProducer(defaultMQProducer);
            }
        }
    }

    @PostConstruct
    public void configTransactionProducer() {
        Map<String, Object> beans = applicationContext.getBeansWithAnnotation(MQTransactionProducer.class);
        if(CollectionUtils.isEmpty(beans)){
            return;
        }
        ExecutorService executorService = new ThreadPoolExecutor(beans.size(), beans.size()*2, 100, TimeUnit.SECONDS, new ArrayBlockingQueue<Runnable>(2000), new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                Thread thread = new Thread(r);
                thread.setName("client-transaction-msg-check-thread");
                return thread;
            }
        });
        Environment environment = applicationContext.getEnvironment();
        beans.entrySet().forEach( transactionProducer -> {
            try {
                AbstractMQTransactionProducer beanObj = AbstractMQTransactionProducer.class.cast(transactionProducer.getValue());
                MQTransactionProducer anno = beanObj.getClass().getAnnotation(MQTransactionProducer.class);

                TransactionMQProducer producer = new TransactionMQProducer(environment.resolvePlaceholders(anno.producerGroup()));
                producer.setNamesrvAddr(mqProperties.getNameServerAddress());
                producer.setExecutorService(executorService);
                producer.setTransactionListener(beanObj);
                producer.start();
                beanObj.setProducer(producer);
            } catch (Exception e) {
                log.error("build transaction producer error : {}", e);
            }
        });
    }
}
