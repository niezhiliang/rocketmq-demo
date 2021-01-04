package cn.isuyu.my.rocketmq.config;

import cn.isuyu.my.rocketmq.listen.ConsumerListener;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.annotation.PreDestroy;


/**
 * @author : niezl
 * @date : 2021/1/3
 */
@Configuration
@Slf4j
public class RocketConfig {

    @Value("${rocketmq.my.nameServer}")
    private String namesrvAddr;

    @Value("${rocketmq.my.producer.group}")
    private String producerGroup;

    @Value("${rocketmq.my.consumer.group}")
    private String consumerGroup;

    @Value("${rocketmq.my.topic}")
    private String topic;

    private DefaultMQPushConsumer mqPushConsumer;

    private DefaultMQProducer mqProducer;


    /**
     * 注入produer
     * @return
     * @throws MQClientException
     */
    @Bean
    public DefaultMQProducer defaultMQProducer() throws MQClientException {
        mqProducer = new DefaultMQProducer(producerGroup);
        mqProducer.setNamesrvAddr(namesrvAddr);
        mqProducer.start();
        log.info("RocketMQ producer start finished....");
        return mqProducer;
    }

    /**
     * 注入consumer
     * @return
     * @throws MQClientException
     */
    @Bean
    public DefaultMQPushConsumer defaultMQPushConsumer() throws MQClientException {
        mqPushConsumer = new DefaultMQPushConsumer(consumerGroup);
        mqPushConsumer.setNamesrvAddr(namesrvAddr);
        mqPushConsumer.setMessageModel(MessageModel.CLUSTERING);
        mqPushConsumer.subscribe(topic,"*");
        mqPushConsumer.registerMessageListener(new ConsumerListener());
        mqPushConsumer.start();
        log.info("RocketMQ consumer start finished,liseten topic:{}....",topic);
        return mqPushConsumer;
    }

    /**
     * 容器关闭时，关闭produer和consumer
     */
    @PreDestroy
    public void beanDestroy() {
        mqProducer.shutdown();
        log.info("RocketMQ producer shutdown finished...");
        mqPushConsumer.shutdown();
        log.info("RocketMQ consumer shutdown finished...");
    }

}
