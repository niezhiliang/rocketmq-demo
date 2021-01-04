package cn.isuyu.rocketmq.demo.simple;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * @author : niezl
 * @date : 2020/12/30
 */
public class SimpleConsumer {

    public static void main(String[] args) throws Exception {
        DefaultMQPushConsumer mqPushConsumer = new DefaultMQPushConsumer("oneGroup");
        mqPushConsumer.setNamesrvAddr(SimpleProducer.NAME_SERVER_ADDR);
        //消费模式  默认 集群消费
        mqPushConsumer.setMessageModel(MessageModel.CLUSTERING);
        //并发消费
        mqPushConsumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                for (MessageExt msg : msgs) {
                    System.out.println(new String(msg.getBody()));
                }
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
        mqPushConsumer.subscribe(SimpleProducer.TOPIC,"*");
        mqPushConsumer.start();
        TimeUnit.SECONDS.sleep(10);
        mqPushConsumer.shutdown();
    }
}
