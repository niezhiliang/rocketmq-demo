package cn.isuyu.rocketmq.demo;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * @author : niezl
 * @date : 2020/10/30
 * 单向消息
 */
public class OneWayMessageTest {

    private static final String NAMESRV_ADDR = "120.78.149.247:9876";

    private static final String PRODUCER_GROUP = "oneway-group";

    private static final String TOPIC = "simple-topic";

    private static final String TAG = "TAG-B";

    /**
     * 发送单相消息
     * @throws Exception
     */
    @Test
    public void oneWayMessageProducer() throws Exception {
        DefaultMQProducer producer = new DefaultMQProducer(PRODUCER_GROUP);
        producer.setNamesrvAddr(NAMESRV_ADDR);
        producer.start();
        Message message = new Message(TOPIC,TAG,"oneway first message".getBytes());
        //延时发送  默认1s 5s 10s 30s 1m 2m 3m 4m 5m 6m 7m 8m 9m 10m 20m 30m 1h 2h
        message.setDelayTimeLevel(2);

        Message message2 = new Message(TOPIC,TAG,"oneway second message".getBytes());
        message2.setDelayTimeLevel(3);
        // 单向消息
        //网络不确定的时候发送
        producer.sendOneway(message);
        producer.sendOneway(message2);
        TimeUnit.SECONDS.sleep(20);
    }

    @Test
    public void oneWayMessageConsumer() throws Exception {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(PRODUCER_GROUP);
        consumer.setNamesrvAddr(NAMESRV_ADDR);
        consumer.subscribe(TOPIC,TAG);
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                for (MessageExt msg : msgs) {
                    System.out.println("customer received: " +new String(msg.getBody()));
                }
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
        consumer.start();
        TimeUnit.SECONDS.sleep(30);
    }

}
