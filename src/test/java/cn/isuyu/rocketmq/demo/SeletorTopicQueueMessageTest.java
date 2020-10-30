package cn.isuyu.rocketmq.demo;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * @author : niezl
 * @date : 2020/10/30
 */
public class SeletorTopicQueueMessageTest {

    private static final String NAMESRV_ADDR = "120.78.149.247:9876";

    private static final String PRODUCER_GROUP = "selector-group";

    private static final String TOPIC = "simple-topic";

    private static final String TAG = "TAG-ORDER";

    /**
     * 向某个topic的某个queue中发送消息
     * @throws Exception
     */
    @Test
    public void selectorQueueMessage() throws Exception {
        DefaultMQProducer producer = new DefaultMQProducer(PRODUCER_GROUP);
        producer.setNamesrvAddr(NAMESRV_ADDR);
        producer.start();
        for (int i = 0; i < 20; i++) {
            Message message = new Message(TOPIC,TAG,("hello " + i).getBytes());

            producer.send(message, new MessageQueueSelector() {
                @Override                   //topic默认有4个queue
                public MessageQueue select(List<MessageQueue> mqs,
                                           Message msg,
                                           Object arg) {
                    //固定向topic的第一个queue发送消息
                    MessageQueue queue = mqs.get(0);
                    return queue;
                }
            },0,2000);
        }

        for (int i = 0; i < 20; i++) {
            Message message = new Message(TOPIC,TAG,("hello2 " + i).getBytes());

            producer.send(message, new MessageQueueSelector() {
                @Override
                public MessageQueue select(List<MessageQueue> mqs, Message msg, Object arg) {
                    //固定向topic的第二个queue发送消息
                    MessageQueue queue = mqs.get(1);
                    return queue;
                }
            },0,2000);
        }
    }

    /**
     * 顺序消费
     * @throws Exception
     */
    @Test
    public void orderConsumer() throws Exception{
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(PRODUCER_GROUP);
        consumer.setNamesrvAddr(NAMESRV_ADDR);
        consumer.subscribe(TOPIC,TAG);
        consumer.setConsumeThreadMax(1);
        consumer.setConsumeThreadMin(1);
        consumer.registerMessageListener(new MessageListenerOrderly() {
            @Override
            public ConsumeOrderlyStatus consumeMessage(List<MessageExt> msgs, ConsumeOrderlyContext context) {
                for (MessageExt msg : msgs) {
                    System.out.println("Thread:" + Thread.currentThread().getName() + "customer received: " +new String(msg.getBody()) + " queueid:" + msg.getQueueId());
                }
                return ConsumeOrderlyStatus.SUCCESS;
            }
        });
        consumer.start();
        TimeUnit.SECONDS.sleep(10);
    }
}
