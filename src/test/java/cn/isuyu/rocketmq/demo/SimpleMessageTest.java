package cn.isuyu.rocketmq.demo;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * @author : niezl
 * @date : 2020/10/27
 * 普通消息
 */
public class SimpleMessageTest {

    private static final String NAMESRV_ADDR = "120.78.149.247:9876";

    private static final String PRODUCER_GROUP = "test-group";

    private static final String TOPIC = "simple-topic";

    /**
     *  普通消息同步发送
     * @throws Exception
     */
    @Test
    public void simpleMessageProducer() throws Exception {
        DefaultMQProducer producer = new DefaultMQProducer(PRODUCER_GROUP);
        producer.setNamesrvAddr(NAMESRV_ADDR);

        producer.start();

        Message message1 = new Message(TOPIC,"simple sync first message".getBytes());
        Message message2 = new Message(TOPIC,"simple sync second message".getBytes());
        Message message3 = new Message(TOPIC,"simple sync thired message".getBytes());

        for (int i = 1 ; i < 10 ; i++ ){
            SendResult sendResult = producer.send(Arrays.asList(message1, message2,message3));
            System.out.println(sendResult);
        }

        //producer.shutdown();
        System.out.println("已经停机");

    }


    /**
     *  普通消息消费
     * @throws Exception
     */
    @Test
    public void simpleMessageConsumer() throws Exception {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(PRODUCER_GROUP);
        consumer.setNamesrvAddr(NAMESRV_ADDR);
        //第一个参数表示：订阅的topic   第二个参数表示消息过滤器：* 表示接收所有信息 一个消费者订阅一个topic
        consumer.subscribe(TOPIC,"*");
        //最大消费线程数
        consumer.setConsumeThreadMax(3);
        //最小消费线程数
        consumer.setConsumeThreadMin(2);
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> list, ConsumeConcurrentlyContext consumeConcurrentlyContext) {
                for (MessageExt messageExt : list) {
                    System.out.println("ThreadId:"+ Thread.currentThread().getName() +"收到的message:"+new String(messageExt.getBody()));
                }
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
        consumer.setMessageModel(MessageModel.CLUSTERING);
        consumer.start();
        System.out.println("simpleConsumer start....");
        TimeUnit.SECONDS.sleep(10);
    }
}
