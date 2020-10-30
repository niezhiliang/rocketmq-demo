package cn.isuyu.rocketmq.demo;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * @author : niezl
 * @date : 2020/10/30
 * 异步消息
 */
public class SimpleAsyncMessageTest {

    private static final String NAMESRV_ADDR = "120.78.149.247:9876";

    private static final String PRODUCER_GROUP = "async-group";

    private static final String TOPIC = "simple-topic";

    private static final String TAG = "TAG-A";




    /**
     * 异步消息发送
     */
    @Test
    public void asyncMessageProducer() throws Exception {
        DefaultMQProducer producer = new DefaultMQProducer(PRODUCER_GROUP);
        producer.setNamesrvAddr(NAMESRV_ADDR);
        producer.start();
        Message message1 = new Message(TOPIC,TAG,"","simple async first message".getBytes());

        producer.send(message1, new SendCallback() {
            @Override
            public void onSuccess(SendResult sendResult) {
                System.out.println("异步回调:"+ sendResult.toString());
            }

            @Override
            public void onException(Throwable e) {
                e.printStackTrace();
                System.out.println("异步发送信息异常");
            }
        });
        TimeUnit.SECONDS.sleep(60);
    }


    @Test
    public void asyncMessageConsumer() throws Exception {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(PRODUCER_GROUP);
        consumer.setNamesrvAddr(NAMESRV_ADDR);
        consumer.subscribe(TOPIC,TAG);
        consumer.registerMessageListener(new MessageListenerConcurrently(){

            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                for (MessageExt msg : msgs) {
                    System.out.println("customer received: " +new String(msg.getBody()));
                }
                return ConsumeConcurrentlyStatus.RECONSUME_LATER;
            }
        });
        consumer.start();
        TimeUnit.SECONDS.sleep(60);
    }
}
