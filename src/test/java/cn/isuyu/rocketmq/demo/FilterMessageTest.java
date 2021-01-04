package cn.isuyu.rocketmq.demo;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.MessageSelector;
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
 */
public class FilterMessageTest {

    private static final String NAMESRV_ADDR = "120.78.149.247:9876";

    private static final String PRODUCER_GROUP = "oneway-group";

    private static final String TOPIC = "simple-topic";

    private static final String TAG = "TAG-B";

    /**
     * 批量消息发送
     * @throws Exception
     */
    @Test
    public void batchMessageSend() throws Exception {
        DefaultMQProducer producer = new DefaultMQProducer(PRODUCER_GROUP);
        producer.setNamesrvAddr(NAMESRV_ADDR);
        producer.start();
        for (int i = 1; i <= 1; i++) {
            Message message = new Message(TOPIC,"key"+i,("batch message no:"+i).getBytes());
            message.putUserProperty("num",String.valueOf(i));
            producer.send(message);
        }
    }

    /**
     * 按条件消费
     * 需要开启配置：enablePropertyFilter
     * 修改broker.conf 添加enablePropertyFilter=true
     * @throws Exception
     */
    @Test
    public void choseConsumer() throws Exception {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(PRODUCER_GROUP);
        consumer.setNamesrvAddr(NAMESRV_ADDR);
        MessageSelector selector = MessageSelector.bySql("num > 16 and num < 30");
        consumer.subscribe("testMsg","*");
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
        TimeUnit.SECONDS.sleep(10);
    }
}
