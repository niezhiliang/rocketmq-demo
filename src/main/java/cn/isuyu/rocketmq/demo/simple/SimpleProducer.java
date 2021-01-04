package cn.isuyu.rocketmq.demo.simple;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

/**
 * @author : niezl
 * @date : 2020/12/30
 * 同步发送
 */
public class SimpleProducer {
    /**
     * nameserver地址
     */
    public final static String NAME_SERVER_ADDR = "127.0.0.1:9876";

    /**
     * 系统的测试Topic
     */
    public final static String TOPIC  = "testMsg";

    /**
     * 生成者组
     */
    public final static String PRODUCER_GROUP = "simpleGroup";

    public static void main(String[] args) throws Exception {
        final DefaultMQProducer mqProducer = new DefaultMQProducer(PRODUCER_GROUP);
        mqProducer.setNamesrvAddr(NAME_SERVER_ADDR);
        //启动producer
        mqProducer.start();
        Message syncMessage = new Message(TOPIC,"tag-a", "sync: hello world".getBytes());
        //设置消息的延迟等级
        syncMessage.setDelayTimeLevel(2);
        //同步发送
        SendResult result = mqProducer.send(syncMessage);
        System.out.println("同步发送成功："+result);

        //异步发送
        Message asyncMessage = new Message(TOPIC, "async: hello world".getBytes());

        mqProducer.send(asyncMessage, new SendCallback() {
            @Override
            public void onSuccess(SendResult sendResult) {
                System.out.println("异步发送成功："+ sendResult);
            }

            @Override
            public void onException(Throwable e) {
                System.out.println("异步发送异常："+ e);
            }
        });

        //单向消息
        Message oneWayMessage = new Message(TOPIC, "oneway: hello world".getBytes());
        mqProducer.sendOneway(oneWayMessage);

        //批量发送
        mqProducer.send(Arrays.asList(asyncMessage,oneWayMessage));


        TimeUnit.SECONDS.sleep(3);
        mqProducer.shutdown();
    }
}
