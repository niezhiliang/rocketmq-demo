package cn.isuyu.rocketmq.demo;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.producer.LocalTransactionState;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.client.producer.TransactionMQProducer;
import org.apache.rocketmq.client.producer.TransactionSendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * @author : niezl
 * @date : 2020/10/27
 */
public class TransactionMessageTest {

    private static final String NAMESRV_ADDR = "120.78.149.247:9876";

    private static final String PRODUCER_GROUP = "transsction-group";

    private static final String TOPIC = "simple-topic";

    private static final String TAG = "TAG-B";

    @Test
    public void simpleMessageProducer() throws Exception {
        TransactionMQProducer producer = new TransactionMQProducer(PRODUCER_GROUP);
        producer.setNamesrvAddr(NAMESRV_ADDR);

        producer.setTransactionListener(new TransactionListener() {

            /**
             * 执行本地事务，如果执行成功返回COMMIT_MESSAGE
             * broker会将消息发送出去，
             * 本地实物执行失败的话，broker会将消息删除
             * @param message
             * @param o
             * @return
             */
            @Override
            public LocalTransactionState executeLocalTransaction(Message message, Object o) {
                System.out.println("------------执行本地事务-------------");
                System.out.println("message:"+new String(message.getBody()));
                System.out.println("messageId:"+message.getTransactionId());

                try {
                    System.out.println("try code exec");
                } catch (Exception e) {
                    //回滚
                    return LocalTransactionState.ROLLBACK_MESSAGE;
                }
                return LocalTransactionState.ROLLBACK_MESSAGE;
            }

            /**
             * broker长时间没收到确认信息
             * 会回调接口来查看本地事务的执行情况
             * @param messageExt
             * @return
             */
            @Override
            public LocalTransactionState checkLocalTransaction(MessageExt messageExt) {
                //broker长时间没收到本地事务返回的状态，会主动回调询问事务状态 时间间隔为messageDelay设置的
                System.out.println("--------------------Broker执行回调检查本地事务状态-----------------------");
                System.out.println("message:"+new String(messageExt.getBody()));
                System.out.println("messageId:"+new String(messageExt.getTransactionId()));
                //回滚信息
                //return LocalTransactionState.ROLLBACK_MESSAGE;
                //等一会
                //return LocalTransactionState.UNKNOW;
                //事务执行成功
                return LocalTransactionState.COMMIT_MESSAGE;

            }
        });

        producer.start();

        TransactionSendResult sendResult = producer.sendMessageInTransaction(new Message(TOPIC, "测试！这是事务消息".getBytes()), null);

        System.out.println(sendResult);

        TimeUnit.SECONDS.sleep(15);
        producer.shutdown();

    }


    @Test
    public void simpleMessageConsumer () throws Exception {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(PRODUCER_GROUP);
        consumer.setNamesrvAddr(NAMESRV_ADDR);
        consumer.subscribe(TOPIC,"*");
        consumer.setMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                for (MessageExt msg : msgs) {
                    System.out.println("msg:"+new String(msg.getBody()));
                }
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
        consumer.start();

        TimeUnit.SECONDS.sleep(20);
    }
}
