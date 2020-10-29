package cn.isuyu.rocketmq.demo;

import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.LocalTransactionState;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.client.producer.TransactionMQProducer;
import org.apache.rocketmq.client.producer.TransactionSendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

/**
 * @author : niezl
 * @date : 2020/10/27
 */
@SpringBootTest
@RunWith(SpringRunner.class)
public class TransactionMessageTest {

    @Test
    public void simpleMessageProducer() throws MQClientException {
        TransactionMQProducer producer = new TransactionMQProducer("testGroup");
        producer.setNamesrvAddr("120.78.149.247:9876");

        producer.setTransactionListener(new TransactionListener() {
            @Override
            public LocalTransactionState executeLocalTransaction(Message message, Object o) {
                System.out.println("------------executeLocalTransaction-------------");
                System.out.println("message:"+new String(message.getBody()));
                System.out.println("messageId:"+message.getTransactionId());

                try {
                    System.out.println("try code exec");
                } catch (Exception e) {
                    return LocalTransactionState.ROLLBACK_MESSAGE;
                }
                return LocalTransactionState.COMMIT_MESSAGE;
            }

            @Override
            public LocalTransactionState checkLocalTransaction(MessageExt messageExt) {
                System.out.println("--------------------checkLocalTransaction-----------------------");
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

        TransactionSendResult sendResult = producer.sendMessageInTransaction(new Message("testTopic", "测试！这是事务消息".getBytes()), null);

        System.out.println(sendResult);

        producer.shutdown();

    }


    @Test
    public void simpleMessageConsumer() {

    }
}
