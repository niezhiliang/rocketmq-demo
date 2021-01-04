package cn.isuyu.my.rocketmq;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.web.bind.annotation.GetMapping;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author : niezl
 * @date : 2021/1/4
 */
@SpringBootApplication
public class Application {
    @Autowired
    private DefaultMQProducer defaultMQProducer;

    private static AtomicInteger num = new AtomicInteger(0);

    public static void main(String[] args) {
        SpringApplication.run(Application.class);
    }

    @GetMapping(value = "send")
    public String sendMsg() throws Exception {
        for (int i = 0; i < 5; i++) {
            Message message = new Message("testMsg",("hello " + num.addAndGet(1)).getBytes());
            defaultMQProducer.send(message);
        }
        return "ok";
    }
}
