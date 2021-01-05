package cn.isuyu.boot.rocketmq;

import org.apache.rocketmq.spring.core.RocketMQTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author : niezl
 * @date : 2021/1/4
 */
@SpringBootApplication
@RestController
public class BootApplication {
    @Autowired
    private RocketMQTemplate rocketMQTemplate;

    private static AtomicInteger num = new AtomicInteger(0);

    public static void main(String[] args) {
        SpringApplication.run(BootApplication.class);
    }

    /**
     * 测试发送
     * @return
     */
    @GetMapping(value = "send")
    public String send() {
        for (int i = 0; i < 5; i++) {
            Message<String> message = MessageBuilder.withPayload("hello boot :" + num.addAndGet(1))
                    //添加消息头信息（支持自定义属性）
                    .setHeader("TAGS","*")
                    .setHeader("num",i)
                    .build();
            rocketMQTemplate.send("testMsg",message);
        }
        return "ok";
    }
}
