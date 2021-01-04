package cn.isuyu.boot.rocketmq.listener;

import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.spring.annotation.RocketMQMessageListener;
import org.apache.rocketmq.spring.core.RocketMQListener;
import org.springframework.stereotype.Component;

/**
 * @author : niezl
 * @date : 2021/1/4
 */
@Slf4j
@Component
@RocketMQMessageListener(topic = "${rocketmq.topic}",consumerGroup = "defaultGroup")
public class BootMessageListener implements RocketMQListener<String> {
    @Override
    public void onMessage(String s) {
        log.info("boot reveiver: {}",s);
    }
}
