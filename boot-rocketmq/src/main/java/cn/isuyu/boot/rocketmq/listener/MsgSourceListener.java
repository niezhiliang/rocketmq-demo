package cn.isuyu.boot.rocketmq.listener;

import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.spring.annotation.RocketMQMessageListener;
import org.apache.rocketmq.spring.core.RocketMQListener;
import org.springframework.stereotype.Component;

/**
 * @author : niezl
 * @date : 2021/1/5
 * 接收原生消息
 */
@Slf4j
@Component
@RocketMQMessageListener(topic = "${rocketmq.topic}",consumerGroup = "defaultGroup",selectorExpression = "*")
public class MsgSourceListener implements RocketMQListener<MessageExt> {
    @Override
    public void onMessage(MessageExt s) {
        log.info("boot reveiver: {}",s);
    }
}
