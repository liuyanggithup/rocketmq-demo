package com.seventeen.rocketmq.springbootstartdemo.listener;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.spring.annotation.RocketMQMessageListener;
import org.apache.rocketmq.spring.core.RocketMQListener;
import org.apache.rocketmq.spring.core.RocketMQPushConsumerLifecycleListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
@RocketMQMessageListener(
        topic = "test-topic2",
        consumerGroup = "test2-group")
public class WithPayloadListener
        implements RocketMQListener<MessageExt>, RocketMQPushConsumerLifecycleListener {

    public static final Logger logger = LoggerFactory.getLogger(WithPayloadListener.class);

    @Override
    public void onMessage(MessageExt messageExt) {
        String message = new String(messageExt.getBody());
        //KEYS:幂等判断的key,可以用流水单号等，保证唯一性
        System.out.println(messageExt.getKeys());
        logger.info("------- MessageExtConsumer received message,messageExt {}, message {}", messageExt.toString(), message);
    }

    @Override
    public void prepareStart(DefaultMQPushConsumer consumer) {
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
    }
}
