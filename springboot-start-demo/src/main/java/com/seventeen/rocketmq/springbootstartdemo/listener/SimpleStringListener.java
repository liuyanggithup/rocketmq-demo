package com.seventeen.rocketmq.springbootstartdemo.listener;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.spring.annotation.RocketMQMessageListener;
import org.apache.rocketmq.spring.core.RocketMQListener;
import org.apache.rocketmq.spring.core.RocketMQPushConsumerLifecycleListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
@RocketMQMessageListener(
        topic = "test-test-topic",
        consumerGroup = "demo-test-test-consumer")
public class SimpleStringListener implements RocketMQListener<String>, RocketMQPushConsumerLifecycleListener {

    public static final Logger logger = LoggerFactory.getLogger(SimpleStringListener.class);

    @Override
    public void onMessage(String message) {
        logger.info("------- SimpleStringListener received: {} ", message);
    }

    @Override
    public void prepareStart(DefaultMQPushConsumer consumer) {
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
    }
}

