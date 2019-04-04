package com.seventeen.rocketmq.beandemo.common;

import lombok.Setter;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class SimpleMQConsumer {

    private static final Logger LOGGER = LoggerFactory.getLogger(SimpleMQConsumer.class);
    private static final int DEFULT_BATCH_MAX_SIZE = 100;
    @Setter
    private RocketMQConfig rocketMQConfig;
    private DefaultMQPushConsumer consumer;
    @Setter
    private int batchMaxSize = DEFULT_BATCH_MAX_SIZE;
    @Setter
    private String topic;
    @Setter
    private String tags;
    // MessageListenerConcurrently:并发;MessageListenerOrderly:顺序
    @Setter
    private MessageListenerConcurrently messageListener;

    /**
     * 启动consumer
     */
    public void init() throws MQClientException {

        consumer = new DefaultMQPushConsumer(rocketMQConfig.getGroupName());
        consumer.setNamesrvAddr(rocketMQConfig.getNamesrvAddr());
        consumer.subscribe(topic, tags);
        consumer.setConsumeMessageBatchMaxSize(batchMaxSize);
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
        consumer.setMessageModel(MessageModel.CLUSTERING);
        consumer.registerMessageListener(messageListener);
        consumer.start();
        LOGGER.info("Topic[{}] RocketMQConsumer consumer started!", topic);

    }

    public void destroy() {
        if (consumer != null) {
            consumer.shutdown();
        }
        LOGGER.info("Topic[{}] RocketMQConsumer consumer shutdown", topic);
    }


}
