package com.seventeen.rocketmq.beandemo.common;

import lombok.Data;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Data
public class SimpleMQProducer extends DefaultMQProducer{
    private static final Logger LOGGER = LoggerFactory.getLogger(SimpleMQProducer.class);

    private RocketMQConfig rocketMQConfig;

    public void init() {

        if (rocketMQConfig == null) {
            throw new NullPointerException("rocketMQConfig is null!");
        }

        this.setProducerGroup(rocketMQConfig.getGroupName()+"ddd");
        this.setNamesrvAddr(rocketMQConfig.getNamesrvAddr());
        try {
            this.start();
        } catch (Exception e) {
            LOGGER.error("RocketMQ producer start fail:{},{}", rocketMQConfig.getGroupName(),
                    rocketMQConfig.getNamesrvAddr());
        }

    }


    

    public void destroy() {
        LOGGER.info("RocketMQ producer shutdown");
        if (this != null) {
            this.shutdown();
        }
    }


}
