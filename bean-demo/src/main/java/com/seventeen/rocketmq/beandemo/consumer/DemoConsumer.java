package com.seventeen.rocketmq.beandemo.consumer;

import com.seventeen.rocketmq.beandemo.common.RocketMQConfig;
import com.seventeen.rocketmq.beandemo.common.SimpleMQConsumer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;

@Component
public class DemoConsumer {

    @Resource
    private SimpleRocketMQListener simpleRocketMQListener;

    @Value("${pay-rocketmq.name-server}")
    private String nameServer;

    @Value("${pay-rocketmq.consumer.group}")
    private String consumerGroup;


    @Bean(initMethod = "init", destroyMethod = "destroy")
    public SimpleMQConsumer simpleMQConsumer() {


        SimpleMQConsumer simpleMQConsumer = new SimpleMQConsumer();
        simpleMQConsumer.setTopic("TEST");
        simpleMQConsumer.setTags("xyz");
        RocketMQConfig rocketMQConfig = new RocketMQConfig();
        rocketMQConfig.setGroupName(consumerGroup);
        rocketMQConfig.setNamesrvAddr(nameServer);
        simpleMQConsumer.setRocketMQConfig(rocketMQConfig);
        simpleMQConsumer.setMessageListener(simpleRocketMQListener);
        return simpleMQConsumer;
    }


}
